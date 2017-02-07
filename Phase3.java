import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.*;

public class Phase3 implements Serializable {

	static JavaSparkContext sc;
	static String inputFile;
	static String outputFile;

	final static double latMin = 40.50;
	final static double latMax = 40.90;
	final static double lonMin = -74.25;
	final static double lonMax = -73.70;
	final static double distRange = 0.01;
	final static int days = 31;
	final static int daysRange = 1;
	final static int numLats = (int) ((latMax - latMin + 0.01) / distRange);
	final static int numLons = (int) Math.abs((lonMax - lonMin + 0.01) / distRange);
	final static int numDays = days;
	final static int totalCells = numLats * numLons * numDays;
	static int[][][] attributeMatrix = new int[numLats][numLons][numDays];
	static double[][][] zScoreMatrix = new double[numLats][numLons][numDays];
	static List<Point> points_50 = new ArrayList<Point>();
	static double total_sum_attribute_matrix = 0.0;

	public Phase3(JavaSparkContext jsc, String f1, String f2) {
		try {
			sc = jsc;
			inputFile = f1;
			outputFile = f2;
			mapReduce();
			calculateZscore();
			writeToFile(f2);
			// printAttributeMatrix();
			// printZScoreMatrix();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public static void mapReduce() {
		JavaRDD<String> csvData = sc.textFile(inputFile);
		JavaPairRDD<String, Integer> t1 = csvData.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] coordinate = line.split(",");
				int date = Integer.parseInt(coordinate[1].split("-|/|\\s+")[2]);
				double lat = Double.parseDouble(coordinate[6]);
				double lon = Double.parseDouble(coordinate[5]);

				if (lat >= latMin && lat <= latMax && lon >= lonMin && lon <= lonMax) {
					date = date - 1;
					lat = (int) ((lat - latMin) / distRange);
					lon = (int) ((lon - lonMin) / distRange);

					String s = lat + "," + lon + "," + date;
					Tuple2<String, Integer> t2 = new Tuple2<String, Integer>(s, 1);
					return t2;
				} else
					return new Tuple2<String, Integer>("", 0);
			}
		});

		JavaPairRDD<String, Integer> t2 = t1.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});

		Map<String, Integer> m1 = t2.collectAsMap();

		for (Map.Entry<String, Integer> m2 : m1.entrySet()) {
			if (m2.getKey() == null || "".equals(m2.getKey()))
				continue;
			String[] coord = m2.getKey().split(",");
			int i = (int) Double.parseDouble(coord[0]);
			int j = (int) Double.parseDouble(coord[1]);
			int k = (int) Double.parseDouble(coord[2]);

			attributeMatrix[i][j][k] = m2.getValue();
		}

	}

	public static void main(String[] args) {
		try {
			JavaSparkContext sc = new JavaSparkContext("local", "Phase3");
			String inputFile = "/Users/santosh/Downloads/Phase3/yellow_tripdata_2015-01.csv";
			String outFile = "/Users/santosh/Downloads/Phase3/output.csv";

			Phase3 g = new Phase3(sc, args[0], args[1]);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void callme(String inputFile, String outFile) {
		try {
			JavaSparkContext sc = new JavaSparkContext("local", "Phase3");
			Phase3 g = new Phase3(sc, inputFile, outFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void writeToFile(String filename) {
		try {
			File file = new File(filename);
			FileWriter fileWriter = new FileWriter(file);
			StringBuffer sb = new StringBuffer();
			for (Point p : points_50) {
				sb.append(p.toString() + "\n");
			}
			fileWriter.write(sb.toString());
			fileWriter.flush();
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void calculateZscore() {
		double mean = calculateMean();
		double variance = calculateVariance(mean);
		ArrayList<Point> l = new ArrayList<>();
		for (int i = 0; i < numLats; i++) {
			for (int j = 0; j < numLons; j++) {
				for (int k = 0; k < numDays; k++) {
					double score = numerator(i, j, k, mean) / denominator(i, j, k, variance);
					zScoreMatrix[i][j][k] = score;
					l.add(new Point(i, j, k, score));
				}
			}
		}

		Collections.sort(l, Collections.<Point> reverseOrder());
		for (int i = 0; i < 50; i++) {
			Point p = l.get(i);
			points_50.add(p);
			System.out.print(l.get(i) + " attributeValue: " + attributeMatrix[p.x][p.y][p.z] + "\n");
		}
		System.out.println(total_sum_attribute_matrix);

	}

	private static double calculateMean() {
		double sum = 0.0;
		for (int i = 0; i < numLats; i++) {
			for (int j = 0; j < numLons; j++) {
				for (int k = 0; k < numDays; k++) {
					sum += attributeMatrix[i][j][k];
				}
			}
		}
		System.out.println("MEAN: " + sum / totalCells);
		total_sum_attribute_matrix = sum;
		return sum / totalCells;
	}

	private static double calculateVariance(double mean) {
		double variance = 0.0;
		for (int i = 0; i < numLats; i++) {
			for (int j = 0; j < numLons; j++) {
				for (int k = 0; k < numDays; k++) {
					variance += (attributeMatrix[i][j][k] * attributeMatrix[i][j][k]);
				}
			}
		}
		System.out.println("VARIANCE: " + Math.sqrt((variance / totalCells) - (mean * mean)));
		return Math.sqrt((variance / totalCells) - (mean * mean));
	}

	public static double numerator(int i, int j, int k, double mean) {
		double n = 0.0;
		int sigmaW = adjacentCubes_sigmaW(i, j, k);
		int sigmaWX = totalPointsInaAdjacentCells_sigmaWX(i, j, k);
		n = sigmaWX - (mean * sigmaW);
		return n;
	}

	public static double denominator(int i, int j, int k, double variance) {
		double d = 0.0;
		int sigmaW = adjacentCubes_sigmaW(i, j, k);

		d = (totalCells * sigmaW - Math.pow(sigmaW, 2)) / (totalCells - 1);
		d = Math.sqrt(d) * variance;

		return d;
	}

	public static int totalPointsInaAdjacentCells_sigmaWX(int i, int j, int k) {

		int count = 0;
		// --------------k-1 th layer------------------------

		List<int[]> l = new ArrayList<int[]>();

		l.add(new int[] { i - 1, j + 1, k - 1 });
		l.add(new int[] { i, j + 1, k - 1 });
		l.add(new int[] { i + 1, j + 1, k - 1 });

		l.add(new int[] { i - 1, j, k - 1 });
		l.add(new int[] { i, j, k - 1 });
		l.add(new int[] { i + 1, j, k - 1 });

		l.add(new int[] { i - 1, j - 1, k - 1 });
		l.add(new int[] { i, j - 1, k - 1 });
		l.add(new int[] { i + 1, j - 1, k - 1 });

		// --------------k th layer------------------------
		l.add(new int[] { i - 1, j + 1, k });
		l.add(new int[] { i, j + 1, k });
		l.add(new int[] { i + 1, j + 1, k });

		l.add(new int[] { i - 1, j, k });
		l.add(new int[] { i, j, k });
		l.add(new int[] { i + 1, j, k });

		l.add(new int[] { i - 1, j - 1, k });
		l.add(new int[] { i, j - 1, k });
		l.add(new int[] { i + 1, j - 1, k });

		// --------------k+1 th layer------------------------

		l.add(new int[] { i - 1, j + 1, k + 1 });
		l.add(new int[] { i, j + 1, k + 1 });
		l.add(new int[] { i + 1, j + 1, k + 1 });

		l.add(new int[] { i - 1, j, k + 1 });
		l.add(new int[] { i, j, k + 1 });
		l.add(new int[] { i + 1, j, k + 1 });

		l.add(new int[] { i - 1, j - 1, k + 1 });
		l.add(new int[] { i, j - 1, k + 1 });
		l.add(new int[] { i + 1, j - 1, k + 1 });

		for (int[] p : l) {
			int ii = p[0];
			int jj = p[1];
			int kk = p[2];

			if (ii < 0 || jj < 0 || kk < 0 || ii >= numLats || jj >= numLons || kk >= numDays)
				continue;
			count += attributeMatrix[ii][jj][kk];
		}

		return count;
	}

	public static int adjacentCubes_sigmaW(int i, int j, int k) {
		int extreme = 0;

		if (i == 0 || i == numLats - 1)
			extreme++;

		if (j == 0 || j == numLons - 1)
			extreme++;

		if (k == 0 || k == numDays - 1)
			extreme++;

		if (extreme == 3)
			return 8;
		else if (extreme == 2)
			return 12;
		else if (extreme == 1)
			return 18;
		else
			return 27;
	}

	private static void printAttributeMatrix() {
		for (int i = 0; i < numLats; i++) {
			for (int j = 0; j < numLons; j++) {
				System.out.print(attributeMatrix[i][j][0]);
			}
			System.out.println();
		}
	}

	private static void printZScoreMatrix() {
		for (int i = 0; i < numLats; i++) {
			for (int j = 0; j < numLons; j++) {
				System.out.printf("%.2f ", zScoreMatrix[i][j][14]);
			}
			System.out.println();
		}
	}

	static class Point implements Comparable {
		int x;
		int y;
		int z;
		double score;

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Point point = (Point) o;

			return Double.compare(point.score, score) == 0;

		}

		@Override
		public int hashCode() {
			long temp = Double.doubleToLongBits(score);
			return (int) (temp ^ (temp >>> 32));
		}

		@Override
		public String toString() {
			return (int) (x + latMin * 100) + "," + (int) (y + lonMin * 100) + "," + z + "," + score;
		}

		@Override
		public int compareTo(Object o) {
			if (this.score > ((Point) o).score)
				return 1;
			else if (this.score < ((Point) o).score)
				return -1;
			return 0;

		}

		Point(int i, int j, int k, double s) {
			x = i;
			y = j;
			z = k;
			score = s;
		}

	}
}
