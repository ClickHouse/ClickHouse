#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import argparse
import tempfile
import random
import subprocess
import bisect
from copy import deepcopy

def generate_data_source(host, port, begin, end, count):
	chunk_size = round((end - begin) / float(count))
	used_values = set()

	cur_count = 0
	next_size = 0
	i = 0
	j = 0

	with tempfile.TemporaryDirectory() as tmp_dir:
		filename = tmp_dir + '/table.txt'
		file_handle = open(filename, 'w+b')

		while cur_count < count:
			next_size += chunk_size

			while len(used_values) < next_size:
				h = random.randrange(begin, end + 1)
				used_values.add(h)
				outstr = str(h) + "\t" + str(j) + "\n";
				file_handle.write(bytes(str(outstr), 'UTF-8'));
				i = i + 1
			cur_count = cur_count + 1
			j = j + 1

		file_handle.close()

		host = 'localhost'
		port = 9000

		query = 'DROP TABLE IF EXISTS data_source'
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])
		query = 'CREATE TABLE data_source(UserID UInt64, KeyID UInt64) ENGINE=TinyLog'
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])

		cat = subprocess.Popen(('cat', filename), stdout=subprocess.PIPE)
		subprocess.check_output(('POST', 'http://localhost:8123/?query=INSERT INTO data_source FORMAT TabSeparated'), stdin=cat.stdout)
		cat.wait()

def perform_query(host, port):
    query  = "SELECT runningAccumulate(uniqExactState(UserID), KeyID) AS exact, "
    query += "runningAccumulate(uniqCombinedRawState(UserID), KeyID) AS approx "
    query += "FROM data_source GROUP BY KeyID"
    return subprocess.check_output(["clickhouse-client", "--host", host, "--port", port, "--query", query])

def parse_result(output):
    parsed = []
    lines = output.decode().split("\n")
    for cur_line in lines:
        rows = cur_line.split("\t")
        if len(rows) == 2:
            parsed.append([float(rows[0]), float(rows[1])])
    return parsed

def accumulate(stats, data):
    if not stats:
        stats = deepcopy(data)
    else:
        for row1, row2 in zip(stats, data):
            row1[1] += row2[1];
    return stats

def generate_result(stats, count):
	expected_tab = []
	bias_tab = []
	for row in stats:
		exact = row[0]
		expected = row[1] / count
		bias = expected - exact

		expected_tab.append(expected)
		bias_tab.append(bias)
	return [ expected_tab, bias_tab ]

def generate_sample(raw_estimates, biases, n_generated):
	result = []

	min_card = raw_estimates[0]
	max_card = raw_estimates[len(raw_estimates) - 1]
	step = (max_card - min_card) / n_generated

	for i in range(0, n_generated + 1):
		x = min_card + i * step
		j = bisect.bisect_left(raw_estimates, x)

		if j == len(raw_estimates):
			result.append((raw_estimates[j - 1], biases[j - 1]))
		elif raw_estimates[j] == x:
			result.append((raw_estimates[j], biases[j]))
		else:
			# Найти 6 ближайших соседей. Вычислить среднее арифметическое.

			# 6 точек слева x [ j-6 j-5 j-4 j-3 j-2 j-1]

			begin = max(j - 6, 0) - 1
			end = j - 1

			T = []
			for k in range(end, begin, -1):
				T.append(x - raw_estimates[k])

			# 6 точек справа x [j j+1 j+2 j+3 j+4 j+5]

			begin = j
			end = min(j + 5, len(raw_estimates) - 1) + 1

			U = []
			for k in range(begin, end):
				U.append(raw_estimates[k] - x)

			# Сливаем расстояния.

			V = []

			end = min(len(T), len(U))
			lim = len(T) + len(U)
			k1 = 0
			k2 = 0
			for k in range(0, lim):
				if k1 < end and T[k1] < U[k1]:
					V.append(j - k1 - 1)
					k1 = k1 + 1
				else:
					V.append(j + k2)
					k2 = k2 + 1

			# Выбираем 6 ближайших точек.
			# Вычилсяем средние.

			begin = 0
			end = min(len(V), 6)

			sum = 0
			bias = 0
			for k in range(begin, end):
				sum += raw_estimates[V[k]]
				bias += biases[V[k]]
			sum /= float(end)
			bias /= float(end)

			result.append((sum, bias))

	return result

def dump_tables(stats):
	is_first = True
	sep = ''

	print("// For UniqCombinedBiasData::getRawEstimates():")
	print("{")
	for row in stats:
		print("\t{0}{1}".format(sep, row[0]))
		if is_first == True:
			is_first = False
			sep = ","
	print("}")

	is_first = True
	sep = ""

	print("\n// For UniqCombinedBiasData::getBiases():")
	print("{")
	for row in stats:
		print("\t{0}{1}".format(sep, row[1]))
		if is_first == True:
			is_first = False
			sep = ","
	print("}")

def start():
	parser = argparse.ArgumentParser(description = "Generate bias correction tables.")
	parser.add_argument("-x", "--host", default="127.0.0.1", help="clickhouse host name");
	parser.add_argument("-p", "--port", type=int, default=9000, help="clickhouse port");
	parser.add_argument("-i", "--iterations", type=int, default=5000, help="number of iterations");
	parser.add_argument("-s", "--samples", type=int, default=700000, help="number of sample values");
	parser.add_argument("-g", "--generated", type=int, default=200, help="number of generated values");
	args = parser.parse_args()

	stats = []

	for i in range(0, args.iterations):
		print(i)
		generate_data_source(args.host, str(args.port), 0, args.samples, 1000)
		output = perform_query(args.host, str(args.port))
		data = parse_result(output)
		stats = accumulate(stats, data)

	result = generate_result(stats, args.iterations)
	sample = generate_sample(result[0], result[1], args.generated)
	dump_tables(sample)

if __name__ == "__main__": start()
