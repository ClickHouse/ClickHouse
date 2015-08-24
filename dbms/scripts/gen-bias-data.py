#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import tempfile
import random
import subprocess
import bisect
from copy import deepcopy

def generate_query(n_samples):
    n1 = random.randrange(0, 32767)
    n2 = random.randrange(0, 32767)
    query =  "SELECT exact, approx FROM (SELECT UserID % 1000 AS k, runningAccumulate(uniqCombinedRawState(UserID)) "
    query += "AS approx, runningAccumulate(uniqExactState(UserID)) AS exact FROM "
    query += "(SELECT sipHash64(reinterpretAsString((number + {0}) * {1})) AS UserID FROM system.numbers "
    query += "LIMIT {2}) GROUP BY k ORDER BY k)"
    return query.format(n1, n2, n_samples)

def perform_query(host, port, query):
    return subprocess.check_output(["clickhouse-client", "--host", host, "--port", port, "--query", query])

def parse_result(output):
    parsed = []
    lines = output.split('\n')
    for cur_line in lines:
        rows = cur_line.split('\t')
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
			for k in xrange(end, begin, -1):
				T.append(x - raw_estimates[k])

			# 6 точек справа x [j j+1 j+2 j+3 j+4 j+5]

			begin = j
			end = min(j + 5, len(raw_estimates) - 1) + 1

			U = []
			for k in xrange(begin, end):
				U.append(raw_estimates[k] - x)

			# Сливаем расстояния.

			V = []

			end = min(len(T), len(U))
			lim = len(T) + len(U)
			k1 = 0
			k2 = 0
			for k in xrange(0, lim):
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
			for k in xrange(begin, end):
				sum += raw_estimates[V[k]]
				bias += biases[V[k]]
			sum /= float(end)
			bias /= float(end)

			result.append((sum, bias))

	return result

def dump_tables(stats):
	is_first = True
	sep = ''

	print "// For UniqCombinedBiasData::getRawEstimates():"
	print "{"
	for row in stats:
		print "\t{0}{1}".format(sep, row[0])
		if is_first == True:
			is_first = False
			sep = ','
	print "}"

	is_first = True
	sep = ''

	print "\n// For UniqCombinedBiasData::getBiases():"
	print "{"
	for row in stats:
		print "\t{0}{1}".format(sep, row[1])
		if is_first == True:
			is_first = False
			sep = ','
	print "}"

def start():
	parser = argparse.ArgumentParser(description = 'Generate bias correction tables.')
	parser.add_argument('-x', '--host', default='127.0.0.1', help='clickhouse host name');
	parser.add_argument('-p', '--port', type=int, default=9000, help='clickhouse port');
	parser.add_argument('-i', '--iterations', type=int, default=5000, help='number of iterations');
	parser.add_argument('-s', '--samples', type=int, default=700000, help='number of sample values');
	parser.add_argument('-g', '--generated', type=int, default=200, help='number of generated values');
	args = parser.parse_args()

	stats = []

	for i in range(0, args.iterations):
		query = generate_query(args.samples)
		output = perform_query(args.host, str(args.port), query)
		data = parse_result(output)
		stats = accumulate(stats, data)

	result = generate_result(stats, args.iterations)
	sample = generate_sample(result[0], result[1], args.generated)
	dump_tables(sample)

if __name__ == "__main__": start()
