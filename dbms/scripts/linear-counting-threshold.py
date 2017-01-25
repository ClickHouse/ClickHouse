#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import sys
import argparse
import tempfile
import random
import subprocess
import bisect
from copy import deepcopy

# Псевдослучайный генератор уникальных чисел.
# http://preshing.com/20121224/how-to-generate-a-sequence-of-unique-random-integers/
class UniqueRandomGenerator:
	prime = 4294967291

	def __init__(self, seed_base, seed_offset):
		self.index = self.permutePQR(self.permutePQR(seed_base) + 0x682f0161)
		self.intermediate_offset = self.permutePQR(self.permutePQR(seed_offset) + 0x46790905)

	def next(self):
		val = self.permutePQR((self.permutePQR(self.index) + self.intermediate_offset) ^ 0x5bf03635)
		self.index = self.index + 1
		return val

	def permutePQR(self, x):
		if x >=self.prime:
			return x
		else:
			residue = (x * x) % self.prime
			if x <= self.prime/2:
				return residue
			else:
				return self.prime - residue

# Создать таблицу содержащую уникальные значения.
def generate_data_source(host, port, http_port, min_cardinality, max_cardinality, count):
	chunk_size = round((max_cardinality - (min_cardinality + 1)) / float(count))
	used_values = 0

	cur_count = 0
	next_size = 0

	sup = 32768
	n1 = random.randrange(0, sup)
	n2 = random.randrange(0, sup)
	urng = UniqueRandomGenerator(n1, n2)

	is_first = True

	with tempfile.TemporaryDirectory() as tmp_dir:
		filename = tmp_dir + '/table.txt'
		with open(filename, 'w+b') as file_handle:
			while cur_count < count:

				if is_first == True:
					is_first = False
					if min_cardinality != 0:
						next_size = min_cardinality + 1
					else:
						next_size = chunk_size
				else:
					next_size += chunk_size

				while used_values < next_size:
					h = urng.next()
					used_values = used_values + 1
					out = str(h) + "\t" + str(cur_count) + "\n";
					file_handle.write(bytes(out, 'UTF-8'));
				cur_count = cur_count + 1

		query = "DROP TABLE IF EXISTS data_source"
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])
		query = "CREATE TABLE data_source(UserID UInt64, KeyID UInt64) ENGINE=TinyLog"
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])

		cat = subprocess.Popen(("cat", filename), stdout=subprocess.PIPE)
		subprocess.check_output(("POST", "http://{0}:{1}/?query=INSERT INTO data_source FORMAT TabSeparated".format(host, http_port)), stdin=cat.stdout)
		cat.wait()

def perform_query(host, port):
    query  = "SELECT runningAccumulate(uniqExactState(UserID)) AS exact, "
    query += "runningAccumulate(uniqCombinedRawState(UserID)) AS raw, "
    query += "runningAccumulate(uniqCombinedLinearCountingState(UserID)) AS linear_counting, "
    query += "runningAccumulate(uniqCombinedBiasCorrectedState(UserID)) AS bias_corrected "
    query += "FROM data_source GROUP BY KeyID"
    return subprocess.check_output(["clickhouse-client", "--host", host, "--port", port, "--query", query])

def parse_clickhouse_response(response):
    parsed = []
    lines = response.decode().split("\n")
    for cur_line in lines:
        rows = cur_line.split("\t")
        if len(rows) == 4:
            parsed.append([float(rows[0]), float(rows[1]), float(rows[2]), float(rows[3])])
    return parsed

def accumulate_data(accumulated_data, data):
    if not accumulated_data:
        accumulated_data = deepcopy(data)
    else:
        for row1, row2 in zip(accumulated_data, data):
            row1[1] += row2[1];
            row1[2] += row2[2];
            row1[3] += row2[3];
    return accumulated_data

def dump_graphs(data, count):
	with open("raw_graph.txt", "w+b") as fh1, open("linear_counting_graph.txt", "w+b") as fh2, open("bias_corrected_graph.txt", "w+b") as fh3:
		expected_tab = []
		bias_tab = []
		for row in data:
			exact = row[0]
			raw = row[1] / count;
			linear_counting = row[2] / count;
			bias_corrected = row[3] / count;

			outstr = "{0}\t{1}\n".format(exact, abs(raw - exact) / exact)
			fh1.write(bytes(outstr, 'UTF-8'))

			outstr = "{0}\t{1}\n".format(exact, abs(linear_counting - exact) / exact)
			fh2.write(bytes(outstr, 'UTF-8'))

			outstr = "{0}\t{1}\n".format(exact, abs(bias_corrected - exact) / exact)
			fh3.write(bytes(outstr, 'UTF-8'))

def start():
	parser = argparse.ArgumentParser(description = "Generate graphs that help to determine the linear counting threshold.")
	parser.add_argument("-x", "--host", default="localhost", help="clickhouse host name");
	parser.add_argument("-p", "--port", type=int, default=9000, help="clickhouse client TCP port");
	parser.add_argument("-t", "--http_port", type=int, default=8123, help="clickhouse HTTP port");
	parser.add_argument("-i", "--iterations", type=int, default=5000, help="number of iterations");
	parser.add_argument("-m", "--min_cardinality", type=int, default=16384, help="minimal cardinality");
	parser.add_argument("-M", "--max_cardinality", type=int, default=655360, help="maximal cardinality");
	args = parser.parse_args()

	accumulated_data = []

	for i in range(0, args.iterations):
		print(i + 1)
		sys.stdout.flush()

		generate_data_source(args.host, str(args.port), str(args.http_port), args.min_cardinality, args.max_cardinality, 1000)
		response = perform_query(args.host, str(args.port))
		data = parse_clickhouse_response(response)
		accumulated_data = accumulate_data(accumulated_data, data)

	dump_graphs(accumulated_data, args.iterations)

if __name__ == "__main__": start()
