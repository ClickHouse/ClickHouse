#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import sys
import argparse
import tempfile
import random
import subprocess
import bisect
from copy import deepcopy

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

def generate_data_source(host, port, http_port, begin, end, count):
	chunk_size = round((end - begin) / float(count))
	used_values = 0

	cur_count = 0
	next_size = 0

	sup = 32768
	n1 = random.randrange(0, sup)
	n2 = random.randrange(0, sup)
	urng = UniqueRandomGenerator(n1, n2)

	with tempfile.TemporaryDirectory() as tmp_dir:
		filename = tmp_dir + '/table.txt'
		file_handle = open(filename, 'w+b')

		while cur_count < count:
			next_size += chunk_size

			while used_values < next_size:
				h = urng.next()
				used_values = used_values + 1
				outstr = str(h) + "\t" + str(cur_count) + "\n";
				file_handle.write(bytes(outstr, 'UTF-8'));

			cur_count = cur_count + 1

		file_handle.close()

		query = 'DROP TABLE IF EXISTS data_source'
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])
		query = 'CREATE TABLE data_source(UserID UInt64, KeyID UInt64) ENGINE=TinyLog'
		subprocess.check_output(["clickhouse-client", "--host", host, "--port", str(port), "--query", query])

		cat = subprocess.Popen(("cat", filename), stdout=subprocess.PIPE)
		subprocess.check_output(("POST", "http://localhost:{0}/?query=INSERT INTO data_source FORMAT TabSeparated".format(http_port)), stdin=cat.stdout)
		cat.wait()

def perform_query(host, port):
    query  = "SELECT runningAccumulate(uniqExactState(UserID)) AS exact, "
    query += "runningAccumulate(uniqCombinedRawState(UserID)) AS raw, "
    query += "runningAccumulate(uniqCombinedLinearCountingState(UserID)) AS linear_counting, "
    query += "runningAccumulate(uniqCombinedBiasCorrectedState(UserID)) AS bias_corrected "
    query += "FROM data_source GROUP BY KeyID"
    return subprocess.check_output(["clickhouse-client", "--host", host, "--port", port, "--query", query])

def parse_result(output):
    parsed = []
    lines = output.decode().split("\n")
    for cur_line in lines:
        rows = cur_line.split("\t")
        if len(rows) == 4:
            parsed.append([float(rows[0]), float(rows[1]), float(rows[2]), float(rows[3])])
    return parsed

def accumulate(stats, data):
    if not stats:
        stats = deepcopy(data)
    else:
        for row1, row2 in zip(stats, data):
            row1[1] += row2[1];
            row1[2] += row2[2];
            row1[3] += row2[3];
    return stats

def generate_result(stats, count):
	fh1 = open("raw_graph.txt", "w+b")
	fh2 = open("linear_counting_graph.txt", "w+b")
	fh3 = open("bias_corrected_graph.txt", "w+b")

	expected_tab = []
	bias_tab = []
	for row in stats:
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
	parser = argparse.ArgumentParser(description = "Generate bias correction tables.")
	parser.add_argument("-x", "--host", default="127.0.0.1", help="clickhouse host name");
	parser.add_argument("-p", "--port", type=int, default=9000, help="clickhouse client TCP port");
	parser.add_argument("-t", "--http_port", type=int, default=8123, help="clickhouse HTTP port");
	parser.add_argument("-i", "--iterations", type=int, default=5000, help="number of iterations");
	parser.add_argument("-s", "--samples", type=int, default=700000, help="number of sample values");
	parser.add_argument("-g", "--generated", type=int, default=200, help="number of generated values");
	args = parser.parse_args()

	stats = []

	for i in range(0, args.iterations):
		print(i + 1)
		sys.stdout.flush()
		generate_data_source(args.host, str(args.port), str(args.http_port), 0, args.samples, 1000)
		output = perform_query(args.host, str(args.port))
		data = parse_result(output)
		stats = accumulate(stats, data)

	generate_result(stats, args.iterations)

if __name__ == "__main__": start()
