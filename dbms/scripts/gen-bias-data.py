#!/usr/bin/python3
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
    chunk_size = round((max_cardinality - min_cardinality) / float(count))
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
    query += "runningAccumulate(uniqCombinedRawState(UserID)) AS approx "
    query += "FROM data_source GROUP BY KeyID"
    return subprocess.check_output(["clickhouse-client", "--host", host, "--port", port, "--query", query])

def parse_clickhouse_response(response):
    parsed = []
    lines = response.decode().split("\n")
    for cur_line in lines:
        rows = cur_line.split("\t")
        if len(rows) == 2:
            parsed.append([float(rows[0]), float(rows[1])])
    return parsed

def accumulate_data(accumulated_data, data):
    if not accumulated_data:
        accumulated_data = deepcopy(data)
    else:
        for row1, row2 in zip(accumulated_data, data):
            row1[1] += row2[1];
    return accumulated_data

def generate_raw_result(accumulated_data, count):
    expected_tab = []
    bias_tab = []
    for row in accumulated_data:
        exact = row[0]
        expected = row[1] / count
        bias = expected - exact

        expected_tab.append(expected)
        bias_tab.append(bias)
    return [ expected_tab, bias_tab ]

def generate_sample(raw_estimates, biases, n_samples):
    result = []

    min_card = raw_estimates[0]
    max_card = raw_estimates[len(raw_estimates) - 1]
    step = (max_card - min_card) / (n_samples - 1)

    for i in range(0, n_samples + 1):
        x = min_card + i * step
        j = bisect.bisect_left(raw_estimates, x)

        if j == len(raw_estimates):
            result.append((raw_estimates[j - 1], biases[j - 1]))
        elif raw_estimates[j] == x:
            result.append((raw_estimates[j], biases[j]))
        else:
            # Найти 6 ближайших соседей. Вычислить среднее арифметическое.

            # 6 точек слева x [j-6 j-5 j-4 j-3 j-2 j-1]

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

            lim = min(len(T), len(U))
            k1 = 0
            k2 = 0

            while k1 < lim and k2 < lim:
                if T[k1] == U[k2]:
                    V.append(j - k1 - 1)
                    V.append(j + k2)
                    k1 = k1 + 1
                    k2 = k2 + 1
                elif T[k1] < U[k2]:
                    V.append(j - k1 - 1)
                    k1 = k1 + 1
                else:
                    V.append(j + k2)
                    k2 = k2 + 1

            if k1 < len(T):
                while k1 < len(T):
                    V.append(j - k1 - 1)
                    k1 = k1 + 1
            elif k2 < len(U):
                while k2 < len(U):
                    V.append(j + k2)
                    k2 = k2 + 1

            # Выбираем 6 ближайших точек.
            # Вычисляем средние.

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

    # Пропустить последовательные результаты, чьи оценки одинаковые.
    final_result = []
    last = -1
    for entry in result:
        if entry[0] != last:
            final_result.append((entry[0], entry[1]))
            last = entry[0]

    return final_result

def dump_arrays(data):

    print("Size of each array: {0}\n".format(len(data)))

    is_first = True
    sep = ''

    print("raw_estimates = ")
    print("{")
    for row in data:
        print("\t{0}{1}".format(sep, row[0]))
        if is_first == True:
            is_first = False
            sep = ","
    print("};")

    is_first = True
    sep = ""

    print("\nbiases = ")
    print("{")
    for row in data:
        print("\t{0}{1}".format(sep, row[1]))
        if is_first == True:
            is_first = False
            sep = ","
    print("};")

def start():
    parser = argparse.ArgumentParser(description = "Generate bias correction tables for HyperLogLog-based functions.")
    parser.add_argument("-x", "--host", default="localhost", help="ClickHouse server host name");
    parser.add_argument("-p", "--port", type=int, default=9000, help="ClickHouse server TCP port");
    parser.add_argument("-t", "--http_port", type=int, default=8123, help="ClickHouse server HTTP port");
    parser.add_argument("-i", "--iterations", type=int, default=5000, help="number of iterations");
    parser.add_argument("-m", "--min_cardinality", type=int, default=16384, help="minimal cardinality");
    parser.add_argument("-M", "--max_cardinality", type=int, default=655360, help="maximal cardinality");
    parser.add_argument("-s", "--samples", type=int, default=200, help="number of sampled values");
    args = parser.parse_args()

    accumulated_data = []

    for i in range(0, args.iterations):
        print(i + 1)
        sys.stdout.flush()

        generate_data_source(args.host, str(args.port), str(args.http_port), args.min_cardinality, args.max_cardinality, 1000)
        response = perform_query(args.host, str(args.port))
        data = parse_clickhouse_response(response)
        accumulated_data = accumulate_data(accumulated_data, data)

    result = generate_raw_result(accumulated_data, args.iterations)
    sampled_data = generate_sample(result[0], result[1], args.samples)
    dump_arrays(sampled_data)

if __name__ == "__main__": start()
