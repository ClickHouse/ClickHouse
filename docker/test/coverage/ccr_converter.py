import sys
from typing import TextIO
from time import time
from collections import Counter
import argparse

abs_path_to_src = ""
files = []
tests = []
tests_names = []

def convert_to_slim_genhtml_report(file_name: str):
    elapsed = time()
    data = {}

    with open(file_name, "w") as f:
        f.write("TN:global_report\n")

        for test in tests:
            for source_id, (test_funcs_hit, test_lines_hit) in test.items():
                if source_id not in data:
                    data[source_id] = set(test_funcs_hit), set(test_lines_hit)
                else:
                    funcs_hit, lines_hit = data[source_id]
                    funcs_hit.update(test_funcs_hit)
                    lines_hit.update(test_lines_hit)

        for i, (rel_path, funcs_instrumented, lines_instrumented) in enumerate(files):
            f.write("SF:{}\n".format(abs_path_to_src + rel_path))

            funcs_hit, lines_hit = data[i] if i in data else (set(), set())

            f.write("FNF:{}\nFNH:{}\n".format(len(funcs_instrumented), len(funcs_hit)))
            f.write("LF:{}\nLH:{}\n".format(len(lines_instrumented), len(lines_hit)))

            for edge_index, (func_name, func_line) in funcs_instrumented.items():
                f.write("FNDA:{0},{1}\nFN:{2},{1}\n".format(
                    1 if edge_index in funcs_hit else 0, func_name, func_line))

            for line in lines_instrumented:
                f.write("DA:{},{}\n".format(line, 1 if line in lines_hit else 0))

            f.write("end_of_record\n")

    print("Wrote the report, took {}s.".format(int(time() - elapsed)))

def read_report(f: TextIO):
    global abs_path_to_src
    global files
    global tests
    global tests_names

    abs_path_to_src = f.readline().strip()

    elapsed = time()

    for i in range(int(f.readline().split()[1])): # files
        rel_path, funcs_count, lines_count = f.readline().split()

        funcs = {}

        for j in range(int(funcs_count)):
            mangled_name, start_line, edge_index = f.readline().split()
            funcs[int(edge_index)] = mangled_name, int(start_line)

        lines = [int(f.readline()) for j in range(int(lines_count))]

        files.append([rel_path, funcs, lines])

    tests_sources = {}

    while True:
        token = f.readline()

        if token == "TEST\n":
            if len(tests_sources) > 0:
                tests.append(tests_sources)
                tests_sources = {}
            token = f.readline()
        elif not token.startswith("SOURCE"):
            if len(tests_sources) > 0:
                tests.append(tests_sources)
            break

        source_id = int(token.split()[1])
        funcs = list(map(int, next(f).split()))
        lines = list(map(int, next(f).split()))

        tests_sources[source_id] = funcs, lines

    tests_names = f.readlines()

    print("Read the report, took {}s. {} tests, {} source files".format(
        int(time() - elapsed), len(tests), len(files)))

def main():
    parser = argparse.ArgumentParser(prog='CCR converter')
    parser.add_argument('ccr_report_file')

    parser.add_argument('--genhtml-slim-report', nargs=1,
        help="Merges all tests data into a single .info report. Per-test data is not preserved")

    args = parser.parse_args()

    with open(args.ccr_report_file, "r") as f:
        read_report(f)

    if args.genhtml_slim_report is not None:
        convert_to_slim_genhtml_report(args.genhtml_slim_report)

main()
