"""report.py - Utilities for reporting statistics about benchmark results
"""
import os
import re
import copy

class BenchmarkColor(object):
    def __init__(self, name, code):
        self.name = name
        self.code = code

    def __repr__(self):
        return '%s%r' % (self.__class__.__name__,
                         (self.name, self.code))

    def __format__(self, format):
        return self.code

# Benchmark Colors Enumeration
BC_NONE = BenchmarkColor('NONE', '')
BC_MAGENTA = BenchmarkColor('MAGENTA', '\033[95m')
BC_CYAN = BenchmarkColor('CYAN', '\033[96m')
BC_OKBLUE = BenchmarkColor('OKBLUE', '\033[94m')
BC_HEADER = BenchmarkColor('HEADER', '\033[92m')
BC_WARNING = BenchmarkColor('WARNING', '\033[93m')
BC_WHITE = BenchmarkColor('WHITE', '\033[97m')
BC_FAIL = BenchmarkColor('FAIL', '\033[91m')
BC_ENDC = BenchmarkColor('ENDC', '\033[0m')
BC_BOLD = BenchmarkColor('BOLD', '\033[1m')
BC_UNDERLINE = BenchmarkColor('UNDERLINE', '\033[4m')

def color_format(use_color, fmt_str, *args, **kwargs):
    """
    Return the result of 'fmt_str.format(*args, **kwargs)' after transforming
    'args' and 'kwargs' according to the value of 'use_color'. If 'use_color'
    is False then all color codes in 'args' and 'kwargs' are replaced with
    the empty string.
    """
    assert use_color is True or use_color is False
    if not use_color:
        args = [arg if not isinstance(arg, BenchmarkColor) else BC_NONE
                for arg in args]
        kwargs = {key: arg if not isinstance(arg, BenchmarkColor) else BC_NONE
                  for key, arg in kwargs.items()}
    return fmt_str.format(*args, **kwargs)


def find_longest_name(benchmark_list):
    """
    Return the length of the longest benchmark name in a given list of
    benchmark JSON objects
    """
    longest_name = 1
    for bc in benchmark_list:
        if len(bc['name']) > longest_name:
            longest_name = len(bc['name'])
    return longest_name


def calculate_change(old_val, new_val):
    """
    Return a float representing the decimal change between old_val and new_val.
    """
    if old_val == 0 and new_val == 0:
        return 0.0
    if old_val == 0:
        return float(new_val - old_val) / (float(old_val + new_val) / 2)
    return float(new_val - old_val) / abs(old_val)


def filter_benchmark(json_orig, family, replacement=""):
    """
    Apply a filter to the json, and only leave the 'family' of benchmarks.
    """
    regex = re.compile(family)
    filtered = {}
    filtered['benchmarks'] = []
    for be in json_orig['benchmarks']:
        if not regex.search(be['name']):
            continue
        filteredbench = copy.deepcopy(be) # Do NOT modify the old name!
        filteredbench['name'] = regex.sub(replacement, filteredbench['name'])
        filtered['benchmarks'].append(filteredbench)
    return filtered


def generate_difference_report(json1, json2, use_color=True):
    """
    Calculate and report the difference between each test of two benchmarks
    runs specified as 'json1' and 'json2'.
    """
    first_col_width = find_longest_name(json1['benchmarks'])
    def find_test(name):
        for b in json2['benchmarks']:
            if b['name'] == name:
                return b
        return None
    first_col_width = max(first_col_width, len('Benchmark'))
    first_line = "{:<{}s}Time             CPU      Time Old      Time New       CPU Old       CPU New".format(
        'Benchmark', 12 + first_col_width)
    output_strs = [first_line, '-' * len(first_line)]

    gen = (bn for bn in json1['benchmarks'] if 'real_time' in bn and 'cpu_time' in bn)
    for bn in gen:
        other_bench = find_test(bn['name'])
        if not other_bench:
            continue

        if bn['time_unit'] != other_bench['time_unit']:
            continue

        def get_color(res):
            if res > 0.05:
                return BC_FAIL
            elif res > -0.07:
                return BC_WHITE
            else:
                return BC_CYAN
        fmt_str = "{}{:<{}s}{endc}{}{:+16.4f}{endc}{}{:+16.4f}{endc}{:14.0f}{:14.0f}{endc}{:14.0f}{:14.0f}"
        tres = calculate_change(bn['real_time'], other_bench['real_time'])
        cpures = calculate_change(bn['cpu_time'], other_bench['cpu_time'])
        output_strs += [color_format(use_color, fmt_str,
            BC_HEADER, bn['name'], first_col_width,
            get_color(tres), tres, get_color(cpures), cpures,
            bn['real_time'], other_bench['real_time'],
            bn['cpu_time'], other_bench['cpu_time'],
            endc=BC_ENDC)]
    return output_strs

###############################################################################
# Unit tests

import unittest

class TestReportDifference(unittest.TestCase):
    def load_results(self):
        import json
        testInputs = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Inputs')
        testOutput1 = os.path.join(testInputs, 'test1_run1.json')
        testOutput2 = os.path.join(testInputs, 'test1_run2.json')
        with open(testOutput1, 'r') as f:
            json1 = json.load(f)
        with open(testOutput2, 'r') as f:
            json2 = json.load(f)
        return json1, json2

    def test_basic(self):
        expect_lines = [
            ['BM_SameTimes', '+0.0000', '+0.0000', '10', '10', '10', '10'],
            ['BM_2xFaster', '-0.5000', '-0.5000', '50', '25', '50', '25'],
            ['BM_2xSlower', '+1.0000', '+1.0000', '50', '100', '50', '100'],
            ['BM_1PercentFaster', '-0.0100', '-0.0100', '100', '99', '100', '99'],
            ['BM_1PercentSlower', '+0.0100', '+0.0100', '100', '101', '100', '101'],
            ['BM_10PercentFaster', '-0.1000', '-0.1000', '100', '90', '100', '90'],
            ['BM_10PercentSlower', '+0.1000', '+0.1000', '100', '110', '100', '110'],
            ['BM_100xSlower', '+99.0000', '+99.0000', '100', '10000', '100', '10000'],
            ['BM_100xFaster', '-0.9900', '-0.9900', '10000', '100', '10000', '100'],
            ['BM_10PercentCPUToTime', '+0.1000', '-0.1000', '100', '110', '100', '90'],
            ['BM_ThirdFaster', '-0.3333', '-0.3334', '100', '67', '100', '67'],
            ['BM_BadTimeUnit', '-0.9000', '+0.2000', '0', '0', '0', '1'],
        ]
        json1, json2 = self.load_results()
        output_lines_with_header = generate_difference_report(json1, json2, use_color=False)
        output_lines = output_lines_with_header[2:]
        print("\n".join(output_lines_with_header))
        self.assertEqual(len(output_lines), len(expect_lines))
        for i in range(0, len(output_lines)):
            parts = [x for x in output_lines[i].split(' ') if x]
            self.assertEqual(len(parts), 7)
            self.assertEqual(parts, expect_lines[i])


class TestReportDifferenceBetweenFamilies(unittest.TestCase):
    def load_result(self):
        import json
        testInputs = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Inputs')
        testOutput = os.path.join(testInputs, 'test2_run.json')
        with open(testOutput, 'r') as f:
            json = json.load(f)
        return json

    def test_basic(self):
        expect_lines = [
            ['.', '-0.5000', '-0.5000', '10', '5', '10', '5'],
            ['./4', '-0.5000', '-0.5000', '40', '20', '40', '20'],
            ['Prefix/.', '-0.5000', '-0.5000', '20', '10', '20', '10'],
            ['Prefix/./3', '-0.5000', '-0.5000', '30', '15', '30', '15'],
        ]
        json = self.load_result()
        json1 = filter_benchmark(json, "BM_Z.ro", ".")
        json2 = filter_benchmark(json, "BM_O.e", ".")
        output_lines_with_header = generate_difference_report(json1, json2, use_color=False)
        output_lines = output_lines_with_header[2:]
        print("\n")
        print("\n".join(output_lines_with_header))
        self.assertEqual(len(output_lines), len(expect_lines))
        for i in range(0, len(output_lines)):
            parts = [x for x in output_lines[i].split(' ') if x]
            self.assertEqual(len(parts), 7)
            self.assertEqual(parts, expect_lines[i])


if __name__ == '__main__':
    unittest.main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# kate: tab-width: 4; replace-tabs on; indent-width 4; tab-indents: off;
# kate: indent-mode python; remove-trailing-spaces modified;
