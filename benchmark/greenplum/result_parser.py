#!/usr/bin/env python3


import sys
import json


def parse_block(block=[], options=[]):

    # print('block is here', block)
    # show_query = False
    # show_query = options.show_query
    result = []
    query = block[0].strip()
    if len(block) > 4:
        timing1 = block[1].strip().split()[1]
        timing2 = block[3].strip().split()[1]
        timing3 = block[5].strip().split()[1]
    else:
        timing1 = block[1].strip().split()[1]
        timing2 = block[2].strip().split()[1]
        timing3 = block[3].strip().split()[1]
    if options.show_queries:
        result.append(query)
    if not options.show_first_timings:
        result += [timing1, timing2, timing3]
    else:
        result.append(timing1)
    return result


def read_stats_file(options, fname):
    result = []
    int_result = []
    block = []
    time_count = 1
    with open(fname) as f:

        for line in f.readlines():

            if "SELECT" in line:
                if len(block) > 1:
                    result.append(parse_block(block, options))
                block = [line]
            elif "Time:" in line:
                block.append(line)

    return result


def compare_stats_files(options, arguments):
    result = []
    file_output = []
    pyplot_colors = ["y", "b", "g", "r"]
    for fname in arguments[1:]:
        file_output.append((read_stats_file(options, fname)))
    if len(file_output[0]) > 0:
        timings_count = len(file_output[0])
    for idx, data_set in enumerate(file_output):
        int_result = []
        for timing in data_set:
            int_result.append(float(timing[0]))  # y values
        result.append(
            [
                [x for x in range(0, len(int_result))],
                int_result,
                pyplot_colors[idx] + "^",
            ]
        )
    #        result.append([x for x in range(1, len(int_result)) ]) #x values
    #        result.append( pyplot_colors[idx] + '^' )

    return result


def parse_args():
    from optparse import OptionParser

    parser = OptionParser(usage="usage: %prog [options] [result_file_path]..")
    parser.add_option(
        "-q",
        "--show-queries",
        help="Show statements along with timings",
        action="store_true",
        dest="show_queries",
    )
    parser.add_option(
        "-f",
        "--show-first-timings",
        help="Show only first tries timings",
        action="store_true",
        dest="show_first_timings",
    )
    parser.add_option(
        "-c",
        "--compare-mode",
        help="Prepare output for pyplot comparing result files.",
        action="store",
        dest="compare_mode",
    )
    (options, arguments) = parser.parse_args(sys.argv)
    if len(arguments) < 2:
        parser.print_usage()
        sys.exit(1)
    return (options, arguments)


def gen_pyplot_code(options, arguments):
    result = ""
    data_sets = compare_stats_files(options, arguments)
    for idx, data_set in enumerate(data_sets, start=0):
        x_values, y_values, line_style = data_set
        result += "\nplt.plot("
        result += "%s, %s, '%s'" % (x_values, y_values, line_style)
        result += ", label='%s try')" % idx
    print("import matplotlib.pyplot as plt")
    print(result)
    print("plt.xlabel('Try number')")
    print("plt.ylabel('Timing')")
    print("plt.title('Benchmark query timings')")
    print("plt.legend()")
    print("plt.show()")


def gen_html_json(options, arguments):
    tuples = read_stats_file(options, arguments[1])
    print("{")
    print('"system:       GreenPlum(x2),')
    print(('"version":      "%s",' % "4.3.9.1"))
    print('"data_size":    10000000,')
    print('"time":         "",')
    print('"comments":     "",')
    print('"result":')
    print("[")
    for s in tuples:
        print(s)
    print("]")
    print("}")


def main():
    (options, arguments) = parse_args()
    if len(arguments) > 2:
        gen_pyplot_code(options, arguments)
    else:
        gen_html_json(options, arguments)


if __name__ == "__main__":
    main()
