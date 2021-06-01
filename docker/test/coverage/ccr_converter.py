from typing import TextIO
from time import time
from collections import Counter
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import CppLexer
from jinja2 import Environment, FileSystemLoader
from tqdm import tqdm
from copy import deepcopy

import sys
import argparse
import os.path

bounds = { # success bound, warning bound, %
    "lines": [90, 75],
    "funcs": [90, 75],
    "tests": [20, 1]
    }

colors = { # not covered, covered
    "lines": ["#fbd08e", "#deffb7"],
    "funcs": ["#ffb1b1", "#f5c3ff"]
}

files = []
tests = []
tests_names = []

args = None

env = None

class FileEntry:
    def __init__(self, path, lines_with_hits, funcs_with_hits, tests_with_hits):
        self.full_path = path

        self.name = path.split("/")[-1]
        self.url = os.path.join(args.out_dir, path) + ".html"

        a, b, c, d = get_hit(lines_with_hits, funcs_with_hits)

        self.lines = a, b, percent(a, b)
        self.funcs = c, d, percent(c, d)

        self.tests = len(tests_with_hits), percent(len(tests_with_hits), len(tests))

        self.lines_with_hits = lines_with_hits
        self.funcs_with_hits = funcs_with_hits
        self.tests_with_hits = tests_with_hits

class DirEntry:
    def __init__(self, path):
        self.name = path
        self.url = path + "/index.html"

        self.files = []

        self.lines = 0, 0, 0
        self.funcs = 0, 0, 0
        self.tests = 0, 0

        self.tests_with_hits = set()

    def add(self, f):
        self.files.append(f)

        line_hit, line_total = self.lines[0] + f.lines[0], self.lines[1] + f.lines[1]
        func_hit, func_total = self.funcs[0] + f.funcs[0], self.funcs[1] + f.funcs[1]

        test_hit = max(self.tests[0], f.tests[0])

        self.lines = line_hit, line_total, percent(line_hit, line_total)
        self.funcs = func_hit, func_total, percent(func_hit, func_total)
        self.tests = test_hit, percent(test_hit, len(tests))

        self.tests_with_hits.update(f.tests_with_hits)

class CodeFormatter(HtmlFormatter):
    def __init__(self, covered_lines, not_covered_lines, covered_funcs, not_covered_funcs):
        super(CodeFormatter, self).__init__(linenos=True, lineanchors="line")

        self.covered_lines = covered_lines
        self.not_covered_lines= not_covered_lines
        self.covered_funcs = covered_funcs
        self.not_covered_funcs = not_covered_funcs

    def wrap(self, source, outfile):
        return self._wrap_code(source)

    def _wrap_code(self, source):
        frmt = '<span style="background-color:{}">{}</span>'

        yield 0, '<div class="highlight"><pre>'

        for i, (t, value) in enumerate(source):
            if t != 1:
                yield t, value

            if i + 1 in self.covered_lines:
                yield 1, frmt.format(colors["lines"][1], value)
            elif i + 1 in self.not_covered_lines:
                yield 1, frmt.format(colors["lines"][0], value)
            elif i + 1 in self.covered_funcs:
                yield 1, frmt.format(colors["funcs"][1], value)
            elif i + 1 in self.not_covered_funcs:
                yield 1, frmt.format(colors["funcs"][0], value)
            else:
                yield 1, value

        yield 0, '</pre></div>'

def percent(a, b):
  return 0 if b == 0 else int(a * 100 / b)

def get_hit(lines, funcs):
    return len([e for e in lines if e[1]]), len(lines), \
           len([e for e in funcs if e[1]]), len(funcs)

def accumulate_data():
    data = {}

    for test in tests:
        for source_id, (test_funcs_hit, test_lines_hit) in test.items():
            if source_id not in data:
                data[source_id] = set(test_funcs_hit), set(test_lines_hit)
            else:
                funcs_hit, lines_hit = data[source_id]
                funcs_hit.update(test_funcs_hit)
                lines_hit.update(test_lines_hit)

    return data

def get_entries():
    out = []
    data = accumulate_data()
    prefix = ""

    for i, (path, funcs_instrumented, lines_instrumented) in enumerate(files):
        path = os.path.normpath(path)

        dirs, file_name = os.path.split(path)

        funcs_hit, lines_hit = data[i] if i in data else (set(), set())

        lines_with_hits = [(line, line in lines_hit) for line in lines_instrumented]
        funcs_with_hits = [(line, index in funcs_hit) for index, (_, line) in funcs_instrumented.items()]
        tests_with_hits = [j for j, test in enumerate(tests) if i in test.keys()]

        test_file = FileEntry(path, lines_with_hits, funcs_with_hits, tests_with_hits)

        found = False

        for d in out:
            if d.name == dirs:
                d.add(test_file)
                found = True
                break

        if not found:
            out.append(DirEntry(dirs))
            out[-1].add(test_file)

    return sorted(out, key=lambda x: x.name)

def generate_html():
    entries = get_entries()

    root_entry = DirEntry("")
    files_entry = DirEntry("")

    for dir_entry in tqdm(entries):
        root_entry.add(dir_entry)

        generate_dir_page(dir_entry)

        for sf_entry in tqdm(dir_entry.files):
            generate_file_page(sf_entry)
            files_entry.add(sf_entry)

    generate_dir_page(root_entry)

    for e in files_entry.files:
        e.name = e.full_path

    generate_dir_page(files_entry, page_name="files.html")

def render(tpl, **kwargs):
    kwargs.update({
        "bounds": bounds,
        "colors": colors,
        "tests_total": len(tests),
        "root_url": os.path.join(args.out_dir, "index.html"),
        "index_url": os.path.join(args.out_dir, "files.html")
    })

    return env.get_template(tpl).render(kwargs)

def generate_dir_page(entry: DirEntry, page_name="index.html"):
    path = os.path.join(args.out_dir, entry.name)
    os.makedirs(path, exist_ok=True)

    dir_page = os.path.join(path, page_name)

    entries = sorted(entry.files, key=lambda x: x.name)

    special_entry = deepcopy(entry)
    special_entry.url = "./index.html"

    with open(dir_page, "w") as f:
        f.write(render("directory.html", entries=entries, special_entry=special_entry))

def generate_file_page(entry: FileEntry):
    src_file_path = os.path.join(args.sources_dir, entry.full_path)

    if not os.path.exists(src_file_path):
        print("No file", src_file_path)
        return

    parent_dir = entry.full_path.split("/")[-2]

    covered_lines, not_covered_lines, covered_funcs, not_covered_funcs = [], [], [], []

    for i, covered in entry.lines_with_hits:
        (covered_lines if covered else not_covered_lines).append(i)

    for i, covered in entry.funcs_with_hits:
        (covered_funcs if covered else not_covered_funcs).append(i)

    formatter = CodeFormatter(
        sorted(covered_lines), sorted(not_covered_lines),
        sorted(set(covered_funcs)), sorted(set(not_covered_funcs)))

    with open(src_file_path, "r") as src_file:
        lines = highlight(src_file.read(), CppLexer(), formatter)

        output = render("file.html",
            lines=lines,
            entry=entry,
            not_covered_lines=not_covered_lines,
            not_covered_funcs=not_covered_funcs)

        with open(os.path.join(args.out_dir, entry.full_path) + ".html", "w") as f:
            f.write(output)

def read_report(f: TextIO):
    global files
    global tests
    global tests_names

    elapsed = time()

    # We need to skip some source files as they are useless for report (instrumented contribs/).
    # We maintain a original index => real index map.
    # Skipped source files have real index -1
    source_files_map = []
    sf_index = 0

    for i in range(int(f.readline().split()[1])): # files
        rel_path, funcs_count, lines_count = f.readline().split()

        funcs = {}

        for j in range(int(funcs_count)):
            mangled_name, start_line, edge_index = f.readline().split()
            funcs[int(edge_index)] = mangled_name, int(start_line)

        lines = [int(f.readline()) for j in range(int(lines_count))]

        if "contrib/" in rel_path:
            source_files_map.append(-1)
            print("Skipping", rel_path)
        else:
            files.append((rel_path, funcs, lines))
            source_files_map.append(sf_index)
            sf_index += 1

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

        real_index = source_files_map[source_id]

        if real_index == -1:
            continue

        tests_sources[real_index] = funcs, lines

    tests_names = f.readlines()

    assert len(tests_names) == len(tests), f"{len(tests_names)} != {len(tests)}"

    print("Read the report, took {}s. {} tests, {} source files".format(
        int(time() - elapsed), len(tests), len(files)))

def main():
    file_loader = FileSystemLoader(os.path.abspath(os.path.dirname(__file__)) + '/templates', encoding='utf8')

    global env

    env = Environment(loader=file_loader)
    env.trim_blocks = True
    env.lstrip_blocks = True
    env.rstrip_blocks = True

    parser = argparse.ArgumentParser(prog='CCR converter')

    parser.add_argument('ccr_report_file')
    parser.add_argument('out_dir', help="Absolute path")
    parser.add_argument('sources_dir', help="Absolute path")

    global args

    args = parser.parse_args()

    print("Will use {} as output directory".format(args.out_dir))
    print("Will use {} as root CH directory (with src/ inside)".format(args.sources_dir))

    with open(args.ccr_report_file, "r") as f:
        read_report(f)

    generate_html()

if __name__ == '__main__':
    main()
