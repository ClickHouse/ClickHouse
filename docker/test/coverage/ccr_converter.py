from typing import TextIO
from time import time
from collections import Counter
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import CppLexer
from jinja2 import Environment, FileSystemLoader
from tqdm import tqdm

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

class FileEntry:
    def __init__(self, path, lines_with_hits, funcs_with_hits, tests_with_hits):
        self.full_path = path

        self.name = path.split("/")[-1]
        self.url = self.name + ".html"

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
        self.url = path + "index.html"

        self.files = []

        self.lines = 0, 0, 0
        self.funcs = 0, 0, 0
        self.tests = 0, 0

        self.tests_with_hits = set()

    def add_file(self, f):
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
        # TODO Exclude contribs and base/ while reading the report
        if "contrib/" in path or "base/" in path:
            continue

        path = path[path.find("src") + 4:]

        dirs, file_name = os.path.split(path)

        funcs_hit, lines_hit = data[i] if i in data else (set(), set())

        lines_with_hits = [(line, line in lines_hit) for line in lines_instrumented]
        funcs_with_hits = [(line, index in funcs_hit) for index, (_, line) in funcs_instrumented.items()]
        tests_with_hits = [j for j, test in enumerate(tests) if i in test.keys()]

        test_file = FileEntry(path, lines_with_hits, funcs_with_hits, tests_with_hits)

        found = False

        for d in out:
            if d.name == dirs:
                d.add_file(test_file)
                found = True
                break

        if not found:
            out.append(DirEntry(dirs))
            out[-1].add_file(test_file)

    return sorted(out, key=lambda x: x.name)

def render(tpl, **kwargs):
    kwargs.update({
        "bounds": bounds,
        "colors": colors,
        "tests_total": len(tests),
        "pr_url": args.pr_url,
        "pr_index": args.pr_index,
        "commit_url": args.commit_url,
        "commit_hash": args.commit_hash
    })

    return tpl.render(kwargs)

def generate_dir_page(entry: DirEntry, env, upper_level=False):
    special_entries = []

    if not upper_level:
        special_entries.append(DirEntry("../")) # upper dir

    special_entries.append(entry)
    special_entries[-1].url="" # current dir

    template = env.get_template('directory.html')

    entries = sorted(entry.files, key=lambda x: x.name)

    if upper_level:
        for e in entries:
            e.url = e.name + "/index.html"

    path = os.path.join(args.html_dir, entry.name)

    os.makedirs(path, exist_ok=True)

    with open(os.path.join(path, "index.html"), "w") as f:
        f.write(render(template, entries=entries, special_entries=special_entries))

def generate_file_page(entry: FileEntry, env):
    template = env.get_template('file.html')

    src_file_path = os.path.join(args.sources_dir, "src", entry.full_path)

    if not os.path.exists(src_file_path):
        print("No file", src_file_path)
        return

    parent_dir = entry.full_path.split("/")[-2]

    covered_lines = sorted([e[0] for e in entry.lines_with_hits if e[1]])
    not_covered_lines = sorted([e[0] for e in entry.lines_with_hits if not e[1]])
    covered_funcs = sorted(set([e[0] for e in entry.funcs_with_hits if e[1]]))
    not_covered_funcs = sorted(set([e[0] for e in entry.funcs_with_hits if not e[1]]))

    formatter = CodeFormatter(covered_lines, not_covered_lines, covered_funcs, not_covered_funcs)

    with open(src_file_path, "r") as src_file:
        lines = highlight(src_file.read(), CppLexer(), formatter)

        output = render(template,
            lines=lines,
            entry=entry,
            parent_dir=parent_dir,
            not_covered_lines=not_covered_lines,
            not_covered_funcs=not_covered_funcs)

        with open(os.path.join(args.html_dir, entry.full_path) + ".html", "w") as f:
            f.write(output)

def generate_html():
    file_loader = FileSystemLoader(os.path.abspath(os.path.dirname(__file__)) + '/templates', encoding='utf8')

    env = Environment(loader=file_loader)
    env.trim_blocks = True
    env.lstrip_blocks = True
    env.rstrip_blocks = True

    entries = get_entries()

    root_entry = DirEntry("")

    for dir_entry in tqdm(entries):
        root_entry.add_file(dir_entry)

        generate_dir_page(dir_entry, env)

        for sf_entry in tqdm(dir_entry.files):
            generate_file_page(sf_entry, env)

    generate_dir_page(root_entry, env, upper_level=True)

def read_report(f: TextIO):
    global files
    global tests
    global tests_names

    elapsed = time()

    for i in range(int(f.readline().split()[1])): # files
        rel_path, funcs_count, lines_count = f.readline().split()

        funcs = {}

        for j in range(int(funcs_count)):
            mangled_name, start_line, edge_index = f.readline().split()
            funcs[int(edge_index)] = mangled_name, int(start_line)

        lines = [int(f.readline()) for j in range(int(lines_count))]

        files.append((rel_path, funcs, lines))

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

    # assert len(tests_names) == len(tests), f"{len(tests_names)} != {len(tests)}"

    print("Read the report, took {}s. {} tests, {} source files".format(
        int(time() - elapsed), len(tests), len(files)))

def main():
    parser = argparse.ArgumentParser(prog='CCR converter')

    parser.add_argument('ccr_report_file')
    parser.add_argument('html_dir')
    parser.add_argument('sources_dir')
    parser.add_argument('pr_url')
    parser.add_argument('pr_index')
    parser.add_argument('commit_url')
    parser.add_argument('commit_hash')

    global args

    args = parser.parse_args()

    print("Will use {} as output directory".format(args.html_dir))
    print("Will use {} as root CH directory (with src/ inside)".format(args.sources_dir))

    with open(args.ccr_report_file, "r") as f:
        read_report(f)

    generate_html()

if __name__ == '__main__':
    main()
