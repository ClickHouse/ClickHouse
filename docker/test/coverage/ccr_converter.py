from datetime import date
from copy import deepcopy
from collections import defaultdict
from enum import IntEnum

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import CppLexer
from jinja2 import Environment, FileSystemLoader, ModuleLoader, ChoiceLoader
from tqdm import tqdm

import argparse
import os.path

LINES = "Lines"
EDGES = "Edges"
FUNCS = "Funcs"
TESTS = "Tests"

bounds = { # success bound, warning bound, %
    LINES: [90, 75],
    EDGES: [60, 25],
    FUNCS: [90, 75],
    TESTS: [20, 1]
    }

colors = { # covered, not covered. Colors taken from colorsafe.co
    LINES: ["#00aa554d", "#e76e3c4d"],
    EDGES: ["#1ba39c4d", "#f647474d"],
    FUNCS: ["#28a2284d", "#d475004d"]
    }

files = []
tests = []
tests_names = []

args = None
env = None

def percent(a, b):
  return 0 if b == 0 else int(a * 100 / b)

class Entry(IntEnum):
    HITS = 0
    TOTAL = 1
    PERCENT = 2
    LIST = 3

class EntryBase:
    def __init__(self, name, url, funcs=None, edges=None, lines=None, tests=None):
        self.name = name 
        self.url = url 

        # hits, total, percent(hits, total), list_of_items_with_hits
        self.funcs = funcs if funcs is not None else (0, 0, 0, [])
        self.edges = edges if edges is not None else (0, 0, 0, [])
        self.lines = lines if lines is not None else (0, 0, 0, [])
        self.tests = tests if tests is not None else (0, 0, 0, [])

    def types_and_lists(self):
        return [(FUNCS, self.funcs), (EDGES, self.edges), (LINES, self.lines)]

class FileEntry(EntryBase):
    def __init__(self, path, data, tests_with_hits):
        funcs, edges, lines = data

        super().__init__(
            path.split("/")[-1],
            os.path.join(args.out_dir, path) + ".html",
            self._helper(funcs),
            self._helper(edges),
            self._helper(lines),
            self._helper(tests_with_hits))

        self.full_path = path

    def _helper(self, lst):
        total = len(lst)
        hits = len([e for e in lst if e[1]])

        return hits, total, percent(hits, total), lst

class DirEntry(EntryBase):
    def __init__(self, path):
        super().__init__(path, path + "/index.html")

        self.items = []

    def add(self, entry):
        self.items.append(entry)

        self._recalc(entry, "lines")
        self._recalc(entry, "edges")
        self._recalc(entry, "funcs")

        test_hit = max(self.tests[0], entry.tests[0])
        self.tests = test_hit, 0, percent(test_hit, len(tests)), []

    def _recalc(self, entry, name):
        old = getattr(self, name)
        entry_item = getattr(entry, name)

        hit = old[0] + entry_item[0]
        total = old[1] + entry_item[1]

        setattr(self, name, (hit, total, percent(hit, total), []))

class CodeFormatter(HtmlFormatter):
    def __init__(self, data):
        super(CodeFormatter, self).__init__(linenos=True, lineanchors="line")

        self.data = data
        self.traversal_order = [EDGES, FUNCS, LINES]
        self.frmt = '<span style="background-color:{}"> {}</span>'

    def wrap(self, source, outfile):
        return self._wrap_code(source)

    def _wrap_code(self, source):
        yield 0, '<div class="highlight"><pre>'

        for i, (token, value) in enumerate(source):
            if token != 1:
                yield t, value

            for entity in self.traversal_order:
                if i + 1 in self.data[entity][0]:
                    yield 1, self.frmt.format(colors[entity][0], value)
                    break
                if i + 1 in self.data[entity][1]:
                    yield 1, self.frmt.format(colors[entity][1], value)
                    break
            else:
                yield 1, value

        yield 0, '</pre></div>'

def get_lines_with_hits_for_file(file_name):
    with open(os.path.concat(args.gcno_dir, file_name + ".gcno"), "rb") as gcno:
        pass

    return []

def accumulate_coverage_data():
    accumulated = defaultdict(lambda: (set(), set()))

    for test in tests:
        for sf_index, (funcs, edges) in test.items():
            if sf_index not in accumulated:
                accumulated[sf_index] = set(funcs), set(edges)
            else:
                funcs_a, edges_a = accumulated[sf_index]
                funcs_a.update(funcs)
                edges_a.update(edges)

    return accumulated

def get_entries():
    dir_entries = []
    accumulated = accumulate_coverage_data()

    for sf_index, (path, funcs_instrumented, edges_instrumented) in enumerate(files):
        dirs, file_name = os.path.split(path)

        funcs_hit, edges_hit = accumulated[sf_index]

        funcs_with_hits = sorted([
            (start_line, index in funcs_hit) for index, (_, start_line) in funcs_instrumented.items()])

        edges_with_hits = [(edge_line, edge_line in edges_hit) for edge_line in edges_instrumented]
        lines_with_hits = get_lines_with_hits_for_file(file_name)

        test_file = FileEntry(path, (funcs_with_hits, edges_with_hits, lines_with_hits),
            [(i, sf_index in test.keys()) for i, test in enumerate(tests)])

        found = False

        for dir_entry in dir_entries:
            if dir_entry.name == dirs:
                dir_entry.add(test_file)
                found = True
                break

        if not found:
            dir_entries.append(DirEntry(dirs))
            dir_entries[-1].add(test_file)

    return sorted(dir_entries, key=lambda x: x.name)

def generate_html():
    entries = get_entries()

    root_entry = DirEntry("")
    files_entry = DirEntry("")

    for dir_entry in tqdm(entries):
        root_entry.add(dir_entry)

        generate_dir_page(dir_entry)

        for sf_entry in tqdm(dir_entry.items):
            generate_file_page(sf_entry)
            files_entry.add(sf_entry)

    generate_dir_page(root_entry, is_root=True)

    for e in files_entry.items:
        e.name = e.full_path

    generate_dir_page(files_entry, page_name="files.html", is_root=True)

def render(tpl, depth, **kwargs):
    # All generated links must be made relative as args.out_dir != resulting S3 dir root
    root_url = "../" * depth

    kwargs.update({
        "bounds": bounds,
        "colors": colors,
        "HITS": Entry.HITS,
        "TOTAL": Entry.TOTAL,
        "PERCENT": Entry.PERCENT,
        "LIST": Entry.LIST,
        "TESTS": TESTS,
        "tests_total": len(tests),
        "generated_at": date.today(),
        "root_url": os.path.join(root_url, "index.html"),
        "index_url": os.path.join(root_url, "files.html")
    })

    return env.get_template(tpl).render(kwargs)

def generate_dir_page(entry: DirEntry, page_name="index.html", is_root=False):
    path = os.path.join(args.out_dir, entry.name)
    os.makedirs(path, exist_ok=True)

    entries = sorted(entry.items, key=lambda x: x.name)

    special_entry = deepcopy(entry)
    special_entry.url = "./index.html"

    depth = 0 if is_root else (entry.name.count('/') + 1)

    with open(os.path.join(path, page_name), "w") as f:
        f.write(render("directory.html", depth, entries=entries, entry=entry, special_entry=special_entry))

def generate_file_page(entry: FileEntry):
    src_file_path = os.path.join(args.sources_dir, entry.full_path)

    if not os.path.exists(src_file_path):
        print("No file", src_file_path)
        return

    data = {}

    types_and_lists = entry.types_and_lists()

    for entity_type, entity in types_and_lists:
        covered, not_covered = [], []

        for i, is_covered in entity[3]:
            (covered if is_covered else not_covered).append(i)

        data[entity_type] = sorted(set(covered)), sorted(set(not_covered))

    with open(src_file_path, "r") as sf:
        lines = highlight(sf.read(), CppLexer(), CodeFormatter(data))

        depth = entry.full_path.count('/')

        not_covered_ent = [(entity, not_covered) for (entity, (_, not_covered)) in data.items()]

        output = render("file.html", depth, highlighted_sources=lines, entry=entry, not_covered=not_covered_ent)

        with open(os.path.join(args.out_dir, entry.full_path) + ".html", "w") as f:
            f.write(output)

def read_header(report_file):
    global files

    # We need to skip instrumented contribs as they are useless for report
    # We maintain a original index => real index map.
    # Skipped source files have real index -1
    source_files_map = []
    sf_index = 0

    files_count = int(report_file.readline().split()[1])

    for _ in range(files_count):
        file_path, funcs_count, edges_count = report_file.readline().split()
        file_path = os.path.normpath(file_path)

        funcs = {}

        for _ in range(int(funcs_count)):
            mangled_name, start_line, edge_index = report_file.readline().split()
            funcs[int(edge_index)] = mangled_name, int(start_line)

        edges = [int(report_file.readline()) for _ in range(int(edges_count))]

        if "contrib/" in file_path:
            source_files_map.append(-1)
        else:
            files.append((file_path, funcs, edges)) # lines will be parsed from llvm-cov intermediate format

            source_files_map.append(sf_index)
            sf_index += 1

    return source_files_map

def read_tests(report_file, source_files_map):
    global tests

    tests_sources = {}

    while True:
        token = report_file.readline()

        if token == "TEST\n":
            if len(tests_sources) > 0:
                tests.append(tests_sources)
                tests_sources = {}
            token = report_file.readline()
        elif not token.startswith("SOURCE"):
            if len(tests_sources) > 0:
                tests.append(tests_sources)
            return

        source_id = int(token.split()[1])
        covered_funcs = list(map(int, report_file.readline().split()))
        covered_edges = list(map(int, report_file.readline().split()))

        real_index = source_files_map[source_id]

        if real_index != -1:
            tests_sources[real_index] = covered_funcs, covered_edges # covered lines will be calculated later

def read_report(report_file):
    global tests_names

    source_files_map = read_header(report_file)

    read_tests(report_file, source_files_map)

    tests_names = list(map(lambda x: x[:-1], report_file.readlines())) # remove \n

    assert len(tests_names) == len(tests), f"{len(tests_names)} != {len(tests)}"

    print(f"Read the report, {len(tests)} tests, {len(files)} source files")

if __name__ == '__main__':
    dir_path = os.path.abspath(os.path.dirname(__file__))
    tpl_path = os.path.join(dir_path, "templates")
    compiled_tpl_path = os.path.join(dir_path, "compiled_templates")

    tpl_loader = ChoiceLoader([
        ModuleLoader(compiled_tpl_path),
        FileSystemLoader(tpl_path)
    ])

    global env

    env = Environment(loader=tpl_loader, trim_blocks=True, lstrip_blocks=True, rstrip_blocks=True, enable_async=True)

    env.compile_templates(compiled_tpl_path, zip=None, ignore_errors=False)

    parser = argparse.ArgumentParser(
        prog='HTML report generator', description="""
    Reads .ccr report, generates an HTML coverage report out of it.
    Also reads CH source files and corresponding .gcno files.""")

    parser.add_argument('report_file', help=".ccr report file")
    parser.add_argument('out_dir', help="Directory to which the HTML report will be written. Absolute path")
    parser.add_argument('sources_dir', help="Path to ClickHouse sources root directory. Absolute path")
    parser.add_argument('gcno_dir', help="Path to directory with .gcno files generated by the compiler. Absolute path")

    global args

    args = parser.parse_args()

    print(args)

    with open(args.report_file, "r") as f:
        read_report(f)
        generate_html()
