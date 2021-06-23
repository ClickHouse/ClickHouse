from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import CppLexer
from jinja2 import Environment, FileSystemLoader, ModuleLoader
from tqdm import tqdm

from io import BytesIO
from struct import unpack
from enum import IntEnum
from collections import defaultdict
from argparse import ArgumentParser
from datetime import date

import os.path

LINES = "Lines"
EDGES = "Edges"
FUNCS = "Funcs"
TESTS = "Tests"

env = None
tests_count = 0

bounds = {  # success bound, warning bound, %
    LINES: [90, 75],
    EDGES: [60, 25],
    FUNCS: [90, 75],
    TESTS: [20, 1]
    }

colors = {  # covered, not covered. Colors taken from colorsafe.co
    LINES: ["#00aa554d", "#e76e3c4d"],
    EDGES: ["#1ba39c4d", "#f647474d"],
    FUNCS: ["#28a2284d", "#d475004d"]
    }


class GCNO:
    """
    .gcno files parser.

    Based on:
      https://github.com/llvm/llvm-project/blob/main/llvm/lib/ProfileData/GCOV.cpp
      https://github.com/myronww/pycover/blob/main/pycover.py

    Motivation:
      - gcc's gcov can't parse .gcno files produced by clang
      - clang's llvm-cov can't produce json output from parsed data
      - llvm-cov's intermediate representation does not include basic blocks'
        bounds which are needed to calculate lines coverage from arcs coverage.
    """

    def __init__(self, folder):
        self.is_le = False  # endianess
        self.sf_to_funcs = {}

        for name in os.listdir(folder):
            with open(os.path.join(folder, name), "rb") as f:
                self.load_file_header(f)
                self.load_records(f)

    class Magic:
        BE = b"gcno"
        LE = b"oncg"

    class Tag(IntEnum):
        FUNCTION = 0x01000000
        BLOCKS = 0x01410000
        ARCS = 0x01430000
        LINES = 0x01450000

    class PackUInt32:
        LE = "<I"
        BE = ">I"

    class GraphArc():
        def __init__(self, dest_block):
            self.dest_block = dest_block
            self.arc_id = None

    class GraphBlock():
        def __init__(self, block_number):
            self.block_number = block_number

            self.lines = []
            self.arcs_successors = []
            self.arcs_predecessors = []

    class GraphFunction():
        def __init__(self, name):
            self.name = name
            self.blocks = []

        def set_arcs_for_bb(self, next_arc_id, block_no, arcs):
            block = self.blocks[block_no]
            block.arcs_successors = arcs

            for i, arc in enumerate(arcs):
                arc.arc_id = next_arc_id + i

                block_no = arc.dest_block
                block = self.blocks[block_no]
                block.arcs_predecessors.append(arc)

            return next_arc_id + len(arcs)

        def set_lineset_for_bb(self, block_no, lines):
            self.blocks[block_no].lines = lines

        def set_basic_blocks(self, bb_count):
            self.blocks = [GCNO.GraphBlock(i + 1) for i in range(bb_count)]

    def get_name_from_path(self, pathname):
        return os.path.split(pathname)[1]

    def pack_uint(self):
        return self.PackUInt32.LE if self.is_le else self.PackUInt32.BE

    def parse_version(self, version_bin):
        v = (version_bin[::-1] if self.is_le else version_bin).decode('utf-8')

        if v[0] >= 'A':
            ver = (ord(v[0]) - ord('A')) * 100 \
                + (ord(v[1]) - ord('0')) * 10  \
                + ord(v[2]) - ord('0')
        else:
            ver = (ord(v[0]) - ord('0')) * 10 \
                + ord(v[2]) - ord('0')

        if ver != 48:
            raise Exception("Invalid version")

    def load_file_header(self, f):
        magic = self.read_quad_char(f)

        if magic == self.Magic.BE:
            self.is_le = False
        elif magic == self.Magic.LE:
            self.is_le = True
        else:
            raise Exception("Unknown endianess")

        self.parse_version(self.read_quad_char(f))
        self.read_uint32(f)  # stamp

    def get_lines_with_hits(self, file_name, edges):
        lines = {}
        hit_edges = [e for e, hit in edges if hit]

        if file_name not in self.sf_to_funcs:
            print(f"No gcno data for {file_name}, lines coverage is disabled")
            return []

        for func in self.sf_to_funcs[file_name]:
            for bb in func.blocks:
                bb_lines = sorted(set(bb.lines))

                if len(bb_lines) == 0:
                    hit = False
                else:
                    left, right = bb_lines[0], bb_lines[-1]
                    hit = any([left <= e <= right for e in hit_edges])

                for x in bb_lines:
                    lines[x] = hit

        return sorted(lines.items())

    def load_records(self, f):
        """
        Each .gcno file usually contains functions from multiple translation
        units (e.g. for CH compiler generates ~800 .gcno files for ~2k source
        files), so we return a source file -> functions dict.
        """
        pos = f.tell()

        current = None
        next_arc_id = 0

        size = os.fstat(f.fileno()).st_size

        while pos < size:
            if size - pos < 8:
                raise Exception("Invalid size")

            tag, word_len = self.read_uint32(f), self.read_uint32(f)
            record_buf = BytesIO(f.read(word_len * 4))

            if tag == self.Tag.FUNCTION:
                next_arc_id = 0

                name, pathname = self.read_func_record(record_buf)

                filename = self.get_name_from_path(pathname)

                if "contrib/" in pathname:
                    current = self.GraphFunction("")  # fake
                else:
                    current = self.GraphFunction(name)

                    if filename not in self.sf_to_funcs:
                        self.sf_to_funcs[filename] = [current]
                    else:
                        self.sf_to_funcs[filename].append(current)
            elif tag == self.Tag.BLOCKS:
                # flags are not read as they're going to be ignored
                current.set_basic_blocks(word_len)
            elif tag == self.Tag.ARCS:
                block_no, arcs = self.read_arcs_record(record_buf, word_len)
                next_arc_id = current.set_arcs_for_bb(
                    next_arc_id, block_no, arcs)
            elif tag == self.Tag.LINES:
                block_no, lines = self.read_lines_record(record_buf)
                current.set_lineset_for_bb(block_no, lines)
            else:
                break

            pos = f.tell()

    @staticmethod
    def read_quad_char(f):
        return f.read(4)

    def read_uint32(self, f):
        return unpack(self.pack_uint(), self.read_quad_char(f))[0]

    def read_string(self, f):
        word_len = self.read_uint32(f)
        return f.read(word_len * 4).rstrip(b"\0")

    def read_func_record(self, f):
        # ignore function id, line checksum, cfg checksum
        self.read_uint32(f), self.read_uint32(f), self.read_uint32(f)

        name, filename = self.read_string(f), self.read_string(f)
        self.read_uint32(f)  # ignore start_line

        return name.decode("utf-8"), filename.decode("utf-8")

    def read_lines_record(self, f):
        block_no = self.read_uint32(f)
        line_set = []

        while True:
            line = self.read_uint32(f)

            if line != 0:
                line_set.append(line)
            elif len(self.read_string(f)) == 0:  # ignore line_str
                break

        return block_no, line_set

    def read_arcs_record(self, f, record_len):
        block_no = self.read_uint32(f)
        arc_set = []
        arcs_count = (record_len - 1) // 2

        for _ in range(arcs_count):
            dest_block = self.read_uint32(f)
            self.read_uint32(f)  # ignore arc flags
            arc_set.append(self.GraphArc(dest_block))

        return block_no, arc_set


class Entry(IntEnum):
    HITS = 0
    TOTAL = 1
    PERCENT = 2
    LIST = 3


class EntryBase:
    def __init__(self, name, url,
                 funcs=None, edges=None, lines=None, tests=None):
        self.name = name
        self.url = url

        # hits, total, percent(hits, total), list_of_items_with_hits
        self.funcs = funcs if funcs is not None else (0, 0, 0, [])
        self.edges = edges if edges is not None else (0, 0, 0, [])
        self.lines = lines if lines is not None else (0, 0, 0, [])
        self.tests = tests if tests is not None else (0, 0, 0, [])

    @staticmethod
    def percent(a, b):
        return 0 if b == 0 else int(a * 100 / b)

    def types_and_lists(self):
        return [(FUNCS, self.funcs), (EDGES, self.edges), (LINES, self.lines)]

    def render(self, tpl, depth, **kwargs):
        # All generated links must be relative as out_dir != resulting S3 dir
        root_url = "../" * depth

        kwargs.update({
            "bounds": bounds,
            "colors": colors,
            "HITS": Entry.HITS,
            "TOTAL": Entry.TOTAL,
            "PERCENT": Entry.PERCENT,
            "LIST": Entry.LIST,
            "TESTS": TESTS,
            "tests_total": tests_count,
            "generated_at": date.today(),
            "root_url": os.path.join(root_url, "index.html"),
            "index_url": os.path.join(root_url, "files.html")
        })

        return env.get_template(tpl).render(kwargs)


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

        return hits, total, self.percent(hits, total), lst

    def generate_page(self):
        src_file_path = os.path.join(args.sources_dir, self.full_path)

        if not os.path.exists(src_file_path):
            print("No file", src_file_path)
            return

        data = {}
        not_covered_entities = []

        types_and_lists = self.types_and_lists()

        for entity_type, entity in types_and_lists:
            covered, not_covered = set(), set()

            items_with_hits = entity[3]

            for i, is_covered in items_with_hits:
                (covered if is_covered else not_covered).add(i)

            covered, not_covered = sorted(covered), sorted(not_covered)

            data[entity_type] = covered, not_covered
            not_covered_entities.append((entity_type, not_covered))

        with open(src_file_path, "r") as sf:
            lines = highlight(sf.read(), CppLexer(), CodeFormatter(data))

            depth = self.full_path.count('/')

            output = self.render(
                "file.html", depth,
                highlighted_sources=lines, entry=self,
                not_covered=not_covered_entities)

            html_file = os.path.join(args.out_dir, self.full_path) + ".html"

            with open(html_file, "w") as f:
                f.write(output)


class DirEntry(EntryBase):
    def __init__(self, path="", is_root=False):
        super().__init__(path, path + "/index.html")

        self.items = []
        self.is_root = is_root
        self.path = path

    def add(self, entry):
        self.items.append(entry)

        self._recalc(entry, "lines")
        self._recalc(entry, "edges")
        self._recalc(entry, "funcs")

        test_hit = max(self.tests[0], entry.tests[0])
        self.tests = test_hit, 0, self.percent(test_hit, tests_count), []

    def _recalc(self, entry, name):
        old = getattr(self, name)
        entry_item = getattr(entry, name)

        hit = old[0] + entry_item[0]
        total = old[1] + entry_item[1]

        setattr(self, name, (hit, total, self.percent(hit, total), []))

    def generate_page(self, page_name="index.html"):
        path = os.path.join(args.out_dir, self.name)
        os.makedirs(path, exist_ok=True)

        entries = sorted(self.items, key=lambda x: x.name)

        special_entry = DirEntry()

        for item in self.items:
            special_entry.add(item)

        special_entry.url = "./" + page_name

        depth = 0 if self.is_root else (self.name.count('/') + 1)

        with open(os.path.join(path, page_name), "w") as f:
            f.write(self.render(
                "directory.html", depth,
                entries=entries, entry=self, special_entry=special_entry))


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
                yield 0, value
                continue

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


class CCR:
    def __init__(self, args):
        self.args = args

        self.tests_names = []
        self.files = []
        self.tests = []

        self.gcno = GCNO(args.gcno_dir)

        print(len(self.gcno.sf_to_funcs), "source files in gcno files")

        with open(self.args.report_file, "r") as f:
            self.read(f)
            self.generate_html()

    def read(self, report_file):
        self.read_header(report_file)
        self.read_tests(report_file)

        # remove \n
        self.tests_names = list(map(lambda x: x[:-1], report_file.readlines()))

        if len(self.tests_names) != len(self.tests):
            raise Exception("Corrupt report file")

        print("{} tests, {} source files".format(
            len(self.tests), len(self.files)))

        global tests_count
        tests_count = len(self.tests)

    def read_header(self, f):
        """
        We need to skip instrumented contribs as they are useless for report.
        We maintain a original index => real index map.
        Skipped source files have real index -1.
        """
        source_files_map = []
        sf_index = 0

        files_count = int(f.readline().split()[1])

        for _ in range(files_count):
            file_path, funcs_count, edges_count = f.readline().split()
            file_path = os.path.normpath(file_path)

            funcs = {}

            for _ in range(int(funcs_count)):
                mangled_name, start_line, edge_index = f.readline().split()
                funcs[int(edge_index)] = mangled_name, int(start_line)

            edges = [int(f.readline()) for _ in range(int(edges_count))]

            if "contrib/" in file_path:
                source_files_map.append(-1)
            else:
                self.files.append((file_path, funcs, edges))

                source_files_map.append(sf_index)
                sf_index += 1

        self.source_files_map = source_files_map

    def read_tests(self, report_file):
        tests_sources = {}

        while True:
            token = report_file.readline()

            if token == "TEST\n":
                if len(tests_sources) > 0:
                    self.tests.append(tests_sources)
                    tests_sources = {}
                token = report_file.readline()
            elif not token.startswith("SOURCE"):
                if len(tests_sources) > 0:
                    self.tests.append(tests_sources)
                return

            source_id = int(token.split()[1])
            covered_funcs = list(map(int, report_file.readline().split()))
            covered_edges = list(map(int, report_file.readline().split()))

            real_index = self.source_files_map[source_id]

            if real_index != -1:
                tests_sources[real_index] = covered_funcs, covered_edges

    def generate_html(self):
        entries = self.get_entries()

        root_entry = DirEntry(is_root=True)
        files_entry = DirEntry(is_root=True)

        for dir_entry in tqdm(entries):
            root_entry.add(dir_entry)

            dir_entry.generate_page()

            for sf_entry in tqdm(dir_entry.items):
                sf_entry.generate_page()
                files_entry.add(sf_entry)

        root_entry.generate_page()

        for e in files_entry.items:
            e.name = e.full_path

        files_entry.generate_page(page_name="files.html")

    def get_funcs_with_hits(self, funcs, funcs_hit):
        return [(line, index in funcs_hit) for index, (_, line) in funcs]

    def get_edges_with_hits(self, edges, edges_hit):
        return [(edge_line, edge_line in edges_hit) for edge_line in edges]

    def get_tests_with_hits(self, sf_index):
        return [
            (i, sf_index in test.keys()) for i, test in enumerate(self.tests)]

    def find_or_append(self, it, name, test_file):
        for item in it:
            if item.name == name:
                item.add(test_file)
                return

        it.append(DirEntry(name))
        it[-1].add(test_file)

    def get_entries(self):
        dir_entries = []
        acc = self.accumulate_coverage_data()

        for sf_index, sf_data in enumerate(self.files):
            path, funcs, edges = sf_data
            dirs, file_name = os.path.split(path)

            funcs_hit, edges_hit = acc[sf_index]

            edges_with_hits = self.get_edges_with_hits(edges, edges_hit)

            file_entry_data = (
                self.get_funcs_with_hits(funcs.items(), funcs_hit),
                edges_with_hits,
                self.gcno.get_lines_with_hits(
                    file_name, edges_with_hits))

            test_file = FileEntry(
                path, file_entry_data, self.get_tests_with_hits(sf_index))

            self.find_or_append(dir_entries, dirs, test_file)

        return sorted(dir_entries, key=lambda x: x.name)

    def accumulate_coverage_data(self):
        acc = defaultdict(lambda: (set(), set()))

        for test in self.tests:
            for sf_index, (funcs, edges) in test.items():
                if sf_index not in acc:
                    acc[sf_index] = set(funcs), set(edges)
                else:
                    funcs_a, edges_a = acc[sf_index]
                    funcs_a.update(funcs)
                    edges_a.update(edges)

        return acc


if __name__ == '__main__':
    parser = ArgumentParser(
        prog='HTML report generator', description="""
    Reads .ccr report, generates an HTML coverage report out of it.
    Also reads CH source files and corresponding .gcno files.""")

    parser.add_argument('report_file', help=".ccr report file")

    parser.add_argument('out_dir', help="Absolute path to output directory")

    parser.add_argument('sources_dir', help="""
        Absolute path to ClickHouse sources root directory""")

    parser.add_argument('gcno_dir', help="""
        Absolute path to directory with .gcno files""")

    args = parser.parse_args()

    dir_path = os.path.abspath(os.path.dirname(__file__))
    tpl_path = os.path.join(dir_path, "templates")
    compiled_tpl_path = os.path.join(dir_path, "compiled_templates")

    env = Environment(
        loader=FileSystemLoader(tpl_path), trim_blocks=True, enable_async=True)

    # comment these 2 lines for readable errors
    env.compile_templates(compiled_tpl_path, zip=None, ignore_errors=False)
    env.loader = ModuleLoader(compiled_tpl_path)

    CCR(args)
