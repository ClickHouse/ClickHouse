from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import CppLexer
from jinja2 import Environment, FileSystemLoader, ModuleLoader
from tqdm import tqdm

from io import BytesIO

import argparse
import os.path
import struct
import datetime
import copy
import collections
import enum

LINES = "Lines"
EDGES = "Edges"
FUNCS = "Funcs"
TESTS = "Tests"

env = None

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

class GCNO:
    """
    .gcno files parser.

    Based on:
      https://github.com/llvm/llvm-project/blob/main/llvm/lib/ProfileData/GCOV.cpp
      https://github.com/myronww/pycover/blob/main/pycover.py

    Motivation: gcc's gcov can't parse .gcno files produced by clang, clang's llvm-cov gcov can't produce json
    output from parsed data and llvm-cov's intermediate representation does not include basic blocks' bounds which are
    needed to calculate lines coverage from arcs coverage.
    """

    def __init__(self, pathname):
        self.filename = self.get_name_from_path(pathname)

        # includes own functions only (which source file is equal to filename)
        self.functions = []

        self.is_le = False

        with open(pathname, "rb") as f:
            self.load_file_header(f)
            self.load_records(f)

    def get_name_from_path(self, pathname):
        if isinstance(pathname, bytes):
            pathname = pathname.decode('utf-8')

        return os.path.split(pathname)[1]

    class Magic:
        BE = b"gcno"
        LE = b"oncg"

    class Tag(enum.IntEnum):
        FUNCTION = 0x01000000
        BLOCKS   = 0x01410000
        ARCS     = 0x01430000
        LINES    = 0x01450000

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
        def __init__(self, ident, name, is_in_header, start_line):
            self.ident = ident
            self.name = name
            self.start_line = start_line
            self.is_in_header = is_in_header

            self.blocks = []

        def set_arcs_for_bb(self, next_arc_id, block_no, arcs):
            block = self.blocks[block_no]
            block.arcs_successors = arcs

            for arc in arcs:
                arc.arc_id = next_arc_id
                next_arc_id += 1

                block_no = arc.dest_block
                block = self.blocks[block_no]
                block.arcs_predecessors.append(arc)

            return next_arc_id

        def set_lineset_for_bb(self, block_no, lines):
            self.blocks[block_no].lines = lines

        def set_basic_blocks(self, bb_count):
            self.blocks = [GCNO.GraphBlock(i + 1) for i in range(bb_count)]

    def pack_uint(self):
        return self.PackUInt32.LE if self.is_le else self.PackUInt32.BE

    def parse_version(self, version_bin):
        v = (version_bin[::-1] if self.is_le else version_bin).decode('utf-8')

        if v[0] >= 'A':
            ver = (ord(v[0]) - ord('A')) * 100 + (ord(v[1]) - ord('0')) * 10 + ord(v[2]) - ord('0');
        else:
            ver = (ord(v[0]) - ord('0')) * 10 + ord(v[2]) - ord('0');

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
        self.read_uint32(f) # stamp

    def load_records(self, f):
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

                ident, name, filename, start_line = self.read_func_record(record_buf)
                name = name.decode("utf-8")

                filename = self.get_name_from_path(filename)
                is_in_header = filename.endswith(".h")

                if filename.split(".")[0] == self.filename.split(".")[0]:
                    self.functions.append(self.GraphFunction(ident, name, is_in_header, start_line))
                    current = self.functions[-1]
                else:
                    current = self.GraphFunction(0, "", False, 0) # fake
            elif tag == self.Tag.BLOCKS:
                current.set_basic_blocks(word_len) # flags are not read as they're going to be ignored
            elif tag == self.Tag.ARCS:
                block_no, arcs = self.read_arcs_record(record_buf, word_len)
                next_arc_id = current.set_arcs_for_bb(next_arc_id, block_no, arcs)
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
        return struct.unpack(self.pack_uint(), self.read_quad_char(f))[0]

    def read_uint64(self, f):
        lo, hi = read_uint32(f), read_uint32(f)
        return (hi << 32) | lo

    def read_string(self, f):
        word_len = self.read_uint32(f)
        return f.read(word_len * 4).rstrip(b"\0")

    def read_func_record(self, f):
        ident = self.read_uint32(f)
        _, _ = self.read_uint32(f), self.read_uint32(f) # ignore line and cfg checksum
        name, filename = self.read_string(f), self.read_string(f)
        start_line = self.read_uint32(f)

        return ident, name, filename, start_line

    def read_lines_record(self, f):
        block_no = self.read_uint32(f)
        line_set = []

        while True:
            line = self.read_uint32(f)

            if line != 0:
                line_set.append(line)
            elif len(self.read_string(f)) == 0: # line_str, ignored
                break

        return block_no, line_set

    def read_arcs_record(self, f, record_len):
        block_no = self.read_uint32(f)
        arc_set = []
        arcs_count = (record_len - 1) // 2

        for _ in range(arcs_count):
            dest_block = self.read_uint32(f)
            self.read_uint32(f) # ignore arc flags
            arc_set.append(self.GraphArc(dest_block))

        return block_no, arc_set

class Entry(enum.IntEnum):
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

    @staticmethod
    def percent(a, b):
        return 0 if b == 0 else int(a * 100 / b)

    def types_and_lists(self):
        return [(FUNCS, self.funcs), (EDGES, self.edges), (LINES, self.lines)]

    def render(self, tpl, depth, **kwargs):
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
            "tests_total": len(self.tests[3]),
            "generated_at": datetime.date.today(),
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

        types_and_lists = self.types_and_lists()

        for entity_type, entity in types_and_lists:
            covered, not_covered = [], []

            for i, is_covered in entity[3]:
                (covered if is_covered else not_covered).append(i)

            data[entity_type] = sorted(set(covered)), sorted(set(not_covered))

        with open(src_file_path, "r") as sf:
            lines = highlight(sf.read(), CppLexer(), CodeFormatter(data))

            depth = self.full_path.count('/')

            not_covered_ent = [(entity, not_covered) for (entity, (_, not_covered)) in data.items()]

            output = self.render("file.html", depth,
                highlighted_sources=lines, entry=self, not_covered=not_covered_ent)

            with open(os.path.join(args.out_dir, self.full_path) + ".html", "w") as f:
                f.write(output)

class DirEntry(EntryBase):
    def __init__(self, path="", is_root=False, tests_count=0):
        super().__init__(path, path + "/index.html")

        self.items = []
        self.is_root = is_root
        self.tests_count = tests_count
        self.path = path

    def add(self, entry):
        self.items.append(entry)

        self._recalc(entry, "lines")
        self._recalc(entry, "edges")
        self._recalc(entry, "funcs")

        test_hit = max(self.tests[0], entry.tests[0])
        self.tests = test_hit, 0, self.percent(test_hit, self.tests_count), []

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
        special_entry.files = self.files
        special_entry.url = "./" + page_name

        depth = 0 if self.is_root else (self.name.count('/') + 1)

        with open(os.path.join(path, page_name), "w") as f:
            f.write(self.render("directory.html", depth,
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

class CCR:
    def __init__(self, args):
        self.args = args

        self.tests_names = []
        self.files = []
        self.tests = []

        with open(self.args.report_file, "r") as f:
            self.read(f)
            self.generate_html()

    def read(self, report_file):
        self.read_header(report_file)
        self.read_tests(report_file)

        self.tests_names = list(map(lambda x: x[:-1], report_file.readlines())) # remove \n

        if len(self.tests_names) != len(self.tests):
            raise Exception("Corrupt report file")

        print("{} tests, {} source files".format(len(self.tests), len(self.files)))

    def read_header(self, report_file):
        """
        We need to skip instrumented contribs as they are useless for report.
        We maintain a original index => real index map.
        Skipped source files have real index -1.
        """
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
                tests_sources[real_index] = covered_funcs, covered_edges # covered lines will be calculated later

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

    def get_entries(self):
        dir_entries = []
        acc = self.accumulate_coverage_data()

        for sf_index, (path, funcs_instrumented, edges_instrumented) in enumerate(self.files):
            dirs, file_name = os.path.split(path)

            funcs_hit, edges_hit = acc[sf_index]

            funcs_with_hits = sorted([
                (start_line, index in funcs_hit) for index, (_, start_line) in funcs_instrumented.items()])

            edges_with_hits = [(edge_line, edge_line in edges_hit) for edge_line in edges_instrumented]
            lines_with_hits = self.get_lines_with_hits_for_file(file_name, edges_with_hits)

            test_file = FileEntry(path, (funcs_with_hits, edges_with_hits, lines_with_hits),
                [(i, sf_index in test.keys()) for i, test in enumerate(self.tests)])

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

    def accumulate_coverage_data(self):
        acc = collections.defaultdict(lambda: (set(), set()))

        for test in self.tests:
            for sf_index, (funcs, edges) in test.items():
                if sf_index not in acc:
                    acc[sf_index] = set(funcs), set(edges)
                else:
                    funcs_a, edges_a = acc[sf_index]
                    funcs_a.update(funcs)
                    edges_a.update(edges)

        return acc

    def get_lines_with_hits_for_file(self, file_name, edges):
        processing_header = False

        # Compiler emits information for .cpp files only but it's ok as we can get headers' coverage
        # from .cpp files
        if file_name.endswith(".h"):
            file_name = file_name.replace(".h", ".cpp")
            processing_header = True

        lines = []

        gcno_path = os.path.join(args.gcno_dir, file_name + ".gcno")

        try:
            gcno = GCNO(gcno_path)
        except:
            print("Missing gcno file", gcno_path)
            return []

        for func in gcno.functions:
            if func.is_in_header != processing_header:
                continue

            for bb in func.blocks:
                hit = self.bb_is_covered(bb, edges)

                for line in bb.lines:
                    lines.extend([(x, hit) for x in bb.lines])

        return sorted(list(set(lines)))

    def bb_is_covered(self, bb, edges):
        lines = sorted(set(bb.lines))

        if len(lines) == 0:
            return False

        for edge, hit in edges:
            if lines[0] <= edge <= lines[-1] and hit:
                return True

        return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='HTML report generator', description="""
    Reads .ccr report, generates an HTML coverage report out of it.
    Also reads CH source files and corresponding .gcno files.""")

    parser.add_argument('report_file', help=".ccr report file")
    parser.add_argument('out_dir', help="Directory to which the HTML report will be written. Absolute path")
    parser.add_argument('sources_dir', help="Path to ClickHouse sources root directory. Absolute path")
    parser.add_argument('gcno_dir', help="Path to directory with .gcno files generated by the compiler. Absolute path")

    args = parser.parse_args()

    dir_path = os.path.abspath(os.path.dirname(__file__))
    tpl_path = os.path.join(dir_path, "templates")
    compiled_tpl_path = os.path.join(dir_path, "compiled_templates")

    env = Environment(loader=FileSystemLoader(tpl_path), trim_blocks=True, enable_async=True)

    env.compile_templates(compiled_tpl_path, zip=None, ignore_errors=False)
    env.loader = ModuleLoader(compiled_tpl_path)

    CCR(args)
