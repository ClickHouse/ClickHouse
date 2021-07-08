from binary_parser import BinaryParser

import os.path
from io import BytesIO


class GCNOParser(BinaryParser):
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

    FUNCTION = 0x01000000
    BLOCKS = 0x01410000
    ARCS = 0x01430000
    LINES = 0x01450000

    def __init__(self):
        super().__init__()
        self.sf_to_funcs = {}

    def read(self, folder):
        for name in os.listdir(folder):
            with open(os.path.join(folder, name), "rb") as f:
                self.load_file_header(f)
                self.load_records(f)

        return self.sf_to_funcs

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
            self.blocks = \
                [GCNOParser.GraphBlock(i + 1) for i in range(bb_count)]

    def get_name_from_path(self, pathname):
        return os.path.split(pathname)[1]

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
        magic = f.read(4).decode("utf-8")

        if magic == "gcno":
            self.is_le = False
        elif magic == "oncg":
            self.is_le = True
        else:
            raise Exception("Unknown endianess")

        self.parse_version(f.read(4))
        self.read_uint32(f)  # stamp

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
            if size < pos + 8:
                raise Exception("Invalid size")

            tag, word_len = self.read_uint32(f), self.read_uint32(f)
            record_buf = BytesIO(f.read(word_len * 4))

            if tag == self.FUNCTION:
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
            elif tag == self.BLOCKS:
                current.set_basic_blocks(word_len)
            elif tag == self.ARCS:
                block_no, arcs = self.read_arcs_record(record_buf, word_len)
                next_arc_id = current.set_arcs_for_bb(
                    next_arc_id, block_no, arcs)
            elif tag == self.LINES:
                block_no, lines = self.read_lines_record(record_buf)
                current.set_lineset_for_bb(block_no, lines)
            else:
                break

            pos = f.tell()

    def read_func_record(self, f):
        # ignore function id, line checksum, cfg checksum
        self.read_uint32(f), self.read_uint32(f), self.read_uint32(f)

        name = self.read_str(f)
        filename = self.read_str(f)

        self.read_uint32(f)  # ignore start_line

        return name, filename

    def read_lines_record(self, f):
        block_no = self.read_uint32(f)
        line_set = []

        while True:
            line = self.read_uint32(f)

            if line != 0:
                line_set.append(line)
            elif len(self.read_str(f)) == 0:  # ignore line_str
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
