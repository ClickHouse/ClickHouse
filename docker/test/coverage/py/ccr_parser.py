from binary_parser import BinaryParser
import os.path


class CCRParser(BinaryParser):
    def __init__(self):
        super().__init__()

        self.files = []
        self.tests = []

        self.bb = {}
        self.excluded_bb = []

    def read(self, report_file, tests_file):
        with open(report_file, "rb") as f:
            self.read_header(f)
            self.read_tests(f)

        tests_names = None

        with open(tests_file, "r") as f:
            tests_names = f.readlines().split()

        if len(tests_names) != len(self.tests):
            raise Exception("Corrupt report file")

        for name, test in zip(tests_names, self.tests):
            test[0] = name

        return self.files, self.tests, self.bb

    def read_header(self, f):
        magic = self.read_uint32(f)

        if magic == 0xcafefefe:
            self.is_le = False
        elif magic == 0xfefefeca:
            self.is_le = True
        else:
            raise Exception("Not a CCR file")

        files_count = self.read_uint32(f)

        for _ in range(files_count):
            file_path = self.read_str(f)
            bb_count = self.read_uint32(f)

            file_path = os.path.normpath(file_path)
            file_blocks = []

            for _ in range(bb_count):
                bb_index = self.read_uint32(f)
                bb_start_line = self.read_uint32(f)

                self.bb[bb_index] = bb_start_line

                file_blocks.append(bb_index)

            if "contrib/" in file_path:
                self.excluded_bb.extend(file_blocks)
            else:
                self.files.append((file_path, file_blocks))

    def append(self, name, hit_bb):
        diff = set(hit_bb) - set(self.excluded_bb)
        self.tests.append((name, diff))

    def read_tests(self, f):
        test_entry_magic = 0xcafecafe if self.is_le else 0xfecafeca

        if self.read_uint32(f) != test_entry_magic:
            raise Exception("Corrupted file")

        while True:
            hit_bb = []

            while True:
                try:
                    token = self.read_uint32(f)
                except Exception:
                    self.append("", hit_bb)
                    return

                if token == test_entry_magic:
                    break

                hit_bb.append(token)

            self.append("", hit_bb)
