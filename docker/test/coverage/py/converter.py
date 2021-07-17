import os.path
from tqdm import tqdm
from dir_entry import DirEntry
from files_entry import FileEntry


class Converter:
    def __init__(self, sf_to_funcs, files, tests, bb):
        self.sf_to_funcs = sf_to_funcs
        self.files = files
        self.tests = tests
        self.bb = bb

        # bb_index -> (src, func, block)
        self.bb_to_src_location = \
            [self.bb_to_src(bb_index) for bb_index in bb.keys()]

    def bb_to_src(self, bb_index):
        for file_name, bb in self.files:
            if bb_index not in bb:
                continue

            file_name = os.path.split(file_name)[1]

            for func_name, blocks in self.sf_to_funcs[file_name]:
                for block_index, block in enumerate(blocks):
                    lines = sorted(block.lines)

                    if lines[0] <= self.bb[bb_index] <= lines[-1]:
                        return file_name, func_name, block_index

        raise Exception()

    def convert(self):
        pass
        # for file_path, file_blocks in self.files:
        #     dirs, file_name = os.path.split(file_path)

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

    def find_or_append(self, it, name, test_file):
        for item in it:
            if item.name == name:
                item.add(test_file)
                return

        it.append(DirEntry(name))
        it[-1].add(test_file)

    def get_entries(self):
        dir_entries = []

        # for e in self.gcno.sf_to_funcs

        for file_path, file_blocks in self.files:
            dirs, file_name = os.path.split(file_path)

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
