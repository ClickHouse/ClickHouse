from entry_base import EntryBase

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
