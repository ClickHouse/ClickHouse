from entry_base import EntryBase
import os.path


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
