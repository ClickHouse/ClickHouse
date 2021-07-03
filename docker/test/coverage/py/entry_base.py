from enum import IntEnum


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


