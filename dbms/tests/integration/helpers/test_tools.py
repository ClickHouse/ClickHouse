import difflib

class TSV:
    """Helper to get pretty diffs between expected and actual tab-separated value files"""

    def __init__(self, contents):
        self.lines = contents.readlines() if isinstance(contents, file) else contents.splitlines(True)

    def __eq__(self, other):
        return self.lines == other.lines

    def diff(self, other):
        return list(line.rstrip() for line in difflib.context_diff(self.lines, other.lines))[2:]

    @staticmethod
    def toMat(contents):
        return [line.split("\t") for line in contents.split("\n") if line.strip()]
