import difflib
import time

class TSV:
    """Helper to get pretty diffs between expected and actual tab-separated value files"""

    def __init__(self, contents):
        if isinstance(contents, file):
            raw_lines = contents.readlines()
        elif isinstance(contents, str) or isinstance(contents, unicode):
            raw_lines = contents.splitlines(True)
        elif isinstance(contents, list):
            raw_lines = ['\t'.join(map(str, l)) if isinstance(l, list) else str(l) for l in contents]
        else:
            raise TypeError("contents must be either file or string or list, actual type: " + type(contents).__name__)
        self.lines = [l.strip() for l in raw_lines if l.strip()]

    def __eq__(self, other):
        if not isinstance(other, TSV):
            return self == TSV(other)
        return self.lines == other.lines

    def __ne__(self, other):
        if not isinstance(other, TSV):
            return self != TSV(other)
        return self.lines != other.lines

    def diff(self, other, n1=None, n2=None):
        if not isinstance(other, TSV):
            return self.diff(TSV(other), n1=n1, n2=n2)
        return list(line.rstrip() for line in difflib.unified_diff(self.lines, other.lines, fromfile=n1, tofile=n2))[2:]

    def __str__(self):
        return '\n'.join(self.lines)

    @staticmethod
    def toMat(contents):
        return [line.split("\t") for line in contents.split("\n") if line.strip()]

def assert_eq_with_retry(instance, query, expectation, retry_count=20, sleep_time=0.5, stdin=None, timeout=None, settings=None, user=None, ignore_error=False):
    expectation_tsv = TSV(expectation)
    for i in xrange(retry_count):
        try:
            if TSV(instance.query(query, user=user, stdin=stdin, timeout=timeout, settings=settings, ignore_error=ignore_error)) == expectation_tsv:
                break
            time.sleep(sleep_time)
        except Exception as ex:
            print "assert_eq_with_retry retry {} exception {}".format(i + 1, ex)
            time.sleep(sleep_time)
    else:
        val = TSV(instance.query(query, user=user, stdin=stdin, timeout=timeout, settings=settings, ignore_error=ignore_error))
        if expectation_tsv != val:
            raise AssertionError("'{}' != '{}'\n{}".format(expectation_tsv, val, '\n'.join(expectation_tsv.diff(val, n1="expectation", n2="query"))))
