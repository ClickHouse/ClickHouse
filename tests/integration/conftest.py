from helpers.test_tools import TSV


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, TSV) and isinstance(right, TSV) and op == '==':
        return ['TabSeparated values differ: '] + left.diff(right)
