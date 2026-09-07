import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as performance_tests
from ci.jobs.performance_tests import (
    INSERT_HISTORICAL_DATA,
    SLOWER_QUERIES_FAIL_THRESHOLD,
    too_many_slow,
)


def test_historical_insert_parses_threshold_columns():
    # compare.sh appends changed_threshold/unstable_threshold to
    # all-query-metrics.tsv. The historical-data CIDB insert reads that file
    # with a fixed input() TSV schema; if the schema does not declare these two
    # trailing columns, strict TSV parsing drops every row silently (0 rows
    # inserted, no error). This guards that coupling.
    assert "changed_threshold Float64" in INSERT_HISTORICAL_DATA
    assert "unstable_threshold Float64" in INSERT_HISTORICAL_DATA


def test_gate_threshold_value():
    # The Praktika gate must stay aligned with report.py's "> 10" slower-query
    # threshold. If this constant changes, report.py must change in lockstep.
    assert SLOWER_QUERIES_FAIL_THRESHOLD == 10


def test_no_slower_queries_does_not_fail():
    assert too_many_slow("ok") is False
    assert too_many_slow("3 faster") is False


def test_slower_count_at_or_below_threshold_does_not_fail():
    # 6-10 slower queries used to fail the check with the old threshold of 5.
    # They must now pass, which is the whole point of the change.
    for n in (1, 5, 6, 9, 10):
        assert too_many_slow(f"{n} slower") is False, n
        assert too_many_slow(f"2 faster, {n} slower") is False, n


def test_slower_count_above_threshold_fails():
    for n in (11, 12, 50):
        assert too_many_slow(f"{n} slower") is True, n
        assert too_many_slow(f"1 faster, {n} slower") is True, n


def test_cidb_inserts_are_best_effort_not_asserted():
    # CIDB metrics/history inserts are a reporting side-effect, NOT the perf
    # verdict. A bare `assert cidb.is_ready()` turns a transient LogCluster
    # (play.clickhouse.com) timeout into a whole-job exit-1, which is exactly
    # what failed arm_release/master_head shards (PR #107236). Every is_ready()
    # call site in the perf job must use the graceful skip-and-warn guard
    # (`if not cidb.is_ready(): ... return True`) instead of asserting.
    source = inspect.getsource(performance_tests.main)
    assert "assert cidb.is_ready()" not in source, (
        "A bare `assert cidb.is_ready()` re-introduces the whole-job failure on "
        "transient CIDB timeouts. Use `if not cidb.is_ready(): ... return True`."
    )
    # All four is_ready() call sites must be guarded; none asserted.
    assert source.count("cidb.is_ready()") == 4
    assert source.count("if not cidb.is_ready():") == 4


if __name__ == "__main__":
    test_historical_insert_parses_threshold_columns()
    test_gate_threshold_value()
    test_no_slower_queries_does_not_fail()
    test_slower_count_at_or_below_threshold_does_not_fail()
    test_slower_count_above_threshold_fails()
    test_cidb_inserts_are_best_effort_not_asserted()
    print("All perf slow-gate tests passed.")
