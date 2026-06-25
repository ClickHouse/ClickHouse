"""
Regression tests for integration test parallel/sequential routing in
`ci/jobs/scripts/integration_tests_configs.py`.

Context (ClickHouse/ClickHouse#108498 CI report): the integration flaky and
targeted checks run pytest with `--dist=each`, which runs *every* selected
module on *every* xdist worker concurrently. `test_dns_cache` uses fixed IPs on
the single shared subnet defined in `helpers/docker_compose_net.yml`, so
`ClickHouseCluster.start()` holds the global host-level flock
`/tmp/docker_net.lock` for the whole cluster lifetime (acquired in `start()`,
released only in `shutdown()`). When the same fixed-subnet module runs on N
workers at once they all serialize on that one lock and exceed the 10-minute
acquire budget, so `cluster.start()` raises in the setup fixture before any test
body runs and pytest reports every case as a FAIL.

The fix marks such fixed-subnet suites `is_sequential=True` so they run on the
sequential path (`-n 1`, repeated for flakiness signal) where only one cluster
exists at a time and the lock is never contended. These tests pin that routing.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.integration_tests_configs import (
    TEST_CONFIGS,
    get_optimal_test_batch,
)


def _one_file_per_prefix():
    """A tests list with one file for each configured prefix.

    `get_optimal_test_batch` asserts every `TEST_CONFIGS` prefix matches at least
    one input file, so cover them all.
    """
    files = []
    for tc in TEST_CONFIGS:
        files.append(tc.prefix + "test.py" if tc.prefix.endswith("/") else tc.prefix)
    return files


def test_dns_cache_is_configured_sequential():
    """test_dns_cache holds the global net lock for its lifetime, so it must not
    be run concurrently with itself under --dist=each."""
    match = [tc for tc in TEST_CONFIGS if tc.prefix == "test_dns_cache/"]
    assert match, "test_dns_cache/ must be present in TEST_CONFIGS"
    assert match[0].is_sequential is True


def test_dns_cache_routed_to_sequential_batch():
    """End to end: get_optimal_test_batch puts test_dns_cache in the sequential
    list (where the flaky check runs it with -n 1), not the parallel list."""
    tests = _one_file_per_prefix() + ["test_plain_parallel/test.py"]
    parallel, sequential = get_optimal_test_batch(
        tests, total_batches=1, batch_num=1, num_workers=4, job_options="", info=None
    )
    assert "test_dns_cache/test.py" in sequential
    assert "test_dns_cache/test.py" not in parallel
    # A suite with no special config still routes to parallel (control).
    assert "test_plain_parallel/test.py" in parallel
