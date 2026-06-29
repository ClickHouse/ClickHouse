"""
Regression tests for `force_heavy_modules_sequential` (integration flaky check).

PR #105171's flaky integration check (`Integration tests (amd_asan_ubsan, flaky)`)
OOM-hung on the `AMD_MEDIUM` runner (16 cpu, 61 GiB -> workers = min(16//5, 61//11)
= 3). The flaky/targeted parallel bucket runs `-n 3 --dist=each`, so all 3 workers
run every parallel module at once. `test_storage_delta/test.py` starts a 5-node
ClickHouse cluster plus a Spark JVM in its module-scoped fixture; three concurrent
copies meant 15 ASan servers + 3 JVMs on 61 GiB and a kernel OOM (an in-test hang
at `test_checkpoint[1-s3]`). Reducing the per-JVM heap (#106698, 8g->2g) helped but
did not remove the concurrency driver.

The same concurrency driver hits any module that contends on a shared resource
under `--dist=each`. `test_dns_cache` pins fixed IPs on the one shared
`docker_compose_net.yml` subnet, so its concurrent copies serialize on the global
`/tmp/docker_net.lock` and blow the 10-min acquire budget instead of OOMing. It
carries the same flag.

These modules carry `dist_each_sequential=True` in TEST_CONFIGS.
`force_heavy_modules_sequential` reads that flag and moves them out of the
`--dist=each` parallel bucket into the looped sequential bucket (`-n 1`, repeated
>=3x), so at most one such cluster runs at a time while the flakiness signal is
preserved. Normal runs use `--dist=loadfile` (one file -> one worker -> one cluster)
and never call this, so they are unaffected.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.integration_tests_configs import (
    TEST_CONFIGS,
    force_heavy_modules_sequential,
)

HEAVY_PREFIXES = [tc.prefix for tc in TEST_CONFIGS if tc.dist_each_sequential]


def test_heavy_delta_modules_moved_to_sequential():
    # PR #105171 reproducer: all three heavy delta modules start in the parallel
    # (--dist=each) bucket. After re-routing they must be sequential.
    parallel = [
        "test_storage_delta/test.py",
        "test_storage_delta/test_cdf.py",
        "test_storage_delta_disks/test.py",
    ]
    sequential = []
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    assert new_parallel == []
    assert new_sequential == [
        "test_storage_delta/test.py",
        "test_storage_delta/test_cdf.py",
        "test_storage_delta_disks/test.py",
    ]


def test_light_modules_stay_parallel():
    # Non-heavy modules must remain in the concurrent bucket.
    parallel = ["test_storage_s3/test.py", "test_distributed_ddl/test.py"]
    sequential = ["test_server_overload/test.py"]
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    assert new_parallel == ["test_storage_s3/test.py", "test_distributed_ddl/test.py"]
    assert new_sequential == ["test_server_overload/test.py"]


def test_dns_cache_moved_to_sequential():
    # test_dns_cache pins fixed IPs on the one shared docker_compose_net.yml subnet,
    # so concurrent --dist=each copies serialize on the global /tmp/docker_net.lock
    # and blow the 10-min acquire budget. It carries dist_each_sequential=True and
    # must move to the sequential bucket, while light siblings stay parallel.
    parallel = ["test_storage_s3/test.py", "test_dns_cache/test.py"]
    sequential = []
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    assert new_parallel == ["test_storage_s3/test.py"]
    assert new_sequential == ["test_dns_cache/test.py"]


def test_mixed_split_preserves_order_and_existing_sequential():
    parallel = [
        "test_storage_s3/test.py",
        "test_storage_delta/test.py",
        "test_distributed_ddl/test.py",
        "test_storage_delta_disks/test.py",
    ]
    sequential = ["test_server_overload/test.py"]
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    # Heavy ones removed from parallel, order of survivors preserved.
    assert new_parallel == ["test_storage_s3/test.py", "test_distributed_ddl/test.py"]
    # Pre-existing sequential modules kept; heavy ones appended in original order.
    assert new_sequential == [
        "test_server_overload/test.py",
        "test_storage_delta/test.py",
        "test_storage_delta_disks/test.py",
    ]


def test_other_delta_modules_are_not_forced():
    # Only the flagged prefixes are heavy. Lightweight delta modules
    # (no Spark JVM / single instance) stay parallel.
    parallel = [
        "test_storage_delta/test_imds.py",
        "test_storage_delta/test_azure_cluster.py",
        "test_database_delta/test.py",
    ]
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, [])
    assert new_parallel == parallel
    assert new_sequential == []


def test_idempotent_when_nothing_to_move():
    # Returns inputs unchanged (and does not duplicate) when no heavy module is
    # present in the parallel bucket.
    parallel = ["test_storage_s3/test.py"]
    sequential = ["test_storage_delta/test.py"]
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    assert new_parallel == ["test_storage_s3/test.py"]
    assert new_sequential == ["test_storage_delta/test.py"]


def test_heavy_modules_are_not_unconditionally_sequential():
    # The conditional flag must NOT also set is_sequential, otherwise the heavy
    # delta modules would be serialized in the ~140 normal --dist=loadfile jobs
    # too (where one cluster per file is fine). dist_each_sequential is the only
    # flag that should be set for them.
    for tc in TEST_CONFIGS:
        if tc.dist_each_sequential:
            assert not tc.is_sequential, tc.prefix


def test_prefixes_match_real_module_paths():
    # Guard against the TEST_CONFIGS dist_each_sequential prefixes drifting from
    # real test paths. A prefix is either a module file (test_storage_delta/test.py)
    # or a suite directory (test_dns_cache/).
    base = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "integration")
    assert HEAVY_PREFIXES, "expected at least one dist_each_sequential module in TEST_CONFIGS"
    for prefix in HEAVY_PREFIXES:
        path = os.path.join(base, prefix)
        assert os.path.isfile(path) or os.path.isdir(path), prefix
