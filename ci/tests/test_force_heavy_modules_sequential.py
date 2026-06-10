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

`force_heavy_modules_sequential` moves the heavy delta modules out of the
`--dist=each` parallel bucket into the looped sequential bucket (`-n 1`, repeated
>=3x), so at most one delta cluster runs at a time while the flakiness signal is
preserved. Normal runs use `--dist=loadfile` (one file -> one worker -> one cluster)
and never call this, so they are unaffected.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.integration_tests_configs import (
    FLAKY_FORCE_SEQUENTIAL_PREFIXES,
    force_heavy_modules_sequential,
)


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
    parallel = ["test_storage_s3/test.py", "test_dns_cache/test.py"]
    sequential = ["test_server_overload/test.py"]
    new_parallel, new_sequential = force_heavy_modules_sequential(parallel, sequential)
    assert new_parallel == ["test_storage_s3/test.py", "test_dns_cache/test.py"]
    assert new_sequential == ["test_server_overload/test.py"]


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
    # Only the listed prefixes are heavy. Lightweight delta modules
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


def test_prefixes_match_real_module_paths():
    # Guard against the prefixes drifting from real test module paths.
    base = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "integration")
    for prefix in FLAKY_FORCE_SEQUENTIAL_PREFIXES:
        assert os.path.isfile(os.path.join(base, prefix)), prefix
