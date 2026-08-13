"""
Tests for `ci.jobs.functional_tests.stateless_memory_limit`.

The per-test cgroup memory limit passed to `clickhouse-test --memory-limit` must
be 10 GiB for sanitizer builds (their clients hold ~500 MiB RSS each, so a test
running ~10 concurrent clients reaches the 5 GiB non-sanitizer cap and the cgroup
OOM-kills them) and 5 GiB otherwise. The classification must match ANY sanitizer
via `SANITIZERS`, not the literal `asan_ubsan` substring: the `tsan` / `msan`
lanes, and the private `amd_ubsan` lane (which runs the ASan+UBSan binary), do
not contain `asan_ubsan` and were previously under-sized to 5 GiB.
See ClickHouse/ClickHouse#111028.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.functional_tests import SANITIZERS, stateless_memory_limit

GiB = 2**30

# Job-name parameter strings as they appear in `Info().job_name`, plus the
# build-type tokens used during bugfix validation. Every sanitizer flavor must
# resolve to 10 GiB regardless of the surrounding option string or arch prefix.
SANITIZER_SOURCES = [
    "Stateless tests (amd_ubsan, SharedCatalog, meta in keeper, s3 storage, sequential)",
    "Stateless tests (amd_tsan, meta in keeper, s3 storage, sequential, 1/2)",
    "Stateless tests (amd_msan, SharedCatalog, meta in keeper, s3 storage, sequential)",
    "Stateless tests (amd_asan_ubsan, distributed plan, parallel)",
    "Stateless tests (arm_tsan, s3 storage, parallel)",
    "Stateless tests (arm_msan, s3 storage, parallel)",
    "Stateless tests (arm_asan_ubsan, s3 storage, parallel)",
    # bugfix-validation build types (passed as `build_type`):
    "amd_asan_ubsan",
    "amd_tsan",
    "amd_msan",
    "arm_asan_ubsan",
]

NON_SANITIZER_SOURCES = [
    "Stateless tests (amd_debug, distributed cache, meta in keeper, s3 storage, sequential)",
    "Stateless tests (amd_release, s3 storage, parallel)",
    "Stateless tests (amd_binary, parallel)",
    "amd_debug",
    "arm_debug",
]


@pytest.mark.parametrize("source", SANITIZER_SOURCES)
def test_sanitizer_lanes_get_10gib(source):
    assert stateless_memory_limit(source) == 10 * GiB


@pytest.mark.parametrize("source", NON_SANITIZER_SOURCES)
def test_non_sanitizer_lanes_get_5gib(source):
    assert stateless_memory_limit(source) == 5 * GiB


def test_private_amd_ubsan_is_treated_as_sanitizer():
    """The private `amd_ubsan` lane runs the ASan+UBSan binary; its name lacks
    the `asan_ubsan` substring but must still get 10 GiB (the original bug)."""
    assert "asan_ubsan" not in "amd_ubsan"
    assert stateless_memory_limit("amd_ubsan") == 10 * GiB


def test_every_sanitizer_token_lifts_the_limit():
    """Each token in `SANITIZERS` on its own must select the 10 GiB tier, so a
    future refactor back to a single literal substring is caught here."""
    for san in SANITIZERS:
        assert stateless_memory_limit(f"amd_{san}") == 10 * GiB


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
