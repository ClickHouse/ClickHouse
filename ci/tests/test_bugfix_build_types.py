"""
Tests for `ci.jobs.scripts.bugfix_validation.bugfix_build_types`.

The per-arch Bugfix Validation jobs (PR #103541) download the master-HEAD
binaries from S3 to run the regression test on master HEAD. The build-type set
must match the runner architecture: the aarch64 job runs on an ARM runner, so
it must download the ARM builds (`build_arm_*/clickhouse`). Downloading the x86
builds there fails to install (`x86_64-binfmt-P: Could not open
'/lib64/ld-linux-x86-64.so.2'`), so the aarch64 side cannot produce a
validated/not-validated signal. See ClickHouse/ClickHouse#103541.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.bugfix_validation import (
    BUGFIX_BUILD_TYPES,
    BUGFIX_BUILD_TYPES_ARM,
    bugfix_build_types,
)


def test_aarch64_job_selects_arm_builds():
    """The aarch64 per-arch job must use the ARM build set."""
    bt = bugfix_build_types("Bugfix validation (functional tests, aarch64)")
    assert bt == BUGFIX_BUILD_TYPES_ARM
    assert all(b.startswith("arm_") for b in bt)


def test_amd64_job_selects_amd_builds():
    """The amd64 per-arch job must use the x86 build set."""
    bt = bugfix_build_types("Bugfix validation (functional tests, amd64)")
    assert bt == BUGFIX_BUILD_TYPES
    assert all(b.startswith("amd_") for b in bt)


def test_integration_aarch64_job_selects_arm_builds():
    """Same arch-aware selection for the integration-tests variant."""
    bt = bugfix_build_types("Bugfix validation (integration tests, aarch64)")
    assert bt == BUGFIX_BUILD_TYPES_ARM


def test_integration_amd64_job_selects_amd_builds():
    bt = bugfix_build_types("Bugfix validation (integration tests, amd64)")
    assert bt == BUGFIX_BUILD_TYPES


def test_unknown_arch_defaults_to_amd():
    """A job name without an arch token defaults to x86 (the historical set)."""
    assert bugfix_build_types("Bugfix validation (functional tests)") == BUGFIX_BUILD_TYPES
    assert bugfix_build_types("") == BUGFIX_BUILD_TYPES


def test_arm_and_amd_sets_are_first_build_aligned():
    """Both sets must lead with the same sanitizer (asan_ubsan): the runners
    install `build_types[0]` as the primary binary and size the memory limit on
    `asan_ubsan in build_type`, so the first entry must be the asan build on
    both arches.
    """
    assert "asan_ubsan" in BUGFIX_BUILD_TYPES[0]
    assert "asan_ubsan" in BUGFIX_BUILD_TYPES_ARM[0]
    # Same build flavors in the same order, differing only by arch prefix.
    assert [b.split("_", 1)[1] for b in BUGFIX_BUILD_TYPES] == [
        b.split("_", 1)[1] for b in BUGFIX_BUILD_TYPES_ARM
    ]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
