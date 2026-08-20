"""
Tests that the CFI build profile is not classified as a runtime sanitizer build.

The `amd_cfi` profile compiles with Clang CFI, so `CMAKE_CXX_FLAGS_RELWITHDEBINFO`
carries `-fsanitize=cfi-vcall,cfi-derived-cast` (CMakeLists.txt) and that lands in
the `CXX_FLAGS` row of `system.build_options` via cmake/print_flags.cmake. A build
predicate that tests for the bare substring `-fsanitize=` therefore matches CFI,
even though CFI attaches no sanitizer runtime and runs at release speed. Consumers
then apply sanitizer workarounds under CFI: the test-config installer disables
query-profiler symbolization, which empties `symbols` and `lines` in
`system.trace_log`, and integration tests skip themselves.

A runtime sanitizer build is instead marked with `-DSANITIZER`
(cmake/sanitize.cmake), which reaches `CMAKE_CXX_FLAGS` in every branch of the
`if (SANITIZE)` block and is never set for CFI.

Only `WeeklyCFI` builds this profile and it is a scheduled workflow, so no pull
request can exercise these predicates against a real CFI binary. These tests pin
the classification directly so that reverting it fails CI. The flag strings below
are the measured `CXX_FLAGS` rows of real binaries.

See ClickHouse/ClickHouse#115122.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../tests/integration"))

from helpers.cluster import ClickHouseInstance

INSTALLER = os.path.join(os.path.dirname(__file__), "../../tests/config/install.sh")

# Measured CXX_FLAGS rows. CFI is from the amd_cfi binary of the WeeklyCFI job in
# ClickHouse/ClickHouse#115122 (build ID e3992b92853623fcc8ebea13060c0d7603f98201).
CFI = (
    "-O2 -g -DNDEBUG -flto=thin -fwhole-program-vtables "
    "-fsanitize=cfi-vcall,cfi-derived-cast -fno-sanitize-recover=cfi "
    "-fvisibility=default -fno-pie"
)
ASAN = (
    "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=address "
    "-fsanitize-address-use-after-scope"
)
TSAN = "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=thread"
MSAN = (
    "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=memory "
    "-fsanitize-memory-use-after-dtor"
)
UBSAN = (
    "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=undefined "
    "-fno-sanitize-recover=all"
)
ASAN_UBSAN = (
    "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=address,undefined "
    "-fsanitize-address-use-after-scope"
)
DEBUG = "-O0 -g"
RELEASE = "-O3 -DNDEBUG -flto=thin -fwhole-program-vtables"
COVERAGE = "-O2 -DNDEBUG -fsanitize-coverage=trace-pc-guard -DWITH_COVERAGE=1"

SANITIZER_FLAGS = {
    "asan": ASAN,
    "tsan": TSAN,
    "msan": MSAN,
    "ubsan": UBSAN,
    "asan_ubsan": ASAN_UBSAN,
}
NON_SANITIZER_FLAGS = {
    "amd_cfi": CFI,
    "debug": DEBUG,
    "release": RELEASE,
    "coverage": COVERAGE,
}


def classify(flags):
    """Run the real predicate against a build whose CXX_FLAGS row is `flags`."""
    instance = ClickHouseInstance.__new__(ClickHouseInstance)
    instance.query = lambda *args, **kwargs: flags
    return instance


@pytest.mark.parametrize("profile", sorted(SANITIZER_FLAGS))
def test_runtime_sanitizer_builds_are_detected(profile):
    """Every runtime sanitizer profile must still be classified as sanitized."""
    assert classify(SANITIZER_FLAGS[profile]).is_built_with_sanitizer()


@pytest.mark.parametrize("profile", sorted(NON_SANITIZER_FLAGS))
def test_non_sanitizer_builds_are_not_detected(profile):
    """CFI, debug, release and coverage builds carry no sanitizer runtime."""
    assert not classify(NON_SANITIZER_FLAGS[profile]).is_built_with_sanitizer()


def test_cfi_flags_would_match_a_bare_fsanitize_test():
    """Pin why the bare substring test is wrong, so this stays a live oracle.

    Without this, a revert to `-fsanitize=` could look harmless.
    """
    assert "-fsanitize=" in CFI
    assert "-DSANITIZER" not in CFI


@pytest.mark.parametrize("name", ["address", "thread", "memory"])
def test_named_checks_distinguish_between_sanitizers(name):
    """The named checks must keep testing `-fsanitize=<name>`.

    `-DSANITIZER` cannot tell sanitizers apart, and `test_crash_log` passes these
    names through to pick the ASan/TSan/MSan-only cases.
    """
    for profile, flags in {**SANITIZER_FLAGS, **NON_SANITIZER_FLAGS}.items():
        expected = "-fsanitize={}".format(name) in flags
        assert classify(flags).is_built_with_sanitizer(name) == expected, profile


def test_installer_gates_symbolization_on_the_same_invariant():
    """The functional-test installer decides symbolization from the same marker.

    Scoped to `is_sanitizer_build`: `is_fast_build` also tests for `-fsanitize=`,
    but only the Fast test job calls it and that job builds without CFI.
    """
    with open(INSTALLER, "r", encoding="utf-8") as f:
        installer = f.read()
    body = installer.split("function is_sanitizer_build()", 1)
    assert len(body) == 2, "is_sanitizer_build is gone from the installer"
    body = body[1].split("\n}", 1)[0]
    assert "'%-DSANITIZER%'" in body
    assert "LIKE '%-fsanitize=%'" not in body
