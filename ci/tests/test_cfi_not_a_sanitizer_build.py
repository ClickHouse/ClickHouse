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
import subprocess
import sys
from pathlib import Path

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


def symbolization_is_disabled_by_installer(tmp_path, flags):
    """Install the test configs for a build whose `CXX_FLAGS` row is `flags`.

    Reports whether the server reads `trace_log_no_symbolize.xml`, so the link
    must also resolve to the installer's own copy: a dangling link is not an
    installed config. A stub `clickhouse` earlier on PATH answers the installer's
    `system.build_options` probes; the environment is an allow-list so an
    exported `USE_*` cannot steer the run.
    """
    stub = tmp_path / "clickhouse"
    stub.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "flags = {!r}\n"
        "if '--query' not in sys.argv:\n"
        "    print('ClickHouse local version 99.1.1.1.')\n"
        "    raise SystemExit(0)\n"
        "query = sys.argv[sys.argv.index('--query') + 1]\n"
        'start = query.index("\'") + 1\n'
        "pattern = query[start : query.index(\"'\", start)].strip('%')\n"
        "print(int(pattern in flags))\n".format(flags)
    )
    stub.chmod(0o755)

    dest = tmp_path / "server"
    run = subprocess.run(
        ["bash", INSTALLER, str(dest), str(tmp_path / "client")],
        env={"PATH": "{}:{}".format(tmp_path, os.environ["PATH"])},
        capture_output=True,
    )
    assert run.returncode == 0, run.stderr.decode()
    link = dest / "config.d" / "trace_log_no_symbolize.xml"
    return (
        link.is_symlink()
        and link.exists()
        and link.resolve()
        == (Path(INSTALLER).parent / "config.d" / link.name).resolve()
    )


@pytest.mark.parametrize("profile", sorted(NON_SANITIZER_FLAGS))
def test_installer_keeps_symbolization_without_a_sanitizer(tmp_path, profile):
    """CFI, debug, release and coverage builds must keep query-profiler symbols."""
    assert not symbolization_is_disabled_by_installer(
        tmp_path, NON_SANITIZER_FLAGS[profile]
    )


@pytest.mark.parametrize("profile", sorted(SANITIZER_FLAGS))
def test_installer_disables_symbolization_under_a_sanitizer(tmp_path, profile):
    """In-flush symbolization is too slow under a sanitizer runtime."""
    assert symbolization_is_disabled_by_installer(tmp_path, SANITIZER_FLAGS[profile])
