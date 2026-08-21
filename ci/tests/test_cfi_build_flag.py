"""
Tests that `tests/clickhouse-test` classifies the CFI build profile correctly.

The `amd_cfi` stateless lanes of `WeeklyCFI` rely on `collect_build_flags`
recognizing the CFI compile flags: the `cfi` build flag is what makes a
`no-cfi` test tag skip there, while CFI must not count as a runtime sanitizer
(it attaches no runtime and runs at release speed), so `SANITIZED` stays false
and tests tagged `no-sanitizers` still run.

Only `WeeklyCFI` builds this profile and it is a scheduled workflow, so no
pull request can exercise these predicates against a real CFI binary. These
tests pin the classification directly so that reverting it fails CI - the
same approach as test_cfi_not_a_sanitizer_build.py takes for the test-config
installer and the integration-test helpers, whose flag strings are reused.
"""

import importlib.machinery
import importlib.util
import os
import types

# Measured CXX_FLAGS rows, same as in test_cfi_not_a_sanitizer_build.py (not
# imported from there: that module pulls in the integration-test helpers and
# their heavy dependencies just to test a different predicate).
CFI = (
    "-O2 -g -DNDEBUG -flto=thin -fwhole-program-vtables "
    "-fsanitize=cfi-vcall,cfi-derived-cast -fno-sanitize-recover=cfi "
    "-fvisibility=default -fno-pie"
)
ASAN = (
    "-O2 -g -fno-omit-frame-pointer -DSANITIZER -fsanitize=address "
    "-fsanitize-address-use-after-scope"
)

_SCRIPT = os.path.join(os.path.dirname(__file__), "../../tests/clickhouse-test")

_loader = importlib.machinery.SourceFileLoader("clickhouse_test_script", _SCRIPT)
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
ct = importlib.util.module_from_spec(_spec)
_loader.exec_module(ct)


def _collect_build_flags(monkeypatch, cxx_flags):
    """Run the real collect_build_flags against a server whose build_options
    CXX_FLAGS row is `cxx_flags`, with all other probed values pinned to the
    plain-release answers."""

    def fake_execute(args, query, *ignored_args, **ignored_kwargs):
        if "CXX_FLAGS" in query:
            return cxx_flags.encode()
        if "WITH_COVERAGE" in query:  # also matches WITH_COVERAGE_DEPTH
            return b"OFF"
        if "BUILD_TYPE" in query:
            return b"RelWithDebInfo"
        if "allow_deprecated_database_ordinary" in query:
            return b"0"
        if "min_bytes_for_wide_part" in query:
            return b"10485760"
        if "USE_%" in query:
            return b""
        if "SYSTEM_PROCESSOR" in query:
            return b"x86_64"
        raise AssertionError(f"unexpected query in collect_build_flags: {query}")

    monkeypatch.setattr(ct, "clickhouse_execute", fake_execute)
    return ct.collect_build_flags(types.SimpleNamespace(db_engine=None))


def test_cfi_flags_produce_the_cfi_build_flag(monkeypatch):
    flags = _collect_build_flags(monkeypatch, CFI)
    assert ct.BuildFlags.CFI in flags
    assert not ct.SANITIZED


def test_sanitizer_flags_do_not_produce_the_cfi_build_flag(monkeypatch):
    flags = _collect_build_flags(monkeypatch, ASAN)
    assert ct.BuildFlags.CFI not in flags
    assert ct.BuildFlags.ADDRESS in flags
    assert ct.SANITIZED


def test_cfi_is_not_a_sanitizer_build_flag():
    assert ct.BuildFlags.CFI not in ct.BuildFlags.SANITIZERS


class _FalsyArgs:
    """An args namespace where every flag clickhouse-test may consult along
    the should_skip_test path reads as unset."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getattr__(self, name):
        return None


def _skip_reason(tags, build_flags):
    """Run the real should_skip_test tag predicate for a test carrying `tags`
    against a build classified as `build_flags`."""
    case = ct.TestCase.__new__(ct.TestCase)
    case.name = "00000_dummy"
    case.ext = ".sql"
    case.tags = set(tags)
    case.args = _FalsyArgs(build_flags=build_flags)
    suite = _FalsyArgs(
        skip_list=[],
        private_skip_list=[],
        cloud_skip_list=[],
        suite_path="/nonexistent",
    )
    return case.should_skip_test(suite)


def test_no_cfi_tag_skips_in_the_cfi_build():
    reason = _skip_reason({"no-cfi"}, [ct.BuildFlags.CFI, ct.BuildFlags.RELEASE])
    assert reason == ct.FailureReason.BUILD


def test_no_sanitizers_tag_runs_in_the_cfi_build():
    assert _skip_reason({"no-sanitizers"}, [ct.BuildFlags.CFI, ct.BuildFlags.RELEASE]) is None


def test_no_sanitizers_tag_skips_in_a_sanitizer_build():
    assert _skip_reason({"no-sanitizers"}, [ct.BuildFlags.ADDRESS]) == ct.FailureReason.BUILD


def test_no_cfi_tag_runs_in_a_plain_release_build():
    assert _skip_reason({"no-cfi"}, [ct.BuildFlags.RELEASE]) is None
