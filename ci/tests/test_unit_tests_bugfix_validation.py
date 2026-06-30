"""
Tests for the pure helpers of `ci.jobs.unit_tests_bugfix_validation_job`.

The unit-test bugfix validator selects the changed `src/**/tests/*` files of a
bugfix PR, parses the gtest test-suite names declared in them, and builds a
`gtest_filter` that the touched suites are then run against (on both the PR
binary and a merge-base "before" binary). This module covers that selection /
parsing / filter-building logic, which silently mis-selects suites if it
regresses. The build/run orchestration is validated separately in CI.

See `ci/jobs/unit_tests_bugfix_validation_job.py` and the analogous functional
inverter tests in `ci/tests/test_bugfix_validation_inverter.py`.
"""

import os
import shlex
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.unit_tests_bugfix_validation_job import (
    _UNIT_TEST_FILE_RE,
    build_gtest_filter,
    derive_test_suites,
    get_changed_unit_test_files,
)


# --------------------------------------------------------------------------
# _UNIT_TEST_FILE_RE: which changed paths count as unit-test sources.
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "path",
    [
        "src/Functions/tests/gtest_foo.cpp",
        "src/Common/tests/gtest_bar.h",
        "src/IO/tests/x.cc",
        "src/A/B/tests/c.hpp",
        "src/Storages/tests/gtest_x.cxx",
        "src/Common/tests/gtest_a.b.cpp",  # dots in the filename
    ],
)
def test_unit_test_file_re_matches(path):
    assert _UNIT_TEST_FILE_RE.match(path)


@pytest.mark.parametrize(
    "path",
    [
        "src/Functions/foo.cpp",        # not under a tests/ dir
        "tests/integration/test_x.py",  # not under src/
        "programs/tests/x.cpp",         # not under src/
        "utils/tests/x.cpp",            # not under src/
        "src/Common/tests/README.md",   # wrong extension
        "src/Common/tests/data.txt",    # wrong extension
        "src/tests/foo.cpp",            # no <Component> dir before tests/
        "docs/src/Common/tests/x.cpp",  # does not start with src/
    ],
)
def test_unit_test_file_re_rejects(path):
    assert not _UNIT_TEST_FILE_RE.match(path)


def test_unit_test_file_re_is_not_a_shell_sanitizer():
    """The regex permits shell metacharacters (quotes/spaces/&/#) in the filename, so it
    must NOT be relied on to make paths shell-safe — `prepare_before_worktree` shell-quotes
    every PR-controlled path instead. This documents that contract so it is not "simplified"
    away. See the SECURITY comments in unit_tests_bugfix_validation_job.py.
    """
    evil = "src/Foo/tests/a' && touch /tmp/pwned #.cpp"
    assert _UNIT_TEST_FILE_RE.match(evil)  # regex matches — it is NOT a sanitizer
    # With shlex.quote the path is a single inert token: parsing the command yields
    # exactly the original path as one argument, with no injected `&&`/`touch` words.
    # (The old f"'{f}'" quoting would split into many tokens here.)
    tokens = shlex.split(f"git checkout HEAD -- {shlex.quote(evil)}")
    assert tokens == ["git", "checkout", "HEAD", "--", evil]


# --------------------------------------------------------------------------
# derive_test_suites: extract gtest suite names from real files on disk.
# --------------------------------------------------------------------------
_SAMPLE = """\
#include <gtest/gtest.h>

TEST(SuiteA, case_one) {}
TEST_F(SuiteB, case_two) {}
TEST_P(SuiteC, case_three) {}
TYPED_TEST(SuiteD, case_four) {}
TYPED_TEST_P(SuiteE, case_five) {}

   TEST_F  (  SuiteF , spaced ) {}     // odd spacing must still match
// TEST(CommentedOut, nope) {}         // commented-out line must be ignored
MY_TEST(NotAMacro, nope) {}            // macro as a substring must not match
EXPECT_TEST(AlsoNot, nope);            // macro as a substring must not match
TEST(SuiteA, duplicate_suite) {}       // duplicate suite collapses
"""


def test_derive_test_suites_all_macro_forms(tmp_path):
    fp = tmp_path / "gtest_sample.cpp"
    fp.write_text(_SAMPLE)
    assert derive_test_suites([str(fp)]) == [
        "SuiteA",
        "SuiteB",
        "SuiteC",
        "SuiteD",
        "SuiteE",
        "SuiteF",
    ]


def test_derive_test_suites_tolerates_missing_file(tmp_path):
    # An unreadable / deleted path is skipped with a warning, never raises.
    assert derive_test_suites([str(tmp_path / "does_not_exist.cpp")]) == []


# --------------------------------------------------------------------------
# build_gtest_filter: plain + parameterized patterns, ordered by suite.
# --------------------------------------------------------------------------
def test_build_gtest_filter_single_suite():
    assert build_gtest_filter(["SuiteA"]) == "SuiteA.*:*/SuiteA.*"


def test_build_gtest_filter_preserves_order():
    assert (
        build_gtest_filter(["SuiteA", "SuiteB"])
        == "SuiteA.*:*/SuiteA.*:SuiteB.*:*/SuiteB.*"
    )


def test_build_gtest_filter_empty():
    assert build_gtest_filter([]) == ""


# --------------------------------------------------------------------------
# get_changed_unit_test_files: regex filter + on-disk existence + dedup/sort.
# --------------------------------------------------------------------------
class _FakeInfo:
    is_local_run = False

    def __init__(self, changed):
        self._changed = changed

    def get_changed_files(self):
        return self._changed


def test_get_changed_unit_test_files_keeps_only_existing_sources(tmp_path, monkeypatch):
    (tmp_path / "src/Common/tests").mkdir(parents=True)
    present_test = "src/Common/tests/gtest_present.cpp"
    present_nontest = "src/Common/foo.cpp"
    (tmp_path / present_test).touch()
    (tmp_path / present_nontest).touch()

    monkeypatch.chdir(tmp_path)
    info = _FakeInfo(
        [
            present_test,                          # matches + exists -> kept
            present_nontest,                       # not a test file -> dropped
            "src/Common/tests/gtest_deleted.cpp",  # matches but missing -> dropped
            "tests/integration/test_x.py",         # not a unit test -> dropped
            present_test,                          # duplicate -> collapsed
        ]
    )
    assert get_changed_unit_test_files(info) == [present_test]


def test_get_changed_unit_test_files_handles_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert get_changed_unit_test_files(_FakeInfo(None)) == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
