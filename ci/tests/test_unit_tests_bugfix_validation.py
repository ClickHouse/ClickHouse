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
import re
import shlex
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.unit_tests_bugfix_validation_job import (
    _UNIT_TEST_FILE_RE,
    before_run_started_a_test,
    build_gtest_filter,
    derive_test_suites,
    get_changed_unit_test_files,
    gitmodules_shape_violation,
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
    # Plain, value-parameterized, typed, and type-parameterized patterns.
    assert build_gtest_filter(["SuiteA"]) == "SuiteA.*:*/SuiteA.*:SuiteA/*:*/SuiteA/*"


def test_build_gtest_filter_preserves_order():
    assert build_gtest_filter(["SuiteA", "SuiteB"]) == (
        "SuiteA.*:*/SuiteA.*:SuiteA/*:*/SuiteA/*:"
        "SuiteB.*:*/SuiteB.*:SuiteB/*:*/SuiteB/*"
    )


def test_build_gtest_filter_empty():
    assert build_gtest_filter([]) == ""


@pytest.mark.parametrize(
    "full_name",
    [
        "SuiteA.case1",          # plain / fixture
        "Prefix/SuiteA.case1/0",  # value-parameterized (TEST_P)
        "SuiteA/0.case1",         # typed (TYPED_TEST)
        "Prefix/SuiteA/0.case1",  # type-parameterized (TYPED_TEST_P)
    ],
)
def test_build_gtest_filter_matches_all_gtest_name_forms(full_name):
    """Every gtest naming form for a touched suite is matched by some pattern — otherwise
    a typed-only regression test would run zero cases on the before-binary."""
    patterns = build_gtest_filter(["SuiteA"]).split(":")

    def gtest_match(pattern, name):
        # gtest filter semantics: '*' matches any substring, '?' any char, '.'/'/' literal.
        regex = "^" + "".join(
            ".*" if c == "*" else ("." if c == "?" else re.escape(c)) for c in pattern
        ) + "$"
        return re.match(regex, name) is not None

    assert any(gtest_match(p, full_name) for p in patterns), full_name


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


# --------------------------------------------------------------------------
# gitmodules_shape_violation: reject unsafe submodule metadata before any fetch.
# --------------------------------------------------------------------------
_GOOD_GITMODULES = (
    '[submodule "contrib/foo"]\n\tpath = contrib/foo\n'
    "\turl = https://github.com/ClickHouse/foo.git\n"
)


def _write_gitmodules(tmp_path, monkeypatch, content):
    (tmp_path / ".gitmodules").write_text(content)
    monkeypatch.chdir(tmp_path)


def test_gitmodules_shape_clean(tmp_path, monkeypatch):
    _write_gitmodules(tmp_path, monkeypatch, _GOOD_GITMODULES)
    assert gitmodules_shape_violation() is None


def test_gitmodules_shape_rejects_non_github_url(tmp_path, monkeypatch):
    _write_gitmodules(
        tmp_path,
        monkeypatch,
        '[submodule "contrib/evil"]\n\tpath = contrib/evil\n'
        "\turl = https://evil.example.com/x.git\n",
    )
    violation = gitmodules_shape_violation()
    assert violation and "contrib/evil" in violation and "non-github" in violation


@pytest.mark.parametrize(
    "url",
    [
        "git@github.com:ClickHouse/foo.git",       # ssh form, not https
        "https://github.com.evil.com/foo.git",     # look-alike host (no trailing slash after github.com)
        "file:///etc/passwd",                       # local file
        "http://169.254.169.254/foo",               # plain http to metadata IP
    ],
)
def test_gitmodules_shape_rejects_dangerous_urls(tmp_path, monkeypatch, url):
    _write_gitmodules(
        tmp_path,
        monkeypatch,
        f'[submodule "contrib/foo"]\n\tpath = contrib/foo\n\turl = {url}\n',
    )
    assert gitmodules_shape_violation() is not None


def test_gitmodules_shape_rejects_name_path_mismatch(tmp_path, monkeypatch):
    _write_gitmodules(
        tmp_path,
        monkeypatch,
        '[submodule "contrib/foo"]\n\tpath = contrib/bar\n'
        "\turl = https://github.com/ClickHouse/foo.git\n",
    )
    violation = gitmodules_shape_violation()
    assert violation and "not equal to its path" in violation


@pytest.mark.parametrize(
    "path",
    [
        "../../../../ClickHouse",   # parent traversal — the reported exploit
        "contrib/../../etc",        # `..` component after a valid-looking prefix
        "/etc/cron.d/evil",         # absolute path
        "evil",                     # outside contrib/
    ],
)
def test_gitmodules_shape_rejects_traversal_path(tmp_path, monkeypatch, path):
    # name == path and a github URL, so only the path-shape guard can reject these.
    # Otherwise `dst = os.path.join(BEFORE_SRC, path)` would escape before_src and the
    # `rm -rf` in prepare_before_worktree could delete the mounted checkout itself.
    _write_gitmodules(
        tmp_path,
        monkeypatch,
        f'[submodule "{path}"]\n\tpath = {path}\n'
        "\turl = https://github.com/ClickHouse/foo.git\n",
    )
    violation = gitmodules_shape_violation()
    assert violation and "unsafe path" in violation


# --------------------------------------------------------------------------
# determine_merge_base: must anchor on the PR head (info.sha), not `git HEAD`,
# because the default checkout is the base+PR merge commit.
# --------------------------------------------------------------------------
def test_determine_merge_base_uses_pr_head_not_git_head(monkeypatch):
    import ci.jobs.unit_tests_bugfix_validation_job as job

    calls = []

    def fake_check(cmd, **kwargs):
        calls.append(cmd)
        return True

    def fake_get_output(cmd, **kwargs):
        calls.append(cmd)
        return "abc123mergebase"

    monkeypatch.setattr(job.Shell, "check", staticmethod(fake_check))
    monkeypatch.setattr(job.Shell, "get_output", staticmethod(fake_get_output))

    class _Info:
        sha = "deadbeefprheadsha"
        base_branch = "master"

    assert job.determine_merge_base(_Info()) == "abc123mergebase"

    merge_base_cmds = [c for c in calls if "merge-base" in c]
    assert merge_base_cmds, "no `git merge-base` command was issued"
    # It computes merge-base of the PR head vs the base, never of the merge commit (HEAD).
    for c in merge_base_cmds:
        assert "deadbeefprheadsha" in c
        assert "merge-base HEAD " not in c


# --------------------------------------------------------------------------
# before_run_started_a_test: the "[ RUN ]"-marker guard. A clean before-run
# that executed no touched test (marker absent) must NOT be treated as a
# refutation — `unit_tests_dbms` is built from `gtest*.cpp` only, so a touched
# standalone test file matches `_UNIT_TEST_FILE_RE` yet is never compiled, and
# the filter then matches zero cases (exit 0, no "[ RUN ]"). That is
# inconclusive, not a "failed to reproduce".
# --------------------------------------------------------------------------
class _FakeResult:
    def __init__(self, files):
        self.files = files


def test_before_run_started_a_test_detects_run_marker(tmp_path):
    log = tmp_path / "gtest.log"
    log.write_text("[==========] Running 1 test.\n[ RUN      ] Suite.Case\n[       OK ]\n")
    assert before_run_started_a_test(_FakeResult([str(log)])) is True


def test_before_run_started_a_test_no_marker_is_inconclusive(tmp_path):
    # A suite that is not compiled into `unit_tests_dbms` matches zero cases: the
    # binary runs and exits cleanly without ever printing a "[ RUN ]" marker.
    log = tmp_path / "gtest.log"
    log.write_text("[==========] Running 0 tests from 0 test suites.\n[  PASSED  ] 0 tests.\n")
    assert before_run_started_a_test(_FakeResult([str(log)])) is False


def test_before_run_started_a_test_handles_no_files():
    assert before_run_started_a_test(_FakeResult(None)) is False
    assert before_run_started_a_test(_FakeResult([])) is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
