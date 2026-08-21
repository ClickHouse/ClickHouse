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

GTEST_TEST(SuiteG, case_six) {}        // `TEST` is #define'd to this
GTEST_TEST_F(SuiteH, case_seven) {}    // `TEST_F` is #define'd to this

   TEST_F  (  SuiteF , spaced ) {}     // odd spacing must still match
// TEST(CommentedOut, nope) {}         // commented-out line must be ignored
// GTEST_TEST(CommentedOutG, nope) {}  // commented-out line must be ignored
MY_TEST(NotAMacro, nope) {}            // macro as a substring must not match
MY_GTEST_TEST(AlsoNotAMacro, nope) {}  // macro as a substring must not match
EXPECT_TEST(AlsoNot, nope);            // macro as a substring must not match
GTEST_TEST_(SuiteX, nope) {}           // trailing underscore is a different token
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
        "SuiteG",
        "SuiteH",
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
# get_submodule_state_changes: the fail-close guard against submodule state
# differing between the merge-base and the checkout. The before-worktree
# hardlinks submodule working trees from the primary checkout, whose
# submodules are at the checkout `HEAD`'s recorded revisions (normally the
# synthetic base+PR merge ref) — so both a PR-side gitlink/`.gitmodules` edit
# and a base-only submodule bump after the branch split would make the
# "before" binary silently build against the wrong submodule content. The
# guard must detect any such difference from the raw diff of the two commits
# it is given (main() passes merge-base and `git rev-parse HEAD`).
# --------------------------------------------------------------------------
def _run_submodule_state_changes(monkeypatch, raw_diff):
    import ci.jobs.unit_tests_bugfix_validation_job as job

    calls = []

    def fake_get_output(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return raw_diff

    monkeypatch.setattr(job.Shell, "get_output", staticmethod(fake_get_output))
    changed = job.get_submodule_state_changes("mergebase123", "checkouthead456")

    assert len(calls) == 1
    cmd, kwargs = calls[0]
    # `diff.ignoreSubmodules=all` in the environment would otherwise silently
    # drop every gitlink change and the guard would never fire.
    assert "--ignore-submodules=none" in cmd
    # The guard must fail CLOSE on a failed diff: without strict, Shell.get_output
    # swallows a non-zero git exit (missing object in the shallow checkout, transient
    # error) into an empty string and the guard would silently disable itself.
    assert kwargs.get("strict") is True
    # The diff endpoints are exactly the two commits the caller chose — the
    # merge-base and the checkout whose submodule trees are actually copied.
    assert "mergebase123" in cmd and "checkouthead456" in cmd
    return changed


def test_submodule_state_changes_detects_gitlink_bump(monkeypatch):
    raw_diff = (
        ":100644 100644 1111111 2222222 M\tsrc/Common/tests/gtest_foo.cpp\n"
        ":160000 160000 3333333 4444444 M\tcontrib/zstd\n"
    )
    assert _run_submodule_state_changes(monkeypatch, raw_diff) == ["contrib/zstd"]


def test_submodule_state_changes_detects_added_and_removed_gitlinks(monkeypatch):
    # An added submodule has old_mode 000000, a removed one new_mode 000000 —
    # 160000 appears on only one side, and both must still be caught.
    raw_diff = (
        ":000000 160000 0000000 5555555 A\tcontrib/new-lib\n"
        ":160000 000000 6666666 0000000 D\tcontrib/old-lib\n"
    )
    assert _run_submodule_state_changes(monkeypatch, raw_diff) == [
        "contrib/new-lib",
        "contrib/old-lib",
    ]


def test_submodule_state_changes_detects_gitmodules_only_edit(monkeypatch):
    raw_diff = ":100644 100644 7777777 8888888 M\t.gitmodules\n"
    assert _run_submodule_state_changes(monkeypatch, raw_diff) == [".gitmodules"]


def test_submodule_state_changes_clean_diff(monkeypatch):
    # Regular file changes only — no gitlinks, no `.gitmodules` — must not trip
    # the guard, or every bugfix PR would fail close.
    raw_diff = (
        ":100644 100644 1111111 2222222 M\tsrc/Common/tests/gtest_foo.cpp\n"
        ":100644 100644 3333333 4444444 M\tsrc/Common/Foo.cpp\n"
    )
    assert _run_submodule_state_changes(monkeypatch, raw_diff) == []


def test_submodule_state_changes_empty_diff(monkeypatch):
    assert _run_submodule_state_changes(monkeypatch, "") == []


def test_submodule_state_changes_ignores_malformed_lines(monkeypatch):
    # Non-raw output lines (or a truncated raw entry) must be skipped, not crash
    # or produce bogus paths.
    raw_diff = (
        "warning: some noise from git\n"
        ":160000\tcontrib/truncated-meta\n"
        ":160000 160000 3333333 4444444 M\tcontrib/zstd\n"
    )
    assert _run_submodule_state_changes(monkeypatch, raw_diff) == ["contrib/zstd"]


def test_submodule_state_changes_raises_on_diff_failure(monkeypatch):
    # A failed `git diff` must propagate (fail close), not be treated as "no
    # changes": with strict=True, Shell.get_output raises on a non-zero exit,
    # and the guard must not catch it.
    import ci.jobs.unit_tests_bugfix_validation_job as job

    def fake_get_output(cmd, **kwargs):
        if kwargs.get("strict"):
            raise RuntimeError("command failed with, exit_code 128")
        return ""

    monkeypatch.setattr(job.Shell, "get_output", staticmethod(fake_get_output))
    with pytest.raises(RuntimeError):
        job.get_submodule_state_changes("mergebase123", "checkouthead456")


# --------------------------------------------------------------------------
# submodule_worktree_populated: a submodule directory holding only the
# bookkeeping `.git` entry has no sources — git can leave exactly that state
# after a plain `git submodule update` when the cached gitdir exists but the
# working-tree files were removed. A bare `os.listdir` non-empty check would
# accept it and the hardlink copy would propagate an unbuildable tree.
# --------------------------------------------------------------------------
def test_submodule_worktree_populated_rejects_git_only_dir(tmp_path):
    import ci.jobs.unit_tests_bugfix_validation_job as job

    sub = tmp_path / "contrib" / "abseil-cpp"
    sub.mkdir(parents=True)
    (sub / ".git").write_text("gitdir: ../../.git/modules/contrib/abseil-cpp\n")
    assert job.submodule_worktree_populated(str(sub)) is False


def test_submodule_worktree_populated_rejects_empty_dir(tmp_path):
    import ci.jobs.unit_tests_bugfix_validation_job as job

    sub = tmp_path / "contrib" / "zstd"
    sub.mkdir(parents=True)
    assert job.submodule_worktree_populated(str(sub)) is False


def test_submodule_worktree_populated_accepts_real_content(tmp_path):
    import ci.jobs.unit_tests_bugfix_validation_job as job

    sub = tmp_path / "contrib" / "zstd"
    sub.mkdir(parents=True)
    (sub / ".git").write_text("gitdir: ../../.git/modules/contrib/zstd\n")
    (sub / "lib").mkdir()
    (sub / "lib" / "zstd.h").write_text("// header\n")
    assert job.submodule_worktree_populated(str(sub)) is True


def test_main_guard_compares_merge_base_against_checkout_head(monkeypatch):
    """main() must pass the checkout `HEAD` — the commit whose submodule working trees
    are actually hardlinked into the before-worktree — to the fail-close guard, NOT the
    PR head. Diffing merge-base vs the PR head misses a base-only submodule bump after
    the branch split (the PR's own diff is clean), yet `ensure_primary_submodules`
    checks out the base-tip revision and the hardlink step copies it into the
    merge-base worktree, so the before-binary would build against the wrong contrib
    sources and could report a false reproduction/refutation.
    """
    import ci.jobs.unit_tests_bugfix_validation_job as job

    class _Info:
        pr_labels = ["pr-bugfix"]
        sha = "prheadsha777"
        base_branch = "master"
        is_local_run = False

        def get_changed_files(self):
            return []

    guard_calls = []
    finalized = []

    monkeypatch.setattr(job, "Info", _Info)
    monkeypatch.setattr(
        job, "get_changed_unit_test_files", lambda info: ["src/X/tests/gtest_a.cpp"]
    )
    monkeypatch.setattr(job, "derive_test_suites", lambda files: ["SuiteA"])
    monkeypatch.setattr(job, "gitmodules_shape_violation", lambda: None)
    monkeypatch.setattr(job, "determine_merge_base", lambda info: "mergebase123")
    # HEAD resolves to the synthetic merge-ref commit, different from the PR head.
    monkeypatch.setattr(
        job.Shell,
        "get_output",
        staticmethod(lambda cmd, **kw: "mergerefhead999" if "rev-parse HEAD" in cmd else ""),
    )

    def fake_guard(merge_base, checkout_sha):
        guard_calls.append((merge_base, checkout_sha))
        # Report a base-side drift so main() stops at the guard (fail close).
        return ["contrib/zstd"]

    monkeypatch.setattr(job, "get_submodule_state_changes", fake_guard)
    monkeypatch.setattr(
        job, "finalize", lambda results, info_lines: finalized.append(info_lines)
    )

    job.main()

    assert guard_calls == [("mergebase123", "mergerefhead999")]
    assert finalized and "inconclusive" in finalized[0]


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
