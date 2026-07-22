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


def test_gitmodules_shape_validates_an_explicit_file(tmp_path, monkeypatch):
    # The merge-base worktree's `.gitmodules` is validated by passing its path explicitly;
    # a non-github URL there must be rejected just like the cwd file. Guards against a
    # clean PR file but an unsafe merge-base file driving the drift fetch.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".gitmodules").write_text(_GOOD_GITMODULES)  # cwd is clean
    mb = tmp_path / "before_src"
    mb.mkdir()
    (mb / ".gitmodules").write_text(
        '[submodule "contrib/evil"]\n\tpath = contrib/evil\n'
        "\turl = git@example.com:evil/repo.git\n"
    )
    assert gitmodules_shape_violation() is None  # PR file clean
    violation = gitmodules_shape_violation(str(mb / ".gitmodules"))
    assert violation and "non-github URL" in violation


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


# --------------------------------------------------------------------------
# prepare_before_worktree submodule population: hardlink only when the
# merge-base pins the same commit the primary has checked out; otherwise
# (pin/URL drift, e.g. the abseil-cpp fork->upstream switch) populate the
# before-worktree's own copy at the merge-base pin instead of hardlinking
# master's content — which would break configure against the merge-base
# cmake wrappers.
# --------------------------------------------------------------------------
def _drive_prepare_before_worktree(
    monkeypatch,
    want_by_path,
    have_by_path,
    fail_update_for=(),
    git_only_dirs=(),
    mb_bad_url=None,
):
    """Run prepare_before_worktree with Shell + os stubbed, returning `((ok, detail),
    calls)`. `want_by_path` = merge-base gitlink SHA per submodule; `have_by_path` =
    primary checked-out SHA per submodule ("" = primary lacks it). `fail_update_for` =
    paths whose `submodule update` should return False. `git_only_dirs` = before-worktree
    dst paths that contain only a `.git` entry (a failed-update remnant). `mb_bad_url` =
    when set, the merge-base `.gitmodules` reports this non-github URL (so the merge-base
    shape guard must reject before any fetch)."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    calls = []

    def fake_check(cmd, **kwargs):
        calls.append(cmd)
        if "submodule update" in cmd:
            return not any(p in cmd for p in fail_update_for)
        return True

    def fake_get_output(cmd, **kwargs):
        calls.append(cmd)
        # gitmodules_shape_violation URL query (merge-base file, quoted before_src path)
        if "--get-regexp" in cmd and ".url" in cmd:
            if mb_bad_url and "before_src" in cmd:
                return f"submodule.contrib/evil.url {mb_bad_url}"
            return ""
        # gitmodules_shape_violation path query (name==path shape)
        if "--get-regexp" in cmd and "\\.path'" in cmd:
            return "\n".join(f"submodule.{p}.path {p}" for p in want_by_path)
        # merge-base .gitmodules path listing (awk '{print $2}' form)
        if "--get-regexp" in cmd and ".gitmodules" in cmd:
            return "\n".join(want_by_path)
        # want: `git -C before_src rev-parse ... HEAD:<path>`
        if "rev-parse" in cmd and "HEAD:" in cmd:
            for p in want_by_path:
                if f"HEAD:{shlex.quote(p)}" in cmd or f"HEAD:{p}" in cmd:
                    return want_by_path[p]
            return ""
        # have: `git -C <path> rev-parse ... HEAD`
        if "rev-parse" in cmd and "HEAD" in cmd:
            for p in have_by_path:
                if f"-C {shlex.quote(p)} " in cmd or f"-C {p} " in cmd:
                    return have_by_path[p]
            return ""
        return ""

    def fake_listdir(p):
        # A dst path listed in git_only_dirs holds only a `.git` remnant.
        if any(p.endswith(d) for d in git_only_dirs):
            return [".git"]
        return ["x"]

    monkeypatch.setattr(job.Shell, "check", staticmethod(fake_check))
    monkeypatch.setattr(job.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(job, "ensure_primary_submodules", lambda: None)
    monkeypatch.setattr(job.os.path, "realpath", lambda p: "/abs/" + p)
    monkeypatch.setattr(job.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(job.os.path, "isfile", lambda p: True)  # SUBMODULE_MARKER present
    monkeypatch.setattr(job.os, "listdir", fake_listdir)

    result = job.prepare_before_worktree("mb_sha", "pr_sha", ["src/X/tests/gtest_x.cpp"])
    return result, calls


def test_prepare_before_worktree_hardlinks_when_pin_matches(monkeypatch):
    # A submodule whose merge-base pin equals the primary's checked-out SHA is
    # content-correct to hardlink.
    (ok, _detail), calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={"contrib/sysroot": "samesha"},
        have_by_path={"contrib/sysroot": "samesha"},
    )
    assert ok is True
    assert any("cp -al -- contrib/sysroot" in c for c in calls), (
        "matching pin must hardlink from the primary checkout"
    )
    assert not any("submodule update" in c for c in calls), (
        "matching pin must not trigger a submodule fetch"
    )


def test_prepare_before_worktree_repopulates_drifted_submodule(monkeypatch):
    # abseil-cpp drift: the merge-base pins the fork commit, master has upstream
    # checked out. Hardlinking master content breaks the merge-base wrapper, so the
    # before-worktree's own copy must be fetched at the merge-base pin.
    (ok, _detail), calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={
            "contrib/sysroot": "samesha",
            "contrib/abseil-cpp": "forkpin",
        },
        have_by_path={
            "contrib/sysroot": "samesha",
            "contrib/abseil-cpp": "upstreampin",
        },
    )
    assert ok is True
    # sysroot (no drift) is hardlinked; abseil (drift) is NOT.
    assert any("cp -al -- contrib/sysroot" in c for c in calls)
    assert not any("cp -al -- contrib/abseil-cpp" in c for c in calls), (
        "a drifted submodule must not be hardlinked from the primary"
    )
    # abseil is sync'd then updated in the before-worktree at its own pin.
    assert any(
        "submodule sync -- contrib/abseil-cpp" in c for c in calls
    ), "drifted submodule must be re-synced to the merge-base URL"
    assert any(
        "submodule update --init" in c and "contrib/abseil-cpp" in c for c in calls
    ), "drifted submodule must be populated at the merge-base pin"


def test_prepare_before_worktree_rejects_unsafe_merge_base_gitmodules(monkeypatch):
    # SECURITY: an unsafe URL in the MERGE-BASE .gitmodules (even if the PR's current file
    # is clean) must fail closed before any submodule fetch — the drift path fetches using
    # the merge-base file.
    (ok, detail), calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={"contrib/abseil-cpp": "forkpin"},
        have_by_path={"contrib/abseil-cpp": "upstreampin"},
        mb_bad_url="git@evil.example.com:x/y.git",
    )
    assert ok is False
    assert "merge-base" in detail and "unsafe" in detail
    assert not any("submodule update" in c for c in calls), (
        "must not fetch any submodule when the merge-base .gitmodules is unsafe"
    )


def test_prepare_before_worktree_fails_closed_on_failed_update(monkeypatch):
    # A failed `submodule update` for a drifted submodule can leave only a `.git`
    # remnant (nonempty dir). The function must fail closed (ok=False) and name the
    # submodule, never report success on a known-incomplete tree.
    (ok, detail), _calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={"contrib/abseil-cpp": "forkpin"},
        have_by_path={"contrib/abseil-cpp": "upstreampin"},
        fail_update_for=("contrib/abseil-cpp",),
        git_only_dirs=("contrib/abseil-cpp",),
    )
    assert ok is False
    assert "contrib/abseil-cpp" in detail


def test_prepare_before_worktree_fails_closed_on_git_only_dir(monkeypatch):
    # Even if the update command reports success, a dst holding only `.git` (no sources)
    # must not count as populated — the final fail-close check catches it.
    (ok, detail), _calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={"contrib/sysroot": "samesha"},
        have_by_path={"contrib/sysroot": "samesha"},
        git_only_dirs=("contrib/sysroot",),
    )
    assert ok is False
    assert "contrib/sysroot" in detail


def test_prepare_before_worktree_repopulates_when_primary_lacks_submodule(monkeypatch):
    # A submodule the merge-base needs but the primary lacks (master removed it) must
    # be populated from the before-worktree's own pin, not silently skipped.
    (ok, _detail), calls = _drive_prepare_before_worktree(
        monkeypatch,
        want_by_path={"contrib/removed_on_master": "mbpin"},
        have_by_path={"contrib/removed_on_master": ""},
    )
    assert ok is True
    assert any(
        "submodule update --init" in c and "contrib/removed_on_master" in c
        for c in calls
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
