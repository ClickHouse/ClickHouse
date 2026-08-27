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
    BEFORE_SRC,
    BEFORE_SRC_NORMALIZED,
    attribute_compile_errors,
    before_run_started_a_test,
    build_gtest_filter,
    compile_failure_attribution,
    derive_test_suites,
    failed_compile_edge_sources,
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
# --------------------------------------------------------------------------
# attribute_compile_errors: which side of the before-build failure the compile
# errors belong to. Only "every error is inside the PR's overlaid test files"
# may become a green `XFAIL`; anything else (an error in the fix sources, a
# linker error, a log with no parsable diagnostic) must fail close, so that a
# regression in this parser cannot turn an unrelated build failure green.
# --------------------------------------------------------------------------
def _compile_result(tmp_path, *logs):
    """Build a fake compile Result whose `files` hold the given log contents."""
    paths = []
    for i, content in enumerate(logs):
        log = tmp_path / f"compile_{i}.log"
        log.write_text(content)
        paths.append(str(log))
    return _FakeResult(paths)


_OVERLAID = "src/Interpreters/tests/gtest_distributed_query.cpp"


def test_attribute_compile_errors_all_inside_overlaid_files(tmp_path):
    # The real shape this handles: the overlaid call site was adapted to the
    # signature the fix introduces, so it cannot compile on the merge base.
    result = _compile_result(
        tmp_path,
        f"[1/2] Building CXX object {_OVERLAID}.o\n"
        f"{BEFORE_SRC}/{_OVERLAID}:42:5: error: no matching function for call to 'createExchangeLookup'\n"
        f"{BEFORE_SRC}/{_OVERLAID}:99:11: error: too many arguments to function call\n"
        "ninja: build stopped: subcommand failed.\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == [_OVERLAID]
    assert other == []


def test_attribute_compile_errors_strips_ansi_colors(tmp_path):
    # clang colorizes diagnostics when it thinks it writes to a terminal.
    result = _compile_result(
        tmp_path,
        f"\x1b[1m{BEFORE_SRC}/{_OVERLAID}:42:5: \x1b[0m\x1b[0;1;31merror: \x1b[0mno matching function\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == [_OVERLAID]
    assert other == []


def test_attribute_compile_errors_error_in_fix_sources_fails_close(tmp_path):
    # An error outside the overlaid tests is not attributable to the test
    # changes: the caller must report ERROR, never XFAIL.
    result = _compile_result(
        tmp_path,
        f"{BEFORE_SRC}/src/Interpreters/ExchangeLookup.cpp:10:1: error: unknown type name 'Foo'\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == []
    assert other == ["src/Interpreters/ExchangeLookup.cpp"]


def test_attribute_compile_errors_mixed_errors_fail_close(tmp_path):
    result = _compile_result(
        tmp_path,
        f"{BEFORE_SRC}/{_OVERLAID}:42:5: error: no matching function\n"
        f"{BEFORE_SRC}/src/Interpreters/ExchangeLookup.cpp:10:1: fatal error: broken\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == [_OVERLAID]
    assert other == ["src/Interpreters/ExchangeLookup.cpp"]


def test_attribute_compile_errors_linker_error_only_is_unattributable(tmp_path):
    # A link failure carries no `path:line: error:` diagnostic at all. Both
    # lists empty means "not attributable" and the caller fails close.
    result = _compile_result(
        tmp_path,
        "[2/2] Linking CXX executable src/unit_tests_dbms\n"
        "ld.lld: error: undefined symbol: DB::createExchangeLookup(bool)\n"
        "clang++: error: linker command failed with exit code 1\n",
    )
    assert attribute_compile_errors(result, [_OVERLAID]) == ([], [])


def test_attribute_compile_errors_no_diagnostic_is_unattributable(tmp_path):
    result = _compile_result(
        tmp_path, "ninja: build stopped: subcommand failed.\nKilled\n"
    )
    assert attribute_compile_errors(result, [_OVERLAID]) == ([], [])


def test_attribute_compile_errors_ignores_warnings_and_notes(tmp_path):
    # Only hard errors attribute a failure; a warning or a note must not make
    # an otherwise unattributable failure look like an expected one.
    result = _compile_result(
        tmp_path,
        f"{BEFORE_SRC}/{_OVERLAID}:42:5: warning: unused variable 'x' [-Wunused-variable]\n"
        f"{BEFORE_SRC}/src/Interpreters/ExchangeLookup.h:7:1: note: candidate function not viable\n",
    )
    assert attribute_compile_errors(result, [_OVERLAID]) == ([], [])


def test_attribute_compile_errors_paths_outside_the_worktree_stay_verbatim(tmp_path):
    # contrib/system headers are compiled through absolute paths that do not
    # contain the before-worktree marker: they stay as-is and count as "other".
    result = _compile_result(
        tmp_path,
        "/usr/include/c++/v1/vector:100:5: error: static assertion failed\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == []
    assert other == ["/usr/include/c++/v1/vector"]


def test_attribute_compile_errors_reads_every_log_and_deduplicates(tmp_path):
    result = _compile_result(
        tmp_path,
        f"{BEFORE_SRC}/{_OVERLAID}:42:5: error: no matching function\n",
        f"{BEFORE_SRC}/{_OVERLAID}:77:9: error: no matching function\n"
        f"{BEFORE_SRC}/src/Interpreters/ExchangeLookup.cpp:1:1: error: boom\n",
    )
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == [_OVERLAID]
    assert other == ["src/Interpreters/ExchangeLookup.cpp"]


def test_attribute_compile_errors_survives_an_unreadable_log(tmp_path):
    # A missing/unreadable log is skipped with a warning, not raised: the other
    # logs still decide the attribution.
    good = tmp_path / "good.log"
    good.write_text(f"{BEFORE_SRC}/{_OVERLAID}:42:5: error: no matching function\n")
    result = _FakeResult([str(tmp_path / "missing.log"), str(good)])
    overlaid, other = attribute_compile_errors(result, [_OVERLAID])
    assert overlaid == [_OVERLAID]
    assert other == []


def test_attribute_compile_errors_handles_no_files():
    assert attribute_compile_errors(_FakeResult(None), [_OVERLAID]) == ([], [])
    assert attribute_compile_errors(_FakeResult([]), [_OVERLAID]) == ([], [])


# --------------------------------------------------------------------------
# main(): the compile-failure branch that turns the attribution tuple into a
# user-visible verdict. `attribute_compile_errors` is only half the contract —
# the other half is `if overlaid_errors and not other_errors` in main(), which
# is the single place where a RED before-build becomes a GREEN `XFAIL`. These
# tests drive the real branch end to end with the *absolute*
# `/ClickHouse/ci/tmp/before_src/...` diagnostics the production build actually
# emits (cmake is configured from `BEFORE_SRC_NORMALIZED`), so both the
# marker-based path normalization and the XFAIL-vs-ERROR decision are pinned.
# --------------------------------------------------------------------------
def _run_main_with_compile_failure(monkeypatch, tmp_path, log_text, test_files):
    """Drive main() up to the compile step, which fails with the given log.

    Everything before the compile is stubbed out (no git, no worktree, no cmake);
    the compile failure is a real `Result` carrying a real log file, so the
    production attribution and branch run unmodified.
    """
    import ci.jobs.unit_tests_bugfix_validation_job as job

    class _Info:
        pr_labels = ["pr-bugfix"]
        sha = "prheadsha777"
        base_branch = "master"
        is_local_run = False

        def get_changed_files(self):
            return list(test_files)

    monkeypatch.setattr(job, "Info", _Info)
    monkeypatch.setattr(job, "get_changed_unit_test_files", lambda info: list(test_files))
    monkeypatch.setattr(job, "derive_test_suites", lambda files: ["SuiteA"])
    monkeypatch.setattr(job, "gitmodules_shape_violation", lambda: None)
    monkeypatch.setattr(job, "determine_merge_base", lambda info: "mergebase123")
    monkeypatch.setattr(
        job.Shell, "get_output", staticmethod(lambda cmd, **kw: "checkouthead999")
    )
    monkeypatch.setattr(job, "get_submodule_state_changes", lambda base, head: [])
    monkeypatch.setattr(job, "prepare_before_worktree", lambda base, sha, files: True)
    monkeypatch.setattr(job, "reset_before_build_dir", lambda: True)
    monkeypatch.setattr(
        job,
        "configure_before_binary",
        lambda info, build_type: job.Result(
            name=f"Configure before-binary (cmake, {build_type})",
            status=job.Result.Status.OK,
        ),
    )

    log = tmp_path / "compile.log"
    log.write_text(log_text)
    monkeypatch.setattr(
        job,
        "compile_before_binary",
        lambda build_type: job.Result(
            name="Compile before-binary (ninja unit_tests_dbms, without the fix)",
            status=job.Result.Status.FAIL,
            files=[str(log)],
        ),
    )

    finalized = {}

    def fake_finalize(results, info_lines):
        finalized["results"] = results
        finalized["info"] = info_lines

    monkeypatch.setattr(job, "finalize", fake_finalize)
    # Nothing may run after the compile branch: the before-binary does not exist.
    monkeypatch.setattr(
        job,
        "run_gtests",
        lambda *a, **kw: pytest.fail("main() must stop at the compile failure"),
    )

    job.main()
    assert "results" in finalized, "main() returned without finalizing a report"
    return job, finalized


def test_main_compile_failure_only_in_overlaid_tests_is_xfail(monkeypatch, tmp_path):
    # Every error is inside the PR's overlaid test file, addressed by its real
    # absolute path in the before-worktree: nothing to validate, report XFAIL.
    job, finalized = _run_main_with_compile_failure(
        monkeypatch,
        tmp_path,
        f"FAILED: src/CMakeFiles/unit_tests_dbms.dir/{_OVERLAID}.o\n"
        f"{BEFORE_SRC_NORMALIZED}/{_OVERLAID}:42:5: error: no matching function for call to 'createExchangeLookup'\n"
        "ninja: build stopped: subcommand failed.\n",
        [_OVERLAID],
    )
    compile_result = finalized["results"][-1]
    assert compile_result.status == job.Result.Status.XFAIL
    assert job.Result.Label.XFAIL in [
        label["name"] for label in compile_result.ext.get("labels", [])
    ]
    assert _OVERLAID in compile_result.info
    assert "Nothing to validate on the unit side" in finalized["info"]


def test_main_compile_failure_outside_overlaid_tests_is_error(monkeypatch, tmp_path):
    # The overlaid test fails to compile too, but so does a fix source: the
    # failure cannot be attributed to the test changes — fail close with ERROR.
    job, finalized = _run_main_with_compile_failure(
        monkeypatch,
        tmp_path,
        f"{BEFORE_SRC_NORMALIZED}/{_OVERLAID}:42:5: error: no matching function\n"
        f"{BEFORE_SRC_NORMALIZED}/src/Interpreters/ExchangeLookup.cpp:17:9: error: use of undeclared identifier 'x'\n",
        [_OVERLAID],
    )
    compile_result = finalized["results"][-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert "src/Interpreters/ExchangeLookup.cpp" in compile_result.info
    assert "inconclusive" in finalized["info"]


def test_main_compile_failure_without_a_diagnostic_is_error(monkeypatch, tmp_path):
    # A link failure produces no compiler diagnostic at all: unattributable, ERROR.
    job, finalized = _run_main_with_compile_failure(
        monkeypatch,
        tmp_path,
        "ld.lld: error: undefined symbol: DB::createExchangeLookup()\n"
        "ninja: build stopped: subcommand failed.\n",
        [_OVERLAID],
    )
    compile_result = finalized["results"][-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert "inconclusive" in finalized["info"]


# --------------------------------------------------------------------------
# compile_failure_attribution: which before-build compile failures belong to
# the overlaid test files alone (XFAIL) and which must fail close (ERROR).
#
# Attribution by the path on the `error:` line alone misses the failure clang
# reports inside a header the overlaid test includes: a construction through
# `std::make_unique` is performed on a libcxx line, so the only `error:` line
# names a contrib path and the overlaid test appears merely on a `note: in
# instantiation of ...` line. The failing ninja edge names the translation unit
# instead. Every other shape (a failed fix-source or contrib translation unit,
# a link failure, no parsable diagnostic) must stay an ERROR.
# --------------------------------------------------------------------------
_TEST_FILE = "src/Disks/tests/gtest_write_buffer_inline_or_blob.cpp"
_BEFORE = "/ClickHouse/ci/tmp/before_src"


def _compile_log(tmp_path, *contents):
    """A fake compile Result whose `files` are logs with the given contents."""
    paths = []
    for i, content in enumerate(contents):
        log = tmp_path / f"compile_{i}.log"
        log.write_text(content)
        paths.append(str(log))
    return _FakeResult(paths)


def _failed_compile_edge(output, source):
    """A ninja `FAILED:` edge for `output`, followed by a clang command line
    compiling `source` (abridged, but with the real flag ordering: the `-c
    <source>` pair is the last thing on the line, after `-o <output>`)."""
    return (
        f"FAILED: {output} \n"
        f"/usr/local/bin/clang++-22 --target=x86_64-linux-gnu -DNDEBUG -O2 "
        f"-fsanitize=address,undefined -std=c++23 -MD -MT {output} -MF {output}.d "
        f"-o {output} -c {source}\n"
    )


# The diagnostic of the real failure, verbatim in shape: the single `error:` line
# is inside libcxx, the overlaid test only carries an instantiation `note:`, and a
# further `note:` points at the fix source (which is why a note-path rule is unsafe).
_TEMPLATE_INSTANTIATION_DIAGNOSTIC = (
    f"In file included from {_BEFORE}/{_TEST_FILE}:1:\n"
    f"In file included from {_BEFORE}/contrib/googletest/googletest/include/gtest/gtest.h:55:\n"
    f"{_BEFORE}/contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:770:30: error: "
    "no matching constructor for initialization of 'DB::WriteBufferInlineOrBlob'\n"
    f"{_BEFORE}/{_TEST_FILE}:56:21: note: in instantiation of function template "
    "specialization 'std::make_unique<DB::WriteBufferInlineOrBlob, ...>' requested here\n"
    f"{_BEFORE}/src/Disks/IO/WriteBufferInlineOrBlob.h:29:5: note: candidate constructor "
    "not viable: requires 6 arguments, but 7 were provided\n"
)


def _template_instantiation_log(edge_output="src/CMakeFiles/x.dir/gtest_wb.cpp.o"):
    return (
        "[16276/17328] Building CXX object src/CMakeFiles/x.dir/gtest_other.cpp.o\n"
        + _failed_compile_edge(edge_output, f"{_BEFORE}/{_TEST_FILE}")
        + _TEMPLATE_INSTANTIATION_DIAGNOSTIC
        + "ninja: build stopped: subcommand failed.\n"
    )


def test_attribution_covers_error_raised_inside_an_included_header(tmp_path):
    """The gap this fixes: the only `error:` line is in libcxx, but the sole failed
    translation unit is the overlaid test, so the failure is attributable to it."""
    result = _compile_log(tmp_path, _template_instantiation_log())

    # Path-of-error attribution alone cannot see it: the test file is on a `note:`.
    overlaid, other = attribute_compile_errors(result, [_TEST_FILE])
    assert overlaid == []
    assert other == ["contrib/llvm-project/libcxx/include/__memory/unique_ptr.h"]

    reason, _, _ = compile_failure_attribution(result, [_TEST_FILE])
    assert reason
    assert _TEST_FILE in reason


def test_attribution_keeps_error_in_overlaid_test_wording(tmp_path):
    """The pre-existing basis (every `error:` path is an overlaid test) is unchanged
    and still reported as such, not as the new translation-unit wording."""
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + f"{_BEFORE}/{_TEST_FILE}:56:21: error: too many arguments to function call\n"
    )
    reason, other, _ = compile_failure_attribution(
        _compile_log(tmp_path, log), [_TEST_FILE]
    )
    assert "every compile error is inside the PR's changed test files" in reason
    assert other == []


def test_attribution_fails_close_when_a_fix_source_also_fails(tmp_path):
    """A broken fix source is its own translation unit and fails its own edge, so the
    failure is not attributable to the overlaid tests."""
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + _TEMPLATE_INSTANTIATION_DIAGNOSTIC
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/WriteBufferInlineOrBlob.cpp.o",
            f"{_BEFORE}/src/Disks/IO/WriteBufferInlineOrBlob.cpp",
        )
        + f"{_BEFORE}/src/Disks/IO/WriteBufferInlineOrBlob.cpp:12:9: error: "
        "use of undeclared identifier 'sync_metadata_callback'\n"
    )
    reason, _, _ = compile_failure_attribution(_compile_log(tmp_path, log), [_TEST_FILE])
    assert reason == ""


def test_attribution_fails_close_when_a_contrib_translation_unit_fails(tmp_path):
    """A contrib source failing on its own edge is a toolchain/contrib break, never an
    unavoidable test adaptation."""
    log = (
        _failed_compile_edge(
            "contrib/zstd/CMakeFiles/z.dir/zstd.c.o",
            f"{_BEFORE}/contrib/zstd/lib/zstd.c",
        )
        + f"{_BEFORE}/contrib/zstd/lib/zstd.c:99:1: error: expected identifier\n"
    )
    reason, _, _ = compile_failure_attribution(_compile_log(tmp_path, log), [_TEST_FILE])
    assert reason == ""


def _abridged_failed_edge(output):
    """A `FAILED:` line with no command line after it: the next line is whatever the
    caller appends. Abridged CI logs and logs cut short both take this shape."""
    return f"FAILED: {output} \n"


def _link_failure_edge():
    """A failed `unit_tests_dbms` link edge: no `-c <source>` on its command line."""
    return (
        "FAILED: src/unit_tests_dbms \n"
        ": && /usr/local/bin/clang++-22 -fsanitize=address src/CMakeFiles/x.dir/a.o "
        "-o src/unit_tests_dbms && :\n"
        "ld.lld: error: undefined symbol: DB::WriteBufferInlineOrBlob::finalizeImpl()\n"
    )


def test_attribution_fails_close_on_a_link_failure(tmp_path):
    """A failed edge with no `-c <source>` (the linker) is not attributable. Here the only
    `error:` path is the libcxx header, so this covers the translation-unit basis."""
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + _TEMPLATE_INSTANTIATION_DIAGNOSTIC
        + _link_failure_edge()
    )
    result = _compile_log(tmp_path, log)
    assert attribute_compile_errors(result, [_TEST_FILE]) == (
        [],
        ["contrib/llvm-project/libcxx/include/__memory/unique_ptr.h"],
    )
    sources, unattributable, unreadable, diagnostic_free = failed_compile_edge_sources(
        result
    )
    assert sources == [_TEST_FILE]
    assert unattributable == ["src/unit_tests_dbms"]
    assert unreadable == []
    assert diagnostic_free == []

    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert "src/unit_tests_dbms" in refusal


def test_attribution_fails_close_on_a_link_failure_with_an_overlaid_error(tmp_path):
    """The same link failure alongside an `error:` line inside the overlaid test itself.
    The error-path basis is satisfied on its own, so the unattributable link edge is what
    has to hold attribution back."""
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + f"{_BEFORE}/{_TEST_FILE}:56:21: error: too many arguments to function call\n"
        + _link_failure_edge()
    )
    result = _compile_log(tmp_path, log)
    # The error-path basis alone would say "attributable".
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([_TEST_FILE], [])
    assert failed_compile_edge_sources(result) == (
        [_TEST_FILE],
        ["src/unit_tests_dbms"],
        [],
        [],
    )

    reason, other, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    # `other_errors` names nothing here, so the refusal phrase is the only detail the
    # ERROR message can offer.
    assert other == []
    assert refusal == (
        "failed build steps that name no translation unit: src/unit_tests_dbms"
    )


def test_attribution_keeps_the_error_path_basis_without_any_failed_edge(tmp_path):
    """A compile log carrying diagnostics but no ninja `FAILED:` line at all (ninja was
    invoked with a different output mode, or the log was captured per-command). The
    error-path basis stands on its own there, so an empty edge list must not withhold it."""
    log = (
        f"{_BEFORE}/{_TEST_FILE}:56:21: error: too many arguments to function call\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([_TEST_FILE], [])
    assert failed_compile_edge_sources(result) == ([], [], [], [])

    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert "every compile error is inside the PR's changed test files" in reason
    assert refusal == ""


def test_attribution_fails_close_when_a_fix_source_edge_fails_without_a_diagnostic(
    tmp_path,
):
    """An `error:` inside the overlaid test satisfies the error-path basis, while a fix
    source fails its own compile edge with no parsable diagnostic of its own (a killed
    compiler). A translation unit outside the overlaid tests demonstrably failed, so
    attribution must be refused on the edges before either basis is consulted."""
    fix_source = "src/Disks/IO/WriteBufferInlineOrBlob.cpp"
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + f"{_BEFORE}/{_TEST_FILE}:56:21: error: too many arguments to function call\n"
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/WriteBufferInlineOrBlob.cpp.o",
            f"{_BEFORE}/{fix_source}",
        )
        + "clang++-22: error\n"  # no `path:line:` prefix, so not a diagnostic
        + "Killed\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    # Both bases would say "attributable" on their own: the only `error:` path is the
    # overlaid test, and the fix source carries no diagnostic to place outside it.
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([_TEST_FILE], [])
    assert failed_compile_edge_sources(result) == (
        [fix_source, _TEST_FILE],
        [],
        [],
        [fix_source],
    )

    reason, other, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert other == []
    assert refusal == (
        "translation units outside the PR's changed test files failed to compile: "
        + fix_source
    )
    # The overlaid test also failed its own edge, but it is not what defeated attribution.
    assert _TEST_FILE not in refusal


def test_attribution_fails_close_without_a_parsable_diagnostic(tmp_path):
    """A killed compiler produces a failed compile edge on the overlaid test but no
    parsable diagnostic, so nothing states the failure is a test adaptation."""
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + "clang++-22: error\n"  # no `path:line:` prefix, so not a diagnostic
        + "Killed\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([], [])
    # The edge itself is attributable; only the missing diagnostic holds it back.
    assert failed_compile_edge_sources(result) == (
        [_TEST_FILE],
        [],
        [],
        [_TEST_FILE],
    )

    reason, other, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert other == []
    # The per-edge wording names the silent translation unit. The whole-log wording is
    # reserved for a log with no readable edge to name.
    assert refusal == (
        "failed build steps with no compiler diagnostic of their own: " + _TEST_FILE
    )


def test_attribution_strips_ansi_colour_before_scanning_edges(tmp_path):
    """cmake/ninja colour the output when a TTY is detected; the escape stripping must
    apply to the edge scan too, not only to the error scan."""
    plain = _template_instantiation_log()
    coloured = "".join(f"\x1b[1m{line}\x1b[0m\n" for line in plain.splitlines())
    reason, _, _ = compile_failure_attribution(
        _compile_log(tmp_path, coloured), [_TEST_FILE]
    )
    assert reason
    assert _TEST_FILE in reason


def test_attribution_scans_every_log_and_deduplicates(tmp_path):
    """Edges spread across several logs are all scanned, and a repeated edge yields one
    deterministic, sorted entry."""
    other_test = "src/Common/tests/gtest_a.cpp"
    log_a = _template_instantiation_log()
    log_b = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_a.cpp.o", f"{_BEFORE}/{other_test}"
        )
        + f"{_BEFORE}/{other_test}:7:1: error: unknown type name 'Foo'\n"
    )
    result = _compile_log(tmp_path, log_a, log_b, log_a)
    assert failed_compile_edge_sources(result) == (
        [other_test, _TEST_FILE],
        [],
        [],
        [],
    )

    reason, _, _ = compile_failure_attribution(result, [_TEST_FILE, other_test])
    assert reason
    assert _TEST_FILE in reason and other_test in reason


def test_failed_compile_edge_sources_keeps_paths_outside_the_worktree(tmp_path):
    """A source that does not live under the before-worktree cannot be an overlaid test
    file, so it is kept verbatim and makes the failure unattributable."""
    log = (
        _failed_compile_edge(
            "CMakeFiles/x.dir/probe.c.o", "/usr/share/cmake/Modules/probe.c"
        )
        + "/usr/share/cmake/Modules/probe.c:1:1: error: bad probe\n"
    )
    result = _compile_log(tmp_path, log)
    assert failed_compile_edge_sources(result) == (
        ["/usr/share/cmake/Modules/probe.c"],
        [],
        [],
        [],
    )
    assert compile_failure_attribution(result, [_TEST_FILE])[0] == ""


def test_failed_compile_edge_sources_handles_a_truncated_log(tmp_path):
    """A log cut off right after a `FAILED:` line (the runner was killed) has no command
    to read, so the edge is unreadable rather than an IndexError. With no diagnostic
    anywhere the failure is still refused, on the missing diagnostic."""
    result = _compile_log(tmp_path, "FAILED: src/CMakeFiles/x.dir/gtest_wb.cpp.o \n")
    assert failed_compile_edge_sources(result) == (
        [],
        [],
        ["src/CMakeFiles/x.dir/gtest_wb.cpp.o"],
        [],
    )
    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert refusal == "the build produced no parsable compiler diagnostic"


def test_failed_compile_edge_sources_splits_three_ways(tmp_path):
    """The three buckets are distinguished on one log: a read link command, an edge whose
    command line is replaced by a diagnostic, and a read compile command."""
    log = (
        _link_failure_edge()
        + _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/gtest_a.cpp.o", f"{_BEFORE}/src/Common/tests/gtest_a.cpp"
        )
        + f"{_BEFORE}/src/Common/tests/gtest_a.cpp:7:1: error: unknown type name 'Foo'\n"
    )
    assert failed_compile_edge_sources(_compile_log(tmp_path, log)) == (
        ["src/Common/tests/gtest_a.cpp"],
        ["src/unit_tests_dbms"],
        ["src/CMakeFiles/x.dir/gtest_wb.cpp.o"],
        [],
    )


def test_failed_compile_edge_sources_reads_a_shell_wrapped_link_command(tmp_path):
    """The link command is wrapped in cmake's `: && ... && :` and the real compile command
    is a wrapper script, not a compiler. Both must be read as commands: recognising a
    command line must not become a way for a link edge to be filed as unreadable, which
    would stop it defeating attribution."""
    log = (
        _link_failure_edge()
        + "FAILED: src/CMakeFiles/x.dir/gtest_wb.cpp.o \n"
        f"{_BEFORE}/cmake/heavy_build_check_scripts/prlimit_sanitizers.sh "
        f"/usr/bin/sccache /usr/local/bin/clang++-22 -O2 "
        f"-o src/CMakeFiles/x.dir/gtest_wb.cpp.o -c {_BEFORE}/{_TEST_FILE}\n"
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
    )
    assert failed_compile_edge_sources(_compile_log(tmp_path, log)) == (
        [_TEST_FILE],
        ["src/unit_tests_dbms"],
        [],
        [],
    )


def test_failed_compile_edge_sources_scopes_each_edges_diagnostic(tmp_path):
    """Two overlaid-test compile edges where only the first carries an `error:`. The
    diagnostic belongs to the edge it follows, so the silent one is reported."""
    other_test = "src/Common/tests/gtest_second_overlaid.cpp"
    log = (
        _template_instantiation_log()
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/gtest_second.cpp.o", f"{_BEFORE}/{other_test}"
        )
        + "clang++-22: error\n"
        + "Killed\n"
    )
    result = _compile_log(tmp_path, log)
    assert failed_compile_edge_sources(result) == (
        [other_test, _TEST_FILE],
        [],
        [],
        [other_test],
    )

    reason, other, refusal = compile_failure_attribution(result, [_TEST_FILE, other_test])
    assert reason == ""
    assert refusal == (
        "failed build steps with no compiler diagnostic of their own: " + other_test
    )
    # The edge that did explain itself is not what defeated attribution.
    assert _TEST_FILE not in refusal


def test_failed_compile_edge_sources_does_not_borrow_a_later_edges_diagnostic(tmp_path):
    """The silent edge comes first and the explained one after it. Only a window that ends
    at the next `FAILED:` edge can tell them apart: one running to the end of the log would
    lend the second edge's error to the first."""
    other_test = "src/Common/tests/gtest_second_overlaid.cpp"
    log = (
        _failed_compile_edge(
            "src/CMakeFiles/y.dir/gtest_second.cpp.o", f"{_BEFORE}/{other_test}"
        )
        + "clang++-22: error\n"
        + "Killed\n"
        + _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
    )
    result = _compile_log(tmp_path, log)
    assert failed_compile_edge_sources(result) == (
        [other_test, _TEST_FILE],
        [],
        [],
        [other_test],
    )
    assert compile_failure_attribution(result, [_TEST_FILE, other_test])[0] == ""


def test_failed_compile_edge_sources_reads_a_cd_wrapped_custom_command(tmp_path):
    """cmake writes a generated-source edge as `cd <dir> && <tool>`. It is a command, so
    the edge is read and reported as naming no translation unit, which defeats attribution
    outright. Filing it as unreadable instead would only withhold the second basis and let
    a broken code generator pass as a test adaptation."""
    generated = "contrib/llvm-project/llvm/include/llvm/IR/Attributes.inc"
    log = (
        f"FAILED: {generated} \n"
        "cd /ClickHouse/ci/tmp/before_build/contrib/llvm-project-cmake && "
        "/ClickHouse/ci/tmp/before_build/bin/llvm-tblgen -gen-attrs -o Attributes.inc\n"
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
    )
    result = _compile_log(tmp_path, log)
    assert failed_compile_edge_sources(result) == ([], [generated], [], [])

    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert refusal == (
        "failed build steps that name no translation unit: " + generated
    )


@pytest.mark.parametrize(
    "first_line",
    [
        f"In file included from {_BEFORE}/{_TEST_FILE}:1:",
        "    auto buffer = std::make_unique<WriteBufferInlineOrBlob>(a, b);",
        "                  ~~~~~~~~~~~~~~~~^~~~~~~~~~~~~~~~~~~~~~~~",
        f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'",
    ],
    ids=["include-context", "source-excerpt", "caret", "bare-diagnostic"],
)
def test_attribution_keeps_the_error_path_basis_for_every_diagnostic_line_shape(
    tmp_path, first_line
):
    """An abridged `FAILED:` block says nothing about its edge, so it must not withdraw an
    attribution the diagnostics already establish - whichever line clang happens to print
    first. `In file included from` is the shape the real failure this job exists for
    starts with, and only a command line may be read as evidence about the edge."""
    log = (
        _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + first_line
        + "\n"
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    assert failed_compile_edge_sources(result)[2] == [
        "src/CMakeFiles/x.dir/gtest_wb.cpp.o"
    ]
    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert "every compile error is inside the PR's changed test files" in reason
    assert refusal == ""


def test_attribution_keeps_the_error_path_basis_when_a_command_line_is_missing(tmp_path):
    """The regression this fixes: an abridged `FAILED:` block says nothing about its edge,
    so it must not withdraw an attribution the diagnostics already establish."""
    log = (
        _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([_TEST_FILE], [])
    assert failed_compile_edge_sources(result) == (
        [],
        [],
        ["src/CMakeFiles/x.dir/gtest_wb.cpp.o"],
        [],
    )

    reason, other, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert "every compile error is inside the PR's changed test files" in reason
    assert other == []
    assert refusal == ""


def test_attribution_fails_close_when_a_missing_command_line_is_all_that_is_left(
    tmp_path,
):
    """An unreadable edge still defeats the translation-unit basis, which claims every
    failed translation unit is an overlaid test and so needs all of them named. Here the
    only `error:` path is the libcxx header, so that basis is the one being asked for."""
    libcxx = "contrib/llvm-project/libcxx/include/__memory/unique_ptr.h"
    log = (
        _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + f"{_BEFORE}/{libcxx}:770:30: error: no matching constructor for "
        "initialization of 'DB::WriteBufferInlineOrBlob'\n"
        + f"{_BEFORE}/{_TEST_FILE}:56:21: note: in instantiation of function template "
        "specialization 'std::make_unique<DB::WriteBufferInlineOrBlob, ...>' here\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    result = _compile_log(tmp_path, log)
    assert attribute_compile_errors(result, [_TEST_FILE]) == ([], [libcxx])

    reason, _, refusal = compile_failure_attribution(result, [_TEST_FILE])
    assert reason == ""
    assert refusal == (
        "failed build steps whose command line could not be read: "
        "src/CMakeFiles/x.dir/gtest_wb.cpp.o"
    )


def test_failed_compile_edge_sources_handles_no_files():
    assert failed_compile_edge_sources(_FakeResult(None)) == ([], [], [], [])
    assert failed_compile_edge_sources(_FakeResult([])) == ([], [], [], [])


# --------------------------------------------------------------------------
# main() step 4b: the transition the whole job is about. The helpers above are
# pure, so they stay green even if the call site stops consulting them or stops
# flipping the status. The tests below assert the Result main() really produced.
# --------------------------------------------------------------------------
def _drive_main_to_compile_step(monkeypatch, tmp_path, compile_log):
    """Stub everything before step 4b so main() reaches the compile attribution with a
    failed compile Result whose log is `compile_log`. Returns the captured
    `(results, info_lines)` that main() passed to finalize."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    class _Info:
        pr_labels = ["pr-bugfix"]
        sha = "prheadsha777"
        base_branch = "master"
        is_local_run = False

        def get_changed_files(self):
            return [_TEST_FILE]

    log = tmp_path / "compile.log"
    log.write_text(compile_log)

    def fresh_compile_result(build_type):
        # A new Result per arm, as the production compile does: main() relabels the arm it
        # escalates from, and one shared object would carry that relabelling into the next.
        return job.Result(
            name=f"Compile before-binary (ninja unit_tests_dbms, without the fix, {build_type})",
            status=job.Result.Status.FAIL,
            files=[str(log)],
        )

    captured = {}
    monkeypatch.setattr(job, "Info", _Info)
    monkeypatch.setattr(job, "get_changed_unit_test_files", lambda info: [_TEST_FILE])
    monkeypatch.setattr(job, "derive_test_suites", lambda files: ["WriteBufferSuite"])
    monkeypatch.setattr(job, "gitmodules_shape_violation", lambda: None)
    monkeypatch.setattr(job, "determine_merge_base", lambda info: "mergebase123")
    monkeypatch.setattr(
        job.Shell,
        "get_output",
        staticmethod(lambda cmd, **kw: "checkouthead999" if "rev-parse HEAD" in cmd else ""),
    )
    monkeypatch.setattr(job, "get_submodule_state_changes", lambda base, head: [])
    monkeypatch.setattr(job, "prepare_before_worktree", lambda *a, **kw: True)
    monkeypatch.setattr(job, "reset_before_build_dir", lambda: True)
    monkeypatch.setattr(
        job,
        "configure_before_binary",
        lambda info, build_type: job.Result(name="Configure", status=job.Result.Status.OK),
    )
    monkeypatch.setattr(job, "compile_before_binary", fresh_compile_result)
    # A reproduction must never be reached: no build type compiles the overlay, so step 4b
    # decides once the last one has been tried.
    monkeypatch.setattr(
        job, "run_gtests", lambda *a, **kw: pytest.fail("main() ran the gtests")
    )
    monkeypatch.setattr(
        job,
        "finalize",
        lambda results, info_lines: captured.update(
            results=results, info_lines=info_lines
        ),
    )

    job.main()
    assert captured, "main() returned without calling finalize"
    return captured["results"], captured["info_lines"]


def test_main_reports_xfail_for_an_error_inside_an_included_header(
    monkeypatch, tmp_path
):
    """The job's headline outcome: the real #111391 shape (the only `error:` line is in
    libcxx, the sole failed translation unit is the overlaid test) must come out of main()
    as an XFAIL with nothing to validate, not as an ERROR."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    results, info_lines = _drive_main_to_compile_step(
        monkeypatch, tmp_path, _template_instantiation_log()
    )

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.XFAIL
    assert job.Result.Label.XFAIL in compile_result.get_labels()
    assert "every translation unit that failed to compile" in compile_result.info
    assert _TEST_FILE in compile_result.info
    assert "Nothing to validate on the unit side" in info_lines


def test_main_reports_error_when_a_fix_source_also_fails(monkeypatch, tmp_path):
    """Negative control for the test above: with a second failed edge on a fix source the
    same code path must reach ERROR/inconclusive, so an XFAIL there is a real decision and
    not "everything becomes XFAIL"."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + _TEMPLATE_INSTANTIATION_DIAGNOSTIC
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/WriteBufferInlineOrBlob.cpp.o",
            f"{_BEFORE}/src/Disks/IO/WriteBufferInlineOrBlob.cpp",
        )
        + f"{_BEFORE}/src/Disks/IO/WriteBufferInlineOrBlob.cpp:12:9: error: "
        "use of undeclared identifier 'sync_metadata_callback'\n"
    )
    results, info_lines = _drive_main_to_compile_step(monkeypatch, tmp_path, log)

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert job.Result.Label.XFAIL not in compile_result.get_labels()
    assert "cannot be attributed" in compile_result.info
    assert "src/Disks/IO/WriteBufferInlineOrBlob.cpp" in compile_result.info
    assert (
        "translation units outside the PR's changed test files failed to compile"
        in compile_result.info
    )
    assert "inconclusive" in info_lines


def test_main_error_message_names_an_unattributable_link_edge(monkeypatch, tmp_path):
    """The refusal phrase reaches the reported info. Here `other_errors` is empty (the only
    `error:` line is inside the overlaid test), so without the phrase the operator is told
    the errors cannot be attributed without being told what failed."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    log = (
        _failed_compile_edge(
            "src/CMakeFiles/x.dir/gtest_wb.cpp.o", f"{_BEFORE}/{_TEST_FILE}"
        )
        + f"{_BEFORE}/{_TEST_FILE}:56:21: error: too many arguments to function call\n"
        + _link_failure_edge()
    )
    results, info_lines = _drive_main_to_compile_step(monkeypatch, tmp_path, log)

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert job.Result.Label.XFAIL not in compile_result.get_labels()
    assert "errors outside them" not in compile_result.info
    assert "src/unit_tests_dbms" in compile_result.info
    assert "inconclusive" in info_lines


def test_main_reports_xfail_when_a_failed_edge_carries_no_command_line(
    monkeypatch, tmp_path
):
    """An abridged `FAILED:` block plus an `error:` inside the overlaid test: the
    diagnostics attribute the failure on their own, and an edge the log does not describe
    must not turn that into an ERROR. The block leads with the `In file included from`
    line real clang prints first, which is the shape that regressed."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    log = (
        _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + f"In file included from {_BEFORE}/{_TEST_FILE}:1:\n"
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    results, info_lines = _drive_main_to_compile_step(monkeypatch, tmp_path, log)

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.XFAIL
    assert job.Result.Label.XFAIL in compile_result.get_labels()
    assert "every compile error is inside the PR's changed test files" in compile_result.info
    assert _TEST_FILE in compile_result.info
    assert "Nothing to validate on the unit side" in info_lines


def test_main_reports_error_when_an_abridged_log_also_breaks_a_fix_source(
    monkeypatch, tmp_path
):
    """Negative control for the test above: the same abridged shape with a fix-source
    `error:` alongside must still reach ERROR, so a missing command line does not become a
    way to pass anything."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    fix_source = "src/Disks/IO/WriteBufferInlineOrBlob.cpp"
    log = (
        _abridged_failed_edge("src/CMakeFiles/x.dir/gtest_wb.cpp.o")
        + f"{_BEFORE}/{_TEST_FILE}:42:5: error: no matching function for call to 'f'\n"
        + f"{_BEFORE}/{fix_source}:12:9: error: use of undeclared identifier 'cb'\n"
        + "ninja: build stopped: subcommand failed.\n"
    )
    results, info_lines = _drive_main_to_compile_step(monkeypatch, tmp_path, log)

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert job.Result.Label.XFAIL not in compile_result.get_labels()
    assert "cannot be attributed" in compile_result.info
    assert fix_source in compile_result.info
    assert "inconclusive" in info_lines


def test_main_reports_error_when_a_second_overlaid_edge_has_no_diagnostic(
    monkeypatch, tmp_path
):
    """Two overlaid tests each fail their own edge and only the first says why. One edge's
    diagnostic cannot answer for another edge's silence, so the killed translation unit is
    named and the failure stays inconclusive."""
    import ci.jobs.unit_tests_bugfix_validation_job as job

    other_test = "src/Common/tests/gtest_second_overlaid.cpp"
    log = (
        _template_instantiation_log()
        + _failed_compile_edge(
            "src/CMakeFiles/y.dir/gtest_second.cpp.o", f"{_BEFORE}/{other_test}"
        )
        + "clang++-22: error\n"
        + "Killed\n"
    )
    results, info_lines = _drive_main_to_compile_step(monkeypatch, tmp_path, log)

    compile_result = results[-1]
    assert compile_result.status == job.Result.Status.ERROR
    assert job.Result.Label.XFAIL not in compile_result.get_labels()
    assert "cannot be attributed" in compile_result.info
    assert other_test in compile_result.info
    assert "inconclusive" in info_lines




if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
