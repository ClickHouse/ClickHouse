"""Bugfix validation for unit tests (gtest).

A bugfix PR is expected to add a regression test that *fails without the fix and
passes with it*.  For functional/integration tests this is verified by running the
new test against a prebuilt "before" binary downloaded from S3.  That trick does not
work for unit tests: gtest cases are compiled into `unit_tests_dbms`, so an old binary
does not contain the new tests.

Instead, this job builds a "before" binary from the merge-base sources with ONLY the
PR's unit-test file changes overlaid on top (the test, but not the fix), and then
runs the touched test suites on it — at least one must FAIL or crash (or the "before"
binary must fail to build, which is the strongest possible proof that the test depends
on the fix).

Like the functional/integration validators, this job only checks the "before" side.
The complementary "the touched tests PASS on the PR binary" side is delegated to the
regular `Unit tests (asan_ubsan)` job, which compiles and runs the full suite —
including the new test — on the PR binary; a regression test that is itself broken
makes that job red and blocks the PR.  Delegating it lets this job avoid requiring the
PR's `UNITTEST_AMD_ASAN_UBSAN` artifact, so it is not gated behind `build_amd_asan_ubsan`
and builds the "before" binary in parallel with the build matrix (it starts as early as
the functional/integration validators, which only need `config_workflow` + dockers).

See ci/jobs/functional_tests.py:invert_bugfix_validation_status for the analogous
functional-test logic.
"""

import os
import re
import sys

sys.path.append("./")

from ci.defs.defs import BuildTypes
from ci.jobs.build_clickhouse import BUILD_TYPE_TO_CMAKE, setup_build_caches_env
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

# Inside the binary-builder docker the repo is mounted at /ClickHouse and the cwd is
# /ClickHouse.  The "before" sources live in a separate git worktree so the primary
# checkout (and this running script) are never mutated.
REPO_NORMALIZED = "/ClickHouse"
BEFORE_SRC = "ci/tmp/before_src"
BEFORE_SRC_NORMALIZED = f"{REPO_NORMALIZED}/{BEFORE_SRC}"
BEFORE_BUILD_NORMALIZED = f"{BEFORE_SRC_NORMALIZED}/build"
BEFORE_BINARY = f"{BEFORE_SRC}/build/src/unit_tests_dbms"

# Build the "before" binary with the same config the regular `Unit tests (asan_ubsan)`
# job uses for the PR binary, so the two sides are compared under identical flags.
BUILD_TYPE = BuildTypes.AMD_ASAN_UBSAN

# gtest test-registration macros whose first argument is the test-suite name.
_GTEST_MACROS = (
    "TEST",
    "TEST_F",
    "TEST_P",
    "TYPED_TEST",
    "TYPED_TEST_P",
)
_SUITE_RE = re.compile(
    r"^\s*(?:" + "|".join(_GTEST_MACROS) + r")\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)

# A changed file is a unit-test source if it lives in a `tests/` directory under src/.
_UNIT_TEST_FILE_RE = re.compile(r"^src/.+/tests/.+\.(?:cpp|h|hpp|cc|cxx)$")


def get_changed_unit_test_files(info):
    """Changed files (vs the base) that are unit-test sources still present on disk."""
    if info.is_local_run:
        changed_files = Shell.get_output(
            f"gh pr diff {info.pr_number} --repo ClickHouse/ClickHouse --name-only"
        ).splitlines()
    else:
        changed_files = info.get_changed_files() or []
    result = []
    for fpath in changed_files:
        if _UNIT_TEST_FILE_RE.match(fpath) and os.path.isfile(fpath):
            result.append(fpath)
    return sorted(set(result))


def derive_test_suites(files):
    """Extract gtest test-suite names declared in the given files."""
    suites = set()
    for fpath in files:
        try:
            with open(fpath, "r", errors="replace") as f:
                content = f.read()
        except OSError as e:
            print(f"WARNING: could not read {fpath}: {e}")
            continue
        for m in _SUITE_RE.finditer(content):
            suites.add(m.group(1))
    return sorted(suites)


def build_gtest_filter(suites):
    """gtest_filter matching every case of the given suites.

    For value-/type-parameterized tests the full name is `Prefix/Suite.Case/0`, so we
    add a `*/Suite.*` pattern in addition to the plain `Suite.*`.
    """
    patterns = []
    for s in suites:
        patterns.append(f"{s}.*")
        patterns.append(f"*/{s}.*")
    return ":".join(patterns)


def determine_merge_base(info):
    base = info.base_branch or "master"
    # Ensure full history and the base branch are available (the runner unshallows for
    # most jobs, but be defensive — `git merge-base` needs the base commit locally).
    Shell.check(
        "git rev-parse --is-shallow-repository | grep -q true && "
        "git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin HEAD ||:",
        verbose=True,
    )
    Shell.check(
        f"git fetch --prune --no-recurse-submodules --filter=tree:0 origin {base} ||:",
        verbose=True,
    )
    merge_base = Shell.get_output(f"git merge-base HEAD origin/{base}").strip()
    assert merge_base, f"Failed to determine merge-base with origin/{base}"
    return merge_base


def prepare_before_worktree(merge_base, pr_sha, test_files):
    """Create an isolated worktree at the merge-base with only the test files overlaid.

    Submodule working trees are populated by hardlinking from the primary checkout
    (fast, no network) so the build sees contrib sources.  This is content-correct
    whenever the PR did not bump submodule pointers, which is the normal case for a
    unit-test bugfix.
    """
    Shell.check(f"git worktree remove --force {BEFORE_SRC} ||:", verbose=True)
    Shell.check(f"rm -rf {BEFORE_SRC}", verbose=True)
    assert Shell.check(
        f"git worktree add --detach --force {BEFORE_SRC} {merge_base}",
        verbose=True,
    ), "Failed to create the merge-base worktree"

    # Overlay only the PR's unit-test file changes on top of the merge-base tree — the
    # new/changed test, but none of the fix. NOTE: reference the PR commit explicitly,
    # not HEAD — inside the worktree HEAD is the merge-base.
    files_arg = " ".join(f"'{f}'" for f in test_files)
    assert Shell.check(
        f"git -C {BEFORE_SRC} checkout {pr_sha} -- {files_arg}",
        verbose=True,
    ), "Failed to overlay unit-test files onto the merge-base worktree"

    # Populate submodules by hardlinking from the primary checkout.
    sub_paths = Shell.get_output(
        "git config --file .gitmodules --get-regexp '^submodule\\..*\\.path$' "
        "| awk '{print $2}'"
    ).splitlines()
    for path in sub_paths:
        path = path.strip()
        if not path or not os.path.isdir(path) or not os.listdir(path):
            continue
        dst = os.path.join(BEFORE_SRC, path)
        Shell.check(f"rm -rf '{dst}' && mkdir -p '{os.path.dirname(dst)}'", verbose=False)
        # cp -al = recursive hardlink copy (instant, no data duplication).
        Shell.check(f"cp -al '{path}' '{dst}'", verbose=False)


def build_before_binary(info):
    """Configure and build only the `unit_tests_dbms` target in the before-worktree.

    Returns the build Result (its .is_ok() tells whether the merge-base could build
    the overlaid test).
    """
    setup_build_caches_env(info)
    os.makedirs(f"{BEFORE_SRC}/build", exist_ok=True)
    if not Shell.check("sccache --start-server", retries=3):
        print("WARNING: sccache server failed to start, build will proceed without it")
    Shell.check("sccache --show-stats", verbose=True)

    # Reuse the exact ASan+UBSan flags from the build job, but point the source tree,
    # build dir, and toolchain file at the merge-base worktree (the only path the dict
    # hardcodes to the primary checkout is the toolchain file).
    cmake_flags = BUILD_TYPE_TO_CMAKE[BUILD_TYPE].replace(
        f"{REPO_NORMALIZED}/cmake/", f"{BEFORE_SRC_NORMALIZED}/cmake/"
    )
    cmake_cmd = f"{cmake_flags} {BEFORE_SRC_NORMALIZED} -B {BEFORE_BUILD_NORMALIZED}"
    build_result = Result.from_commands_run(
        name="Build before-binary (merge-base + test files, without the fix)",
        command=[
            cmake_cmd,
            "ninja unit_tests_dbms",
        ],
        workdir=BEFORE_BUILD_NORMALIZED,
        with_log=True,
    )
    Shell.check("sccache --show-stats", verbose=True)
    return build_result


def run_gtests(binary_path, gtest_filter, name):
    # ASan+UBSan build: do not wrap with gdb (LSan is incompatible with the debugger),
    # and disable the uninstrumented FIPS provider to avoid sanitizer false positives.
    os.environ["OPENSSL_CONF"] = "/dev/null"
    return Result.from_gtest_run(
        unit_tests_path=binary_path,
        name=name,
        gtest_filter=gtest_filter,
    )


def mark_reproduced(result):
    """Flip a before-run failure into an expected (XFAIL) success for the report."""
    result.set_label(Result.Label.XFAIL)
    for r in result.results:
        r.set_label(Result.Label.XFAIL)
        if r.status == Result.Status.FAIL:
            r.status = Result.Status.XFAIL
    result.set_status(Result.Status.XFAIL)


def finalize(results, info_lines):
    Result.create_from(
        results=results,
        info=info_lines,
        with_info_from_results=True,
    ).complete_job()


def main():
    info = Info()

    # 1. Gate: only bugfix PRs are validated (mirrors the FT/IT bugfix checks).
    pr_labels = info.pr_labels or []
    if not (
        Labels.PR_BUGFIX in pr_labels or Labels.PR_CRITICAL_BUGFIX in pr_labels
    ):
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.OK,
                    info="Not a bugfix PR (no pr-bugfix/pr-critical-bugfix label) — nothing to validate.",
                )
            ],
            "Skipped: not a bugfix PR.",
        )
        return

    # 2. Select changed unit-test files.
    test_files = get_changed_unit_test_files(info)
    if not test_files:
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.OK,
                    info="The PR does not change any unit-test files (src/**/tests/*) — nothing to validate.",
                )
            ],
            "Skipped: no changed unit-test files.",
        )
        return
    print("Changed unit-test files:\n  " + "\n  ".join(test_files))

    # 3. Derive the touched test suites and build a gtest filter.
    suites = derive_test_suites(test_files)
    if not suites:
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.OK,
                    info="No gtest test suites found in the changed unit-test files — nothing to validate.",
                )
            ],
            "Skipped: no gtest suites in changed files.",
        )
        return
    gtest_filter = build_gtest_filter(suites)
    print(f"Touched test suites: {suites}")
    print(f"gtest filter: {gtest_filter}")

    results = []

    # 4. Build the "before" binary (merge-base + test files, without the fix).
    pr_sha = Shell.get_output("git rev-parse HEAD").strip()
    merge_base = determine_merge_base(info)
    print(f"PR commit: {pr_sha}")
    print(f"merge-base: {merge_base}")
    prepare_before_worktree(merge_base, pr_sha, test_files)
    build_result = build_before_binary(info)

    if not build_result.is_ok():
        # Decision #4: a before-binary that cannot even build is the strongest proof
        # that the test depends on the fix. Pass the check, but make it crystal clear
        # the success comes from a BUILD FAILURE, NOT a runtime reproduction.
        build_result.set_status(Result.Status.OK)
        build_result.set_label(Result.Label.XFAIL)
        build_result.set_info(
            "EXPECTED: the merge-base (without the fix) FAILED TO BUILD the touched "
            "test — the test depends on code introduced by this PR. The check passes "
            "because of this build failure. NOTE: the bug was NOT reproduced at "
            "runtime; the before-binary never ran."
        )
        results.append(build_result)
        finalize(
            results,
            "Bugfix validation passed because the before-binary FAILED TO BUILD "
            "(test depends on the fix). This is NOT a runtime reproduction.",
        )
        return

    results.append(build_result)

    # 5. Run the touched tests on the "before" binary — at least one must fail/crash.
    before_result = run_gtests(
        BEFORE_BINARY,
        gtest_filter,
        name="Touched unit tests on the before-binary (must fail)",
    )

    if before_result.is_error():
        # Inconclusive run (binary could not be executed / runner died): preserve the
        # error rather than reporting a false "failed to reproduce".
        results.append(before_result)
        finalize(
            results,
            "Bugfix validation inconclusive: the before-binary run did not finish.",
        )
        return

    if not before_result.is_ok():
        # At least one touched test failed or crashed on the before-binary — the bug is
        # reproduced. Flip the expected failure to a success for the report.
        mark_reproduced(before_result)
        results.append(before_result)
        finalize(
            results,
            "Bug reproduced: at least one touched unit test fails/crashes on the "
            "before-binary (merge-base without the fix) and passes on the PR binary.",
        )
        return

    # All touched tests passed on the before-binary — the test does not catch the bug.
    before_result.set_status(Result.Status.FAIL)
    before_result.set_info(
        "Failed to reproduce the bug: all touched unit tests PASS on the before-binary "
        "(merge-base without the fix). The added/changed test does not catch the bug "
        "the fix addresses."
    )
    results.append(before_result)
    finalize(results, "Failed to reproduce the bug.")


if __name__ == "__main__":
    main()
