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
import shlex
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
    """gtest_filter matching every case of the given suites, across all naming forms.

    gtest encodes the suite differently per test kind, and a pattern that fits one kind
    misses the others, so we emit all four:
      * `Suite.Case`            plain / fixture (TEST, TEST_F)          -> `Suite.*`
      * `Prefix/Suite.Case/0`   value-parameterized (TEST_P)           -> `*/Suite.*`
      * `Suite/0.Case`          typed (TYPED_TEST)                     -> `Suite/*`
      * `Prefix/Suite/0.Case`   type-parameterized (TYPED_TEST_P)      -> `*/Suite/*`
    Without the `Suite/*` / `*/Suite/*` patterns a suite that only has typed tests would
    select zero cases on the before-binary and be misreported as "failed to reproduce".
    """
    patterns = []
    for s in suites:
        patterns.append(f"{s}.*")
        patterns.append(f"*/{s}.*")
        patterns.append(f"{s}/*")
        patterns.append(f"*/{s}/*")
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


# cmake's submodule sanity check (CMakeLists.txt) looks for this file; we use the same
# marker to verify the before-worktree actually has its submodules populated.
SUBMODULE_MARKER = "contrib/sysroot/README.md"


def gitmodules_shape_violation():
    """Return an error string if the PR's `.gitmodules` has an unsafe submodule entry
    (a URL that is not a plain `https://github.com/...`, or a name that differs from its
    path); return None if it is clean.

    SECURITY: this job populates submodules over the network from the PR-controlled
    `.gitmodules`, inside the privileged binary-builder container, and — because it now
    starts early — BEFORE the regular `check_submodules.sh` (run in build_arm_tidy) would
    reject bad metadata. Validating the URL/path shape here, before any `git submodule`
    network access, stops a PR from pointing a submodule at an arbitrary URL and having
    the self-hosted runner fetch it. Mirrors the URL/name rules of
    ci/jobs/scripts/check_style/check_submodules.sh.
    """
    for line in Shell.get_output(
        "git config --file .gitmodules --get-regexp 'submodule\\..+\\.url'"
    ).splitlines():
        line = line.strip()
        if not line:
            continue
        key, _, url = line.partition(" ")
        if not url.startswith("https://github.com/"):
            name = key.removeprefix("submodule.").removesuffix(".url")
            return f"submodule '{name}' has a non-github URL '{url}'"
    for line in Shell.get_output(
        "git config --file .gitmodules --get-regexp 'submodule\\..+\\.path'"
    ).splitlines():
        line = line.strip()
        if not line:
            continue
        key, _, path = line.partition(" ")
        name = key.removeprefix("submodule.").removesuffix(".path")
        if name != path:
            return f"submodule name '{name}' is not equal to its path '{path}'"
    return None


def ensure_primary_submodules():
    """Populate the primary checkout's submodule working trees.

    `needs_submodules=True` only restores the submodule *git data* (`.git/modules`) from
    the S3 cache — the working-tree files still have to be checked out. Without this the
    primary `contrib/*` directories are empty and there is nothing to hardlink into the
    before-worktree. Mirrors the CHECKOUT_SUBMODULES stage of
    ci/jobs/build_clickhouse.py (cache fast-path + full-fetch fallback).
    """
    assert Shell.check(
        "git submodule sync && git submodule init", verbose=True
    ), "Failed to init submodules in the primary checkout"
    if os.path.isdir(".git/modules/contrib") and os.listdir(".git/modules/contrib"):
        print("Submodule cache detected — populating working trees from cache")
        ok = Shell.check(
            "git submodule update --depth 1 --single-branch", retries=3, verbose=True
        )
    else:
        ok = Shell.check(
            "contrib/update-submodules.sh --max-procs 10", retries=3, verbose=True
        )
    assert ok, "Failed to populate submodule working trees in the primary checkout"


def prepare_before_worktree(merge_base, pr_sha, test_files):
    """Create an isolated worktree at the merge-base with only the test files overlaid.

    Submodule working trees are populated by hardlinking from the primary checkout
    (fast, no network) so the build sees contrib sources.  This is content-correct
    whenever the PR did not bump submodule pointers, which is the normal case for a
    unit-test bugfix.  Returns True iff the worktree ended up with its submodules
    populated (checked via SUBMODULE_MARKER) — the caller must fail close otherwise.
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
    # SECURITY: test_files are PR-controlled paths (the regex permits quotes/spaces), so
    # they must be shell-quoted before reaching Shell.check (shell=True) to avoid command
    # injection on the runner. The `--` already terminates option parsing for git.
    files_arg = " ".join(shlex.quote(f) for f in test_files)
    assert Shell.check(
        f"git -C {shlex.quote(BEFORE_SRC)} checkout {shlex.quote(pr_sha)} -- {files_arg}",
        verbose=True,
    ), "Failed to overlay unit-test files onto the merge-base worktree"

    # The primary checkout must have populated submodule working trees before we can
    # hardlink them across (see ensure_primary_submodules).
    ensure_primary_submodules()

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
        # SECURITY: submodule paths come from the PR's `.gitmodules` and are PR-controlled,
        # so shell-quote them (and use `--`) before Shell.check to avoid command/option
        # injection on the runner.
        q_path, q_dst, q_dst_parent = (
            shlex.quote(path),
            shlex.quote(dst),
            shlex.quote(os.path.dirname(dst)),
        )
        Shell.check(f"rm -rf -- {q_dst} && mkdir -p -- {q_dst_parent}", verbose=False)
        # cp -al = recursive hardlink copy (instant, no data duplication).
        Shell.check(f"cp -al -- {q_path} {q_dst}", verbose=False)

    return os.path.isfile(os.path.join(BEFORE_SRC, SUBMODULE_MARKER))


def configure_before_binary(info):
    """Run cmake configure for the before-worktree. Returns the cmake Result.

    Kept separate from the compile step on purpose: a configure failure is an
    environment/infrastructure problem (toolchain, submodules, cache) and must NOT be
    mistaken for "the test depends on the fix". Only a *compile* failure carries that
    meaning. See main().
    """
    setup_build_caches_env(info)
    os.makedirs(f"{BEFORE_SRC}/build", exist_ok=True)
    if not Shell.check("sccache --start-server", retries=3):
        print("WARNING: sccache server failed to start, build will proceed without it")
    Shell.check("sccache --show-stats", verbose=True)

    # NOTE: this is a cold build (~1h, ~0% sccache hits). sccache keys bake in the
    # absolute build path (via `-ffile-prefix-map` and preprocessed `# line` markers),
    # and master's cache was populated by builds at `/ClickHouse`, so building the
    # worktree at `ci/tmp/before_src` misses all of it. This is accepted: the job is off
    # the critical path (it has no artifact dependency and runs in parallel with the
    # build matrix). Bind-mounting the worktree onto `/ClickHouse` to recover hits does
    # NOT work — sccache's server compiles in its own mount namespace, not the client's.
    #
    # Reuse the exact ASan+UBSan flags from the build job, but point the source tree,
    # build dir, and toolchain file at the merge-base worktree (the only path the dict
    # hardcodes to the primary checkout is the toolchain file).
    cmake_flags = BUILD_TYPE_TO_CMAKE[BUILD_TYPE].replace(
        f"{REPO_NORMALIZED}/cmake/", f"{BEFORE_SRC_NORMALIZED}/cmake/"
    )
    cmake_cmd = f"{cmake_flags} {BEFORE_SRC_NORMALIZED} -B {BEFORE_BUILD_NORMALIZED}"
    return Result.from_commands_run(
        name="Configure before-binary (cmake)",
        command=[cmake_cmd],
        workdir=BEFORE_BUILD_NORMALIZED,
        with_log=True,
    )


def compile_before_binary():
    """Compile only the `unit_tests_dbms` target in the configured before-worktree.

    Returns the ninja Result. A failure here means the overlaid test does not compile
    against the merge-base sources — strong evidence it depends on code the PR adds.
    """
    compile_result = Result.from_commands_run(
        name="Compile before-binary (ninja unit_tests_dbms, without the fix)",
        command=["ninja unit_tests_dbms"],
        workdir=BEFORE_BUILD_NORMALIZED,
        with_log=True,
    )
    Shell.check("sccache --show-stats", verbose=True)
    return compile_result


def run_gtests(binary_path, gtest_filter, name):
    # ASan+UBSan build: do not wrap with gdb (LSan is incompatible with the debugger),
    # and disable the uninstrumented FIPS provider to avoid sanitizer false positives.
    os.environ["OPENSSL_CONF"] = "/dev/null"
    return Result.from_gtest_run(
        unit_tests_path=binary_path,
        name=name,
        gtest_filter=gtest_filter,
    )


def before_run_started_a_test(result):
    """Did the before-binary actually start executing a touched test?

    gtest prints "[ RUN      ] Suite.Test" when a test begins. If that marker is present,
    a failure/crash is attributable to the touched suite — a real reproduction (including
    a crash *during* the test, a legitimate crash-bug repro). If it is absent, the binary
    died before any test ran (e.g. a runtime that cannot initialize in this environment),
    which is an infrastructure problem and must NOT be counted as a reproduction.
    """
    for f in result.files or []:
        try:
            if "[ RUN " in Shell.get_output(f"cat {f}", verbose=False):
                return True
        except OSError as e:
            print(f"WARNING: could not read gtest log {f}: {e}")
    return False


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

    # SECURITY: refuse to touch the network if the PR's .gitmodules is unsafe. This job
    # fetches submodules from PR-controlled metadata inside a privileged runner and runs
    # before check_submodules.sh (build_arm_tidy) — validate the URL/path shape first so
    # a PR cannot make the runner fetch an arbitrary submodule URL. Inconclusive (ERROR),
    # not a reproduction; the bad .gitmodules is independently blocked by build_arm_tidy.
    gitmodules_error = gitmodules_shape_violation()
    if gitmodules_error:
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.ERROR,
                    info=(
                        "Refusing to populate submodules before validation: "
                        f"{gitmodules_error}. The before-binary cannot be built; this is "
                        "an infrastructure/safety stop, NOT a reproduction."
                    ),
                )
            ],
            "Bugfix validation inconclusive: refused unsafe .gitmodules before any "
            f"submodule fetch ({gitmodules_error}).",
        )
        return

    submodules_ok = prepare_before_worktree(merge_base, pr_sha, test_files)

    # Fail close: building unit_tests_dbms needs submodules. If they are missing, cmake
    # aborts with a generic "Submodules are not initialized" error that must NOT be
    # mistaken for "the test depends on the fix". Surface it as an infrastructure error.
    if not submodules_ok:
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.ERROR,
                    info=(
                        f"Submodules were not populated in the before-worktree (missing "
                        f"'{SUBMODULE_MARKER}'); cannot build the before-binary. This is "
                        "an infrastructure error — NOT a reproduction."
                    ),
                )
            ],
            "Bugfix validation inconclusive: submodules missing in the before-worktree.",
        )
        return

    # 4a. Configure. A cmake-configure failure is an environment/infra problem, never
    # evidence that the test depends on the fix — report it as an error, do not pass.
    configure_result = configure_before_binary(info)
    results.append(configure_result)
    if not configure_result.is_ok():
        configure_result.set_status(Result.Status.ERROR)
        finalize(
            results,
            "Bugfix validation inconclusive: the before-binary failed to CONFIGURE "
            "(cmake). This is an infrastructure error, not a reproduction.",
        )
        return

    # 4b. Compile. A compile failure is NOT accepted as a reproduction: it only proves
    # the overlaid test references *some* code the PR adds (a new header, helper, or
    # symbol — even in a no-op test), not that it depends on the bug fix or reproduces
    # the old behavior at runtime. We cannot attribute the failure to the touched
    # regression case, so report it as inconclusive (fail close) rather than a pass. A
    # genuine regression test should build against the merge-base and fail at runtime.
    compile_result = compile_before_binary()
    if not compile_result.is_ok():
        compile_result.set_status(Result.Status.ERROR)
        compile_result.set_info(
            "The before-binary FAILED TO COMPILE the overlaid test. This does not prove "
            "the test reproduces the bug — it only shows the test depends on code this PR "
            "adds (a new header/helper/symbol would fail to compile here too). Write a "
            "regression test that builds against the merge-base and fails at runtime "
            "without the fix. " + (compile_result.info or "")
        )
        results.append(compile_result)
        finalize(
            results,
            "Bugfix validation inconclusive: the before-binary failed to COMPILE the "
            "overlaid test, which cannot be attributed to the touched regression case.",
        )
        return
    build_result = compile_result

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
        # A failure/crash only counts as a reproduction if the touched suite actually
        # started executing. If the binary died before any test ran (no "[ RUN ]" marker),
        # it is an environment/infrastructure problem — e.g. a runtime that cannot
        # initialize in this container — NOT evidence the test catches the bug. Fail close.
        if not before_run_started_a_test(before_result):
            before_result.set_status(Result.Status.ERROR)
            before_result.set_info(
                "The before-binary died before running any touched test (no gtest "
                "'[ RUN ]' marker). This is an infrastructure error — NOT a reproduction. "
                + (before_result.info or "")
            )
            results.append(before_result)
            finalize(
                results,
                "Bugfix validation inconclusive: the before-binary did not start any "
                "touched test (environment problem, not a reproduction).",
            )
            return

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
