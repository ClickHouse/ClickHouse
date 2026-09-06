"""Bugfix validation for unit tests (gtest).

A bugfix PR is expected to add a regression test that *fails without the fix and
passes with it*.  For functional/integration tests this is verified by running the
new test against a prebuilt "before" binary downloaded from S3.  That trick does not
work for unit tests: gtest cases are compiled into `unit_tests_dbms`, so an old binary
does not contain the new tests.

Instead, this job builds a "before" binary from the merge-base sources with ONLY the
PR's unit-test file changes overlaid on top (the test, but not the fix), and then
runs the touched test suites on it — at least one must FAIL or crash.

The "before" binary is built and judged once per build type in `BEFORE_BUILD_TYPES`, and
only a build type that both compiled the overlaid tests and ran them has a verdict. Three
outcomes produce none, so they escalate to the next build type instead of deciding:

  * the overlaid test files do not compile there, with every compiler error inside them or
    every failed translation unit being one of them (`compile_failure_attribution`): the
    changed test code depends on the interface the fix introduces (typically a call site
    adapted to a changed signature), which a sanitizer-conditional part of the test can do
    on one build type only;
  * a case of the changed test files did not run, having skipped itself (`GTEST_SKIP()`),
    being disabled, or being compiled out by a preprocessor guard and so absent from the
    report, which leaves the changed regression case possibly unexercised;
  * every touched case ran and passed, which cannot separate "the test does not catch the
    bug" from "this build cannot observe the failure mode".

The last build type decides: all arms passing is a refutation (FAIL), an overlay that
compiles on none of them is expected with nothing to validate (XFAIL) instead of staying
red forever, and anything else, an arm that never ran the tests while another refuted
them, is inconclusive (ERROR). An attributed compile failure on a build type that follows
one which did compile the same overlay is confined to what the two compile differently and
is inconclusive too. Any other build failure is an infrastructure or attribution problem
and stays an ERROR.

Like the functional/integration validators, this job only checks the "before" side.
The complementary "the touched tests PASS on the PR binary" side is delegated to the
regular `Unit tests (asan_ubsan)` job, which compiles and runs the full suite —
including the new test — on the PR binary; a regression test that is itself broken
makes that job red and blocks the PR.  That delegation is per-arm: the counterpart of the
arm that reproduced is the `Unit tests (<that arm's sanitizer>)` job, so a bug reproduced
on the `amd_tsan` arm is complemented by `Unit tests (tsan)`.  Delegating it also lets this
job avoid a direct dependency on the PR's `UNITTEST_AMD_ASAN_UBSAN` artifact.

See ci/jobs/functional_tests.py:invert_bugfix_validation_status for the analogous
functional-test logic.
"""

import fnmatch
import json
import os
import re
import shlex
import shutil
import sys

sys.path.append("./")

from ci.defs.defs import BuildTypes, ToolSet
from ci.jobs.build_clickhouse import BUILD_TYPE_TO_CMAKE, setup_build_caches_env
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result, ResultTranslator
from ci.praktika.utils import Shell

# Inside the binary-builder docker the repo is mounted at /ClickHouse and the cwd is
# /ClickHouse.  The "before" sources live in a separate git worktree so the primary
# checkout (and this running script) are never mutated.
REPO_NORMALIZED = "/ClickHouse"
BEFORE_SRC = "ci/tmp/before_src"
BEFORE_SRC_NORMALIZED = f"{REPO_NORMALIZED}/{BEFORE_SRC}"
BEFORE_BUILD_NORMALIZED = f"{BEFORE_SRC_NORMALIZED}/build"
BEFORE_BINARY = f"{BEFORE_SRC}/build/src/unit_tests_dbms"

# Build types the "before" binary is validated on, in order. A test whose failure mode
# needs a sanitizer the arm lacks passes there either way, so one arm cannot separate
# "the test does not catch the bug" from "this build cannot observe it". Every arm is a
# cold build, so the list stops at two and `amd_msan`/`amd_debug` remain blind spots.
BEFORE_BUILD_TYPES = (BuildTypes.AMD_ASAN_UBSAN, BuildTypes.AMD_TSAN)

# gtest test-registration macros whose first argument is the test-suite name.
# `TEST`/`TEST_F` are `#define`d to `GTEST_TEST`/`GTEST_TEST_F`, so both spellings of
# each register a suite and both must be listed here.
_GTEST_MACROS = (
    "GTEST_TEST",
    "GTEST_TEST_F",
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
_CASE_RE = re.compile(
    r"^\s*(?:"
    + "|".join(_GTEST_MACROS)
    + r")\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)

# A changed file is a unit-test source if it lives in a `tests/` directory under src/.
_UNIT_TEST_FILE_RE = re.compile(r"^src/.+/tests/.+\.(?:cpp|h|hpp|cc|cxx)$")
_TRANSLATION_UNIT_SUFFIXES = (".cpp", ".cc", ".cxx")


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


def can_reach_unit_tests_dbms(fpath):
    """Can the cases this changed unit-test file declares be in `unit_tests_dbms`?

    `grep_gtest_sources` (src/CMakeLists.txt) globs `gtest*.cpp`, so any other translation
    unit under `tests/` is in no build type's binary. A header has no translation unit of its
    own and reaches the binary through whichever `gtest*.cpp` includes it, which is the shape
    gtest documents for `TYPED_TEST_SUITE_P` (gtest-typed-test.h).
    """
    name = os.path.basename(fpath)
    if name.endswith(_TRANSLATION_UNIT_SUFFIXES):
        return fnmatch.fnmatch(name, "gtest*.cpp")
    return True


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
    # IMPORTANT: this workflow checks out GitHub's synthetic MERGE ref by default
    # (CHECKOUT_REF is empty unless DISABLE_CI_MERGE_COMMIT=1 in pull_request.yml), so
    # `git rev-parse HEAD` is the base+PR merge commit, NOT the PR head. Using HEAD here
    # would make `git merge-base` resolve to the current base tip and the overlay pick up
    # the merged test file — i.e. the "before" tree would be master-tip + merged test
    # rather than the true merge-base + PR-head test, causing false reproductions and
    # refutations whenever master touched the buggy code or the test after the branch
    # split. Anchor everything on the PR head commit (info.sha) instead.
    pr_head = info.sha
    assert pr_head, "Info.sha (PR head commit) is empty; cannot compute a correct merge-base"
    Shell.check(
        "git rev-parse --is-shallow-repository | grep -q true && "
        "git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin HEAD ||:",
        verbose=True,
    )
    Shell.check(
        f"git fetch --prune --no-recurse-submodules --filter=tree:0 origin {base} ||:",
        verbose=True,
    )
    # Fetch the PR head explicitly: under the merge-ref checkout it is only reachable as
    # the merge commit's second parent, and may be absent in a partial/shallow clone.
    Shell.check(
        "git fetch --prune --no-recurse-submodules --filter=tree:0 origin "
        f"{shlex.quote(pr_head)} ||:",
        verbose=True,
    )
    merge_base = Shell.get_output(
        f"git merge-base {shlex.quote(pr_head)} origin/{base}"
    ).strip()
    assert merge_base, f"Failed to determine merge-base of {pr_head} with origin/{base}"
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
        # SECURITY: the path is later joined onto BEFORE_SRC and passed to `rm -rf`/`cp -al`
        # (see prepare_before_worktree). An absolute path or a `..` component would let a
        # PR-controlled `.gitmodules` (e.g. path = ../../../../ClickHouse) resolve outside
        # before_src and operate on the mounted checkout itself. Require a plain relative
        # path under contrib/ (all real submodules live there) with no `..` component. This
        # is the pre-fetch half of the defense; prepare_before_worktree adds a realpath
        # containment check as the second half before the destructive `rm -rf`.
        if (
            os.path.isabs(path)
            or ".." in path.split("/")
            or not path.startswith("contrib/")
        ):
            return f"submodule '{name}' has an unsafe path '{path}'"
    return None


def get_submodule_state_changes(merge_base, checkout_sha):
    """Paths whose submodule state differs between the merge-base and the checkout.

    Returns the list of changed submodule gitlinks (mode 160000 entries in either tree)
    plus `.gitmodules` itself if it changed. The before-worktree is populated by
    hardlinking submodule working trees from the primary checkout, whose submodules are
    checked out at `checkout_sha`'s recorded revisions — in this workflow that is `HEAD`,
    normally GitHub's synthetic base+PR merge ref. That content is correct for the
    merge-base build only while no submodule state differs between the two commits. A
    difference can come from the PR itself (a gitlink or `.gitmodules` edit) or from the
    base branch moving a submodule after the branch split — comparing against the PR
    head instead of the checkout would miss the latter, and the "before" binary would
    silently build merge-base sources against base-tip submodule content. Either way
    the caller must fail close instead of validating against the wrong code.
    """
    # `--ignore-submodules=none` is essential: a `diff.ignoreSubmodules=all` config (set
    # in some environments) would otherwise silently drop every gitlink change from the
    # diff and this guard would never fire. `strict=True` is equally essential: a failed
    # diff (missing object in the partial/shallow checkout, transient git error) must
    # raise rather than yield an empty string — otherwise the guard would fail open and
    # the validator would proceed against potentially wrong submodule content.
    diff = Shell.get_output(
        "git diff --raw --ignore-submodules=none "
        f"{shlex.quote(merge_base)} {shlex.quote(checkout_sha)}",
        strict=True,
    )
    changed = []
    for line in diff.splitlines():
        # raw format: ':<old_mode> <new_mode> <old_sha> <new_sha> <status>\t<path>'
        if not line.startswith(":"):
            continue
        meta, _, path = line.partition("\t")
        parts = meta.lstrip(":").split()
        if len(parts) < 4 or not path:
            continue
        old_mode, new_mode = parts[0], parts[1]
        if old_mode == "160000" or new_mode == "160000" or path == ".gitmodules":
            changed.append(path)
    return sorted(set(changed))


def submodule_worktree_populated(path):
    """True iff `path` contains real working-tree content, not just the bookkeeping
    `.git` entry. After the working-tree files of a cached submodule are removed, git
    can leave the directory holding exactly `['.git']` (a plain `git submodule update`
    exits 0 in that state), so a bare `os.listdir` non-empty check would accept a
    sourceless tree and the hardlink copy would propagate it into the before-worktree,
    reintroducing the misleading cmake "cannot find source file" failure.
    """
    return any(entry != ".git" for entry in os.listdir(path))


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
        # `--force` is essential here (unlike build_clickhouse.py's checkout): the S3 cache
        # restores each submodule's `.git/modules` data with `HEAD` already at the recorded
        # gitlink, but the *working tree* is empty. Without `--force`, `git submodule update`
        # sees `HEAD` == the recorded commit, considers the submodule up-to-date, and leaves
        # the working tree empty. The hardlink step then finds nothing to copy and the
        # before-binary fails to configure with a misleading "cannot find source file"
        # error. `--force` runs `git checkout --force` unconditionally, repopulating the
        # empty working tree from objects already present in the cache.
        ok = Shell.check(
            "git submodule update --force --depth 1 --single-branch",
            retries=3,
            verbose=True,
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
    whenever the checkout's submodule state equals the merge-base's, which is the
    normal case for a unit-test bugfix — main() fails close (via
    get_submodule_state_changes, merge-base vs the checkout `HEAD`) before calling
    this when any gitlink or `.gitmodules` differs.  Returns True iff
    every submodule the primary checkout has was hardlinked into the worktree
    non-empty — the caller must fail close otherwise.
    """
    # Populate the primary checkout's submodule working trees FIRST, before touching the
    # worktree: the hardlink step below has nothing to copy otherwise, and doing it up
    # front keeps the worktree machinery from racing with submodule checkout.
    ensure_primary_submodules()

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

    # Populate submodules by hardlinking from the primary checkout.
    sub_paths = Shell.get_output(
        "git config --file .gitmodules --get-regexp '^submodule\\..*\\.path$' "
        "| awk '{print $2}'"
    ).splitlines()
    before_src_real = os.path.realpath(BEFORE_SRC)
    failures = []
    for path in sub_paths:
        path = path.strip()
        if not path or not os.path.isdir(path):
            continue
        if not submodule_worktree_populated(path):
            # The primary checkout left this submodule empty (possibly holding only the
            # bookkeeping `.git` entry). There is nothing to hardlink, and the build
            # needs its sources, so record it as a hard failure instead of silently
            # skipping it: a silent skip previously surfaced as a misleading cmake
            # "cannot find source file" error rather than the infrastructure error it
            # really is (see ensure_primary_submodules).
            failures.append(f"{path}: empty in the primary checkout, cannot hardlink it")
            continue
        dst = os.path.join(BEFORE_SRC, path)
        # SECURITY: defense in depth against a traversal path that somehow slipped past
        # gitmodules_shape_violation — never `rm -rf`/`cp` a destination that resolves
        # outside before_src (e.g. the mounted checkout itself). realpath collapses any
        # `..` / symlink before the containment test; a violation is a hard stop, not a
        # skip, because the shape guard above should already have rejected it.
        dst_real = os.path.realpath(dst)
        if dst_real != before_src_real and not dst_real.startswith(
            before_src_real + os.sep
        ):
            raise RuntimeError(
                f"refusing to populate submodule '{path}': destination '{dst}' "
                f"escapes '{BEFORE_SRC}'"
            )
        # SECURITY: submodule paths come from the PR's `.gitmodules` and are PR-controlled,
        # so shell-quote them (and use `--`) before Shell.check to avoid command/option
        # injection on the runner.
        q_path, q_dst, q_dst_parent = (
            shlex.quote(path),
            shlex.quote(dst),
            shlex.quote(os.path.dirname(dst)),
        )
        # cp -al = recursive hardlink copy (instant, no data duplication). Check the exit
        # status AND that the destination ended up with real content (not just a stray
        # `.git` entry): an unchecked failed copy (e.g. cross-device link, ENOSPC) would
        # otherwise leave the submodule empty and only fail much later inside cmake.
        ok = Shell.check(
            f"rm -rf -- {q_dst} && mkdir -p -- {q_dst_parent} && cp -al -- {q_path} {q_dst}",
            verbose=False,
        )
        if not ok or not (os.path.isdir(dst) and submodule_worktree_populated(dst)):
            failures.append(f"{path}: hardlink copy into the before-worktree failed")

    if failures:
        print(
            "Failed to populate before-worktree submodules:\n  " + "\n  ".join(failures)
        )
        return False

    # Belt-and-suspenders on top of the per-submodule checks above: the marker file must
    # exist in the worktree, confirming at least the reference submodule is really there.
    return os.path.isfile(os.path.join(BEFORE_SRC, SUBMODULE_MARKER))


def reset_before_build_dir():
    """Empty the build directory so the next build type is configured from scratch.

    Returns False if it survives: configuring a different `SANITIZE` value on top of the
    previous arm's `CMakeCache.txt` builds one sanitizer while the report names another.
    """
    Shell.check(f"rm -rf {BEFORE_SRC}/build", verbose=True)
    return not os.path.exists(f"{BEFORE_SRC}/build")


def configure_before_binary(info, build_type):
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
    # worktree at `ci/tmp/before_src` misses all of it. This is accepted: no other job
    # requires this one's artifacts, so the cold build delays nothing but the final
    # report. Bind-mounting the worktree onto `/ClickHouse` to recover hits does
    # NOT work — sccache's server compiles in its own mount namespace, not the client's.
    #
    # Reuse the exact flags the build job uses for this build type, but point the source
    # tree, build dir, and toolchain file at the merge-base worktree (the only path the
    # dict hardcodes to the primary checkout is the toolchain file).
    cmake_flags = BUILD_TYPE_TO_CMAKE[build_type].replace(
        f"{REPO_NORMALIZED}/cmake/", f"{BEFORE_SRC_NORMALIZED}/cmake/"
    )
    cmake_cmd = f"{cmake_flags} {BEFORE_SRC_NORMALIZED} -B {BEFORE_BUILD_NORMALIZED}"
    return Result.from_commands_run(
        name=f"Configure before-binary (cmake, {build_type})",
        command=[cmake_cmd],
        workdir=BEFORE_BUILD_NORMALIZED,
        with_log=True,
    )


def compile_before_binary(build_type):
    """Compile only the `unit_tests_dbms` target in the configured before-worktree.

    Returns the ninja Result. A failure here means the overlaid test does not compile
    against the merge-base sources — strong evidence it depends on code the PR adds.
    """
    compile_result = Result.from_commands_run(
        name=(
            "Compile before-binary (ninja unit_tests_dbms, without the fix, "
            f"{build_type})"
        ),
        command=["ninja unit_tests_dbms"],
        workdir=BEFORE_BUILD_NORMALIZED,
        with_log=True,
    )
    Shell.check("sccache --show-stats", verbose=True)
    return compile_result


# A clang/gcc diagnostic line: "path:line[:col]: [fatal] error: ...". Notes and
# warnings deliberately do not match — only hard errors attribute the build failure.
_COMPILE_ERROR_LINE_RE = re.compile(
    r"^(?P<path>[^\s:]+):\d+(?::\d+)?:\s*(?:fatal\s+)?error:", re.MULTILINE
)
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def attribute_compile_errors(compile_result, test_files):
    """Split the before-build's compile-error paths into the PR's overlaid test files
    vs everything else.

    Returns `(overlaid, other)` — sorted lists of error-carrying paths, repo-relative
    for paths under the before-worktree. Both empty means the ninja failure produced
    no parsable compiler diagnostic (compiler killed, link failure, ninja internal
    error) — not attributable either way, so the caller must fail close.
    """
    test_file_set = set(test_files)
    marker = f"{BEFORE_SRC}/"
    overlaid = set()
    other = set()
    for log in compile_result.files or []:
        try:
            with open(log, "r", errors="replace") as f:
                content = _ANSI_ESCAPE_RE.sub("", f.read())
        except OSError as e:
            print(f"WARNING: could not read compile log {log}: {e}")
            continue
        for m in _COMPILE_ERROR_LINE_RE.finditer(content):
            path = m.group("path")
            idx = path.find(marker)
            rel = path[idx + len(marker) :] if idx != -1 else path
            (overlaid if rel in test_file_set else other).add(rel)
    return sorted(overlaid), sorted(other)


# ninja prints `FAILED: <outputs>` and then the exact command it ran, so the compiled
# translation unit of a failed edge is the `-c <source>` argument of the next line.
_NINJA_FAILED_LINE_RE = re.compile(r"^FAILED:(?:\s|$)")
_COMPILE_SOURCE_RE = re.compile(r"(?:^|\s)-c\s+(\S+)")
# The line after `FAILED:` is the command only if it starts with the program ninja ran:
# a path to a tool or wrapper script, optionally behind cmake's `: && ` or `cd <dir> && `
# prefix. A diagnostic never takes that shape - it indents, or its first token carries
# the `path:line:` colons, or it leads with a bare word such as `In` or `clang`.
_NINJA_COMMAND_LINE_RE = re.compile(r"^(?::\s*&&\s+)?(?:cd|[^\s:]*/[^\s:]*)(?:\s|$)")


def failed_compile_edge_sources(compile_result):
    """Split the before-build's failed ninja edges four ways.

    Returns `(sources, unattributable, unreadable, diagnostic_free)` - sorted lists of,
    respectively, the compiled translation units of the failed edges (repo-relative for
    paths under the before-worktree); the outputs of the edges whose command line was
    read as a command and carries no `-c <source>`, such as a link step, an archive step
    or a custom command; the outputs of the edges whose command line is absent or is not
    recognisable as a command, because the log ends there or something else stands in its
    place; and the translation units of the compile edges that raised no parsable
    compiler error of their own.

    A non-empty `unattributable` or `diagnostic_free` is evidence that something failed
    which either is not a translation unit or never said why. A non-empty `unreadable` is
    only the absence of evidence about that edge, which is why they are kept apart.

    An edge's own diagnostics are the lines up to the next `FAILED:` edge, so one edge's
    error cannot vouch for another edge's silence.
    """
    marker = f"{BEFORE_SRC}/"
    sources = set()
    unattributable = set()
    unreadable = set()
    diagnostic_free = set()
    for log in compile_result.files or []:
        try:
            with open(log, "r", errors="replace") as f:
                content = _ANSI_ESCAPE_RE.sub("", f.read())
        except OSError as e:
            print(f"WARNING: could not read compile log {log}: {e}")
            continue
        lines = content.splitlines()
        edges = [i for i, line in enumerate(lines) if _NINJA_FAILED_LINE_RE.match(line)]
        for n, i in enumerate(edges):
            outputs = lines[i][len("FAILED:") :].strip() or "unnamed edge"
            command = lines[i + 1] if i + 1 < len(lines) else ""
            if not command.strip() or not _NINJA_COMMAND_LINE_RE.match(command):
                unreadable.add(outputs)
                continue
            m = _COMPILE_SOURCE_RE.search(command)
            if not m:
                unattributable.add(outputs)
                continue
            path = m.group(1)
            idx = path.find(marker)
            rel = path[idx + len(marker) :] if idx != -1 else path
            sources.add(rel)
            end = edges[n + 1] if n + 1 < len(edges) else len(lines)
            if not any(
                _COMPILE_ERROR_LINE_RE.match(line) for line in lines[i + 1 : end]
            ):
                diagnostic_free.add(rel)
    return (
        sorted(sources),
        sorted(unattributable),
        sorted(unreadable),
        sorted(diagnostic_free),
    )


def compile_failure_attribution(compile_result, test_files):
    """Decide whether the before-build failure belongs to the overlaid test files alone.

    Returns `(reason, other_errors, refusal)`: `reason` is a non-empty sentence naming the
    attribution basis when the failure is fully attributable to the overlaid tests (the
    caller then reports XFAIL), and an empty string when it is not (the caller fails
    close with ERROR). `other_errors` is the error-carrying paths outside the overlaid
    tests, and `refusal` names what defeated attribution when no such path did, both for
    the ERROR message.

    Two attribution bases, tried in that order:

    * the path on every `error:` diagnostic is an overlaid test file;
    * every translation unit that failed to compile is an overlaid test file. This covers
      the failure clang reports inside a header the overlaid test includes: a
      construction through a template (`std::make_unique` and friends) is performed on a
      libcxx line, so the only `error:` line names a contrib path while the overlaid test
      appears merely on a `note: in instantiation of ...` line. The failing ninja edge
      names the translation unit instead, which is what attribution really asks, because
      the before-worktree is merge-base sources with only the PR's test files overlaid:
      a broken fix source or contrib header is a different translation unit and fails
      its own edge.

    A failed edge that was read and is not a compile at all (a link step, an archive step,
    a custom command), or that compiled a translation unit other than an overlaid test
    file, defeats both bases whatever the diagnostics say: something outside the overlaid
    tests demonstrably failed.

    An edge whose command line could not be read defeats only the second basis, which
    claims every failed translation unit is an overlaid test and therefore needs all of
    them enumerated. The first basis reasons about the diagnostics that are present, so an
    edge this log does not describe cannot contradict it.

    A compile edge that raised no parsable error of its own is not attributable either: a
    killed compiler says nothing about why that translation unit failed, and a diagnostic
    belonging to a different edge cannot answer for it.
    """
    overlaid_errors, other_errors = attribute_compile_errors(compile_result, test_files)
    sources, unattributable, unreadable, diagnostic_free = failed_compile_edge_sources(
        compile_result
    )
    if unattributable:
        return (
            "",
            other_errors,
            "failed build steps that name no translation unit: "
            + ", ".join(unattributable),
        )
    non_test_sources = [source for source in sources if source not in set(test_files)]
    if non_test_sources:
        return (
            "",
            other_errors,
            "translation units outside the PR's changed test files failed to compile: "
            + ", ".join(non_test_sources),
        )
    if diagnostic_free:
        return (
            "",
            other_errors,
            "failed build steps with no compiler diagnostic of their own: "
            + ", ".join(diagnostic_free),
        )
    if overlaid_errors and not other_errors:
        return (
            "every compile error is inside the PR's changed test files ("
            + ", ".join(overlaid_errors)
            + ")",
            other_errors,
            "",
        )
    if not (overlaid_errors or other_errors):
        return "", other_errors, "the build produced no parsable compiler diagnostic"
    if unreadable:
        return (
            "",
            other_errors,
            "failed build steps whose command line could not be read: "
            + ", ".join(unreadable),
        )
    if not sources:
        return "", other_errors, ""
    return (
        "every translation unit that failed to compile is one of the PR's changed test "
        "files (" + ", ".join(sources) + "), and the compile error is raised inside a "
        "header they include",
        other_errors,
        "",
    )


_SANITIZER_OPTION_VARS = ("ASAN_OPTIONS", "TSAN_OPTIONS", "UBSAN_OPTIONS", "MSAN_OPTIONS")


def set_sanitizer_symbolizer_options():
    """Point the sanitizer runtimes at an explicit `llvm-symbolizer`.

    A report is symbolized before the process aborts, and with no unversioned
    `llvm-symbolizer` on `PATH` the runtime falls back to `addr2line`, which does not finish
    on a multi-GB `unit_tests_dbms`, so the run reaches the report as a timeout rather than a
    verdict. The binary-builder image installs `llvm-symbolizer-<version>` without that
    symlink. Setting only this option keeps the binary's compiled-in defaults in force
    (`base/base/sanitizer_options.h`).
    """
    symbolizer = shutil.which("llvm-symbolizer") or shutil.which(
        f"llvm-symbolizer-{ToolSet.COMPILER_C.rsplit('-', 1)[-1]}"
    )
    if not symbolizer:
        print(
            "WARNING: no llvm-symbolizer found; a sanitizer report would be symbolized "
            "with addr2line, which does not finish on this binary"
        )
        return
    for var in _SANITIZER_OPTION_VARS:
        options = os.environ.get(var, "")
        if "external_symbolizer_path" in options:
            continue
        os.environ[var] = f"{options} external_symbolizer_path={symbolizer}".strip()


def run_gtests(binary_path, gtest_filter, name):
    # Sanitizer builds: do not wrap with gdb (LSan is incompatible with the debugger),
    # and disable the uninstrumented FIPS provider to avoid sanitizer false positives.
    os.environ["OPENSSL_CONF"] = "/dev/null"
    set_sanitizer_symbolizer_options()
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


def declared_case_matchers(test_files):
    """Per case declared in the changed test files, a regex matching how gtest reports it.

    A case declared behind a preprocessor guard is never registered, so it is absent from the
    report rather than reported as not run. Comparing declarations against the report needs
    every naming form `build_gtest_filter` documents, plus the empty-`INSTANTIATE_TEST_SUITE_P`
    prefix form `Suite.Case/0` (`gtest-param-util.h` drops the `Prefix/` when it is empty; the
    typed macro static_asserts a non-empty one, so it has no such form).
    """
    matchers = {}
    for fpath in test_files:
        # The primary checkout is the base+PR MERGE ref, so it can hold a case the base added
        # and the overlay does not; read the overlaid PR-head file that the arm actually built.
        overlaid = os.path.join(BEFORE_SRC, fpath)
        source = overlaid if os.path.isfile(overlaid) else fpath
        try:
            with open(source, "r", errors="replace") as f:
                content = f.read()
        except OSError as e:
            print(f"WARNING: could not read {source}: {e}")
            continue
        for m in _CASE_RE.finditer(content):
            suite, case = re.escape(m.group(1)), re.escape(m.group(2))
            matchers[f"{m.group(1)}.{m.group(2)}"] = re.compile(
                rf"^(?:{suite}\.{case}(?:/.+)?|.+/{suite}\.{case}/.+"
                rf"|{suite}/.+\.{case}|.+/{suite}/.+\.{case})$"
            )
    return matchers


def changed_cases_unexercised(test_files):
    """Did this arm fail to exercise the changed regression cases?

    gtest reports a `GTEST_SKIP()` case as `"result": "SKIPPED"` with `"status": "RUN"` and a
    disabled one as `"SUPPRESSED"`/`"NOTRUN"`, while `ResultTranslator.from_gtest` keys on
    `"status"` alone, so a case that never ran reaches a caller of `run_gtests` as a passing
    one; read the gtest report directly instead. A case is matched to a changed file by the
    basename of its `"file"`, which `-ffile-prefix-map` (CMakeLists.txt) reduces to the
    source-relative path, and a case declared in a changed file but absent from the report was
    compiled out by a preprocessor guard (the filter covers every suite those files declare,
    so a registered case is always reported). Either way the file is only partly exercised and
    which of its cases is the regression one is not known here; when the changed files declare
    no case at all, such as a touched header, only a run that executed nothing is provably no
    measurement. Returns (unexercised,
    unexercised_case_names, reason), and `unexercised` rests on a case positively reporting
    `SKIPPED`/`SUPPRESSED` or on a declaration positively absent from a readable report, so a
    report that cannot be read leaves the caller's existing verdict in place.
    """
    report_path = ResultTranslator.GTEST_RESULT_FILE
    try:
        with open(report_path, "r", encoding="utf-8", errors="ignore") as f:
            report = json.load(f)
        cases = [
            (suite.get("name", "?"), case)
            for suite in report["testsuites"]
            for case in suite["testsuite"]
        ]
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"WARNING: could not read the gtest report {report_path}: {e}")
        return False, [], ""
    changed_basenames = {os.path.basename(f) for f in test_files}
    changed = [
        (suite_name, case)
        for suite_name, case in cases
        if os.path.basename(case.get("file") or "") in changed_basenames
    ]
    unexecuted = {"SKIPPED", "SUPPRESSED"}
    reported = [f"{suite_name}.{case.get('name', '?')}" for suite_name, case in cases]
    not_run = [
        f"{name} ({case.get('result')})"
        for name, (_, case) in zip(reported, cases)
        if case.get("result") in unexecuted
    ]
    missing = [
        name
        for name, matcher in declared_case_matchers(test_files).items()
        if not any(matcher.match(r) for r in reported)
    ]
    not_run += [f"{name} (not registered)" for name in missing]
    if missing:
        reason = "a case declared in the changed test files was not registered"
        unexercised = True
    elif changed:
        reason = "a case of the changed test files did not run"
        unexercised = any(case.get("result") in unexecuted for _, case in changed)
    else:
        reason = "no selected case ran"
        unexercised = bool(cases) and all(
            case.get("result") in unexecuted for _, case in cases
        )
    return unexercised, not_run, reason


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
    # Overlay the PR-HEAD version of the test files (info.sha), NOT `git rev-parse HEAD`:
    # the default checkout is the base+PR merge commit, whose test file is base-merged, not
    # what the PR author wrote. See determine_merge_base.
    pr_sha = info.sha
    assert pr_sha, "Info.sha (PR head commit) is empty; cannot overlay the PR's test files"
    merge_base = determine_merge_base(info)
    print(f"PR head commit: {pr_sha}")
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

    # Fail close if submodule state (any gitlink or `.gitmodules`) differs between the
    # merge-base and the checkout `HEAD`: the before-worktree's submodules are
    # hardlinked from the primary checkout, whose submodules are at HEAD's recorded
    # revisions (normally the synthetic base+PR merge ref). Comparing against HEAD —
    # not the PR head — also catches a base-only submodule bump after the branch split,
    # which would otherwise leak base-tip contrib sources into the merge-base build.
    # Either way the "before" binary would be built against the wrong submodule content
    # (or miss a merge-base-only submodule entirely) and the validator could report a
    # false reproduction or refutation. Inconclusive (ERROR), not a pass.
    checkout_head = Shell.get_output("git rev-parse HEAD").strip()
    assert (
        checkout_head
    ), "Failed to resolve the checkout HEAD; cannot verify submodule state"
    submodule_changes = get_submodule_state_changes(merge_base, checkout_head)
    if submodule_changes:
        finalize(
            [
                Result(
                    name="Bugfix validation (unit tests)",
                    status=Result.Status.ERROR,
                    info=(
                        "Submodule state differs between the merge-base and the "
                        "checkout (" + ", ".join(submodule_changes) + ") — either the "
                        "PR changes submodule state, or the base branch moved a "
                        "submodule after the branch split. The before-worktree can "
                        "only be populated with the primary checkout's submodule "
                        "content, not the merge-base's, so building the before-binary "
                        "would validate against the wrong submodule code. This is "
                        "inconclusive — NOT a reproduction or a refutation."
                    ),
                )
            ],
            "Bugfix validation inconclusive: submodule state differs between the "
            "merge-base and the checkout, and the before-worktree cannot be populated "
            "at the merge-base submodule revisions.",
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
                        "Submodules were not fully populated in the before-worktree "
                        "(see the job log for the specific submodule that could not be "
                        "hardlinked); cannot build the before-binary. This is an "
                        "infrastructure error — NOT a reproduction."
                    ),
                )
            ],
            "Bugfix validation inconclusive: submodules missing in the before-worktree.",
        )
        return

    # 4-5. Build the "before" binary and judge the touched tests on it, one build type at
    # a time, sharing one build directory. Refuting the tests needs a verdict from every
    # build type, so an arm that produced none escalates and is remembered instead.
    arms_tried = []
    arms_compiled = []
    arms_without_a_verdict = []
    for build_type in BEFORE_BUILD_TYPES:
        if arms_tried and not reset_before_build_dir():
            results.append(
                Result(
                    name=f"Reset before-build directory ({build_type})",
                    status=Result.Status.ERROR,
                    info=(
                        f"Could not empty {BEFORE_SRC}/build before configuring "
                        f"{build_type}; refusing to configure on top of the "
                        f"{arms_tried[-1]} CMakeCache.txt, which would build one sanitizer "
                        "and report another. This is an infrastructure error, NOT a "
                        "reproduction."
                    ),
                )
            )
            finalize(
                results,
                "Bugfix validation inconclusive: could not empty the before-build "
                "directory between build types.",
            )
            return
        arms_tried.append(build_type)

        # 4a. Configure. A cmake-configure failure is an environment/infra problem, never
        # evidence that the test depends on the fix — report it as an error, do not pass.
        configure_result = configure_before_binary(info, build_type)
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
        # the overlaid test references *some* code the PR adds, not that it catches the bug
        # at runtime. Attribute the failure instead:
        #  * every compiler error inside the overlaid test files, or every translation unit
        #    that failed to compile being an overlaid test file (compile_failure_attribution)
        #    → the changed test code depends on the fix's interface (typically a call site
        #    adapted to a changed signature). The PR author cannot avoid that adaptation and
        #    the unit side has nothing left to judge, so on the first build type report the
        #    step as an expected failure (XFAIL) with nothing to validate — NOT as a
        #    reproduction; on a later one an earlier arm already compiled this overlay, so
        #    the same attribution is ambiguous and stays an ERROR. When the PR
        #    also carries functional/integration tests, new_tests_check.py still demands a
        #    real validation from those jobs; for a unit-only PR the merge gate already
        #    treats inconclusive as non-blocking, so this changes report truthfulness, not
        #    gating.
        #  * anything else (a failed fix-source or contrib translation unit, the linker, no
        #    parsable diagnostic) → cannot be attributed to the touched test changes; fail
        #    close (ERROR).
        compile_result = compile_before_binary(build_type)
        if not compile_result.is_ok():
            attributed_to, other_errors, refusal = compile_failure_attribution(
                compile_result, test_files
            )
            if attributed_to and arms_compiled:
                # An earlier arm compiled this same overlay, so the failure is confined to
                # what this build type compiles differently, and decides nothing either way.
                compile_result.set_status(Result.Status.ERROR)
                compile_result.set_info(
                    f"The before-binary compiled the overlaid unit-test changes on "
                    f"{arms_compiled[0]} but not on {build_type}: "
                    + attributed_to
                    + ". Only what this build type compiles differently can be at fault, "
                    "which may include a sanitizer-conditional part of the test that depends "
                    "on the interface this PR introduces. Either way nothing can be "
                    "concluded: this is inconclusive — NOT a refutation. "
                    + (compile_result.info or "")
                )
                results.append(compile_result)
                finalize(
                    results,
                    f"Bugfix validation inconclusive: the before-binary compiled on "
                    f"{arms_compiled[0]} but failed to COMPILE on {build_type}.",
                )
                return
            if attributed_to and len(arms_tried) < len(BEFORE_BUILD_TYPES):
                # No arm has compiled this overlay yet and a build type is left. What a
                # test compiles is build-type-dependent too (a sanitizer-conditional part of
                # it can reference the fix's interface), so the next arm may still validate.
                compile_result.set_label(Result.Label.XFAIL)
                compile_result.set_status(Result.Status.XFAIL)
                compile_result.set_info(
                    f"The before-binary cannot compile the overlaid unit-test changes on "
                    f"{build_type}: "
                    + attributed_to
                    + f". That is expected when the changed test code depends on the "
                    f"interface this PR introduces, and it is not a reproduction, but it "
                    f"is not a refutation either, and a build type that may compile the "
                    f"overlay is left. Escalating to {BEFORE_BUILD_TYPES[len(arms_tried)]}."
                )
                results.append(compile_result)
                arms_without_a_verdict.append(f"{build_type} (overlay does not compile)")
                continue
            if attributed_to:
                compile_result.set_label(Result.Label.XFAIL)
                compile_result.set_status(Result.Status.XFAIL)
                compile_result.set_info(
                    "The before-binary cannot compile the overlaid unit-test changes on any "
                    "validated build type ("
                    + ", ".join(arms_tried)
                    + "): "
                    + attributed_to
                    + ". The changed test code depends on the interface this PR introduces "
                    "(e.g. a call site adapted to a changed signature), so there is nothing "
                    "the unit side can validate on the merge base. This is expected, not an "
                    "error — and it is not counted as a reproduction either. "
                    + (compile_result.info or "")
                )
                results.append(compile_result)
                finalize(
                    results,
                    "Nothing to validate on the unit side: the changed unit-test files do "
                    "not compile against the merge base because they depend on the fix's "
                    "interface. Regression coverage is judged by the functional/integration "
                    "Bugfix validation jobs (enforced by new_tests_check.py when such tests "
                    "exist).",
                )
                return
            compile_result.set_status(Result.Status.ERROR)
            compile_result.set_info(
                "The before-binary FAILED TO COMPILE, and the errors cannot be attributed "
                "to the overlaid test files alone"
                + (f" (errors outside them: {', '.join(other_errors)})" if other_errors else "")
                + (f" ({refusal})" if refusal else "")
                + ". This does not prove the test reproduces the bug. Write a regression "
                "test that builds against the merge-base and fails at runtime without the "
                "fix. " + (compile_result.info or "")
            )
            results.append(compile_result)
            finalize(
                results,
                "Bugfix validation inconclusive: the before-binary failed to COMPILE and "
                "the failure cannot be attributed to the touched regression case.",
            )
            return
        build_result = compile_result
        arms_compiled.append(build_type)

        results.append(build_result)

        # 5. Run the touched tests on the "before" binary — at least one must fail/crash.
        before_result = run_gtests(
            BEFORE_BINARY,
            gtest_filter,
            name=f"Touched unit tests on the before-binary ({build_type})",
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

        # A "pass" only refutes the bug if the touched suite actually ran, and a clean exit
        # with no "[ RUN ]" marker means the filter matched nothing in this binary. Whether
        # another build type can still run those cases is what decides the job here:
        # `_UNIT_TEST_FILE_RE` also matches a standalone `*.cpp`/`*.cc`/`*.cxx` under `tests/`
        # (e.g. `test_hive_catalog_url_parsing.cpp`), whose suite is derived but is in no build
        # type's binary, so nothing is left to try; a file that can reach the binary was instead
        # compiled out on this build type alone, and the declaration check below turns that into
        # "no verdict". Either way it is not a refutation.
        compiled_in = [f for f in test_files if can_reach_unit_tests_dbms(f)]
        if not before_run_started_a_test(before_result) and not (
            compiled_in and declared_case_matchers(compiled_in)
        ):
            before_result.set_status(Result.Status.ERROR)
            before_result.set_info(
                "The before-binary ran no touched test (no gtest '[ RUN ]' marker) yet exited "
                "cleanly — the touched suite is not compiled into `unit_tests_dbms`, which is "
                "built from `gtest*.cpp` sources only. This is inconclusive — NOT a refutation."
            )
            results.append(before_result)
            finalize(
                results,
                "Bugfix validation inconclusive: none of the touched unit tests are compiled "
                "into `unit_tests_dbms` (e.g. a standalone, non-`gtest*.cpp` test file).",
            )
            return

        # A case that skipped itself or is disabled is still reported as passing, and a
        # `GTEST_SKIP()` even prints a "[ RUN " marker, so an arm that never ran a changed case
        # reaches this point as "all touched tests pass": no verdict, as with a wrong build.
        unexercised, not_run_cases, reason = changed_cases_unexercised(test_files)
        arms_left = len(BEFORE_BUILD_TYPES) - len(arms_tried)
        escalating = f" Escalating to {BEFORE_BUILD_TYPES[len(arms_tried)]}." if arms_left else ""
        not_exercised = (
            f" Not exercised there: {', '.join(not_run_cases)}." if not_run_cases else ""
        )
        if unexercised:
            before_result.set_status(Result.Status.SKIPPED)
            before_result.set_info(
                f"On the {build_type} before-binary {reason} "
                f"({', '.join(not_run_cases)}), so this build type did not exercise the "
                f"changed regression case: no verdict, NOT a refutation." + escalating
            )
            arms_without_a_verdict.append(f"{build_type}: {reason}")
            results.append(before_result)
            if arms_left:
                continue
        elif arms_left:
            # Every touched test that ran passed on this arm. That only refutes the bug once
            # no build type is left: a failure mode this build cannot observe is
            # indistinguishable from a test that does not catch the bug.
            before_result.set_info(
                f"All touched unit tests PASS on the {build_type} before-binary. That is not "
                f"a refutation on its own: this build cannot observe a failure mode specific "
                f"to another build type." + not_exercised + escalating
            )
            results.append(before_result)
            continue

        # No build type is left. Refuting the test needs a verdict from every one of them.
        if arms_without_a_verdict:
            results.append(
                Result(
                    name="Bugfix validation verdict",
                    status=Result.Status.ERROR,
                    info=(
                        "The touched unit tests were never both built and run on at least "
                        "one validated build type: "
                        + "; ".join(arms_without_a_verdict)
                        + ". The bug is therefore neither reproduced nor refuted on the "
                        "merge base: this is inconclusive, NOT a refutation. A regression "
                        "case that runs, and fails without the fix, on one of "
                        + ", ".join(BEFORE_BUILD_TYPES)
                        + " would be validated here."
                    ),
                )
            )
            finalize(
                results,
                "Bugfix validation inconclusive: not every validated build type produced a "
                "verdict on the touched unit tests.",
            )
            return

        before_result.set_status(Result.Status.FAIL)
        before_result.set_info(
            "Failed to reproduce the bug: all touched unit tests PASS on the before-binary "
            "(merge-base without the fix) on every validated build type ("
            + ", ".join(arms_tried)
            + "). The added/changed test does not catch the bug the fix addresses."
            + not_exercised
        )
        results.append(before_result)
        finalize(results, "Failed to reproduce the bug.")
        return


if __name__ == "__main__":
    main()
