---
name: update-contrib
description: Update a ClickHouse third-party library (contrib submodule) to a new version. Handles fork management, submodule pointer bumps, CMake adaptation, and source code fixes. Use when the user wants to bump a dependency.
argument-hint: <library-name> [target-version]
disable-model-invocation: false
allowed-tools: Agent, Task, Bash, Read, Write, Edit, Glob, Grep, WebFetch, AskUserQuestion
---

# Update a ClickHouse Contrib (Third-Party Library)

Bump a git submodule under `contrib/` to a new version, adapting CMake build files and ClickHouse source code as needed.

## Arguments

- `$0` (required): Library name as it appears under `contrib/` (e.g., `curl`, `openssl`, `arrow`)
- `$1` (optional): Target version — a git tag, branch, or commit SHA. If omitted, the latest upstream release tag is used.

## Two modes

**Interactive** (a human runs `/update-contrib`): follow every step below, including the confirmations.

**Non-interactive / pipeline mode**: the invoking prompt states that the run is automated and passes the target,
the branch name, the build directory and often a facts block (submodule URL, current pin, target commit, fork branch,
CVE list). In that mode:
- a given target is confirmed — skip step 2's discovery and never call `AskUserQuestion`;
- never push, never create branches in forks, never open PRs, never use `gh`; the pipeline pushes and opens the PR;
- do not re-derive facts the prompt already states (URL, pin, tags, build flags) — the pipeline fetched full history
  and tags into the submodule before the run;
- if the update needs a change inside a ClickHouse fork that does not exist yet, stop and write the reason to the
  `BLOCKED.md` path named in the prompt instead of improvising;
- finish by writing the report the prompt asks for (`REPORT.md`) with an explicit "Open concerns" section
  ("none" when there are none).

## Background

ClickHouse vendors all third-party libraries as git submodules under `contrib/`. Each library has:
- `contrib/<lib>/` — the submodule (upstream repo or a ClickHouse fork)
- `contrib/<lib>-cmake/` — ClickHouse's own CMake build files

`contrib/update-submodules.sh` deletes the upstream `CMakeLists.txt` files after checkout; ClickHouse relies
exclusively on its own `*-cmake/` files. Consequences: `git -C contrib/<lib> status` normally shows deleted
upstream CMake files — that is expected, do not restore them and do not re-run the deletion by hand; ninja does not
need it either.

Submodule URLs point to either upstream directly (unpatched) or a fork at `github.com/ClickHouse/<repo>` (when
patches are needed; the repo name may differ from the contrib name, e.g. `contrib/cassandra` → `ClickHouse/cpp-driver`).
Fork branches are named `ClickHouse/<version>` — usually the bare upstream tag (`ClickHouse/v1.52.1`,
`ClickHouse/2.17.1`), sometimes with the upstream's own prefix (`ClickHouse/openssl-3.5.8`,
`ClickHouse/release-2.14.1`) — never `ClickHouse/master` or `ClickHouse/main`. A pin can also sit on a fork branch
while `.gitmodules` still names the upstream URL (GitHub serves fork-network objects through the parent repo); check
the fork's branches before assuming a pin is a plain upstream commit.

Submodules are cloned shallow in most checkouts. `git describe` and `git log OLD..NEW` then report nonsense or fail;
run `git -C contrib/<lib> fetch --unshallow --tags origin` first (the pipeline does this for you).

A few libraries are built through cargo instead of a plain CMake file list — either from `rust/workspace/<lib>/`
(`wasmtime`) or from a `contrib/<lib>-cmake/` file that drives cargo (`chdig`, `delta-kernel-rs`); they export
`ch_rust::*` targets. See [rust.md](rust.md) for those.

## Process

### 1. Gather information about the library

Throughout this skill, `$LIB` refers to the library name passed as `$0` and `$VERSION` to the target version once
resolved (step 2). Both must match `[A-Za-z0-9._-]+` so they are safe to use unquoted in paths and branch names;
reject anything else.

Skip whatever the prompt already states. Otherwise:

```bash
git config --file .gitmodules --get "submodule.contrib/$LIB.url"   # upstream or ClickHouse fork?
git -C "contrib/$LIB" rev-parse HEAD                                 # current pin
git -C "contrib/$LIB" fetch --unshallow --tags origin 2>/dev/null || git -C "contrib/$LIB" fetch --tags origin
git -C "contrib/$LIB" describe --tags --abbrev=0 2>/dev/null || echo "no tags"
```

Then locate the integration file(s) and keep them in `$INTEGRATION`; every later step works from that variable.
Three shapes exist:

```bash
INTEGRATION=$(ls contrib/${LIB}-cmake/CMakeLists.txt rust/workspace/${LIB}/CMakeLists.txt 2>/dev/null)
# Shared wrapper: no directory of its own, built by another contrib's CMake file
# (aws-c-auth → contrib/aws-cmake/, the aws-c-* family; individual boost libs → contrib/boost-cmake/).
[ -n "$INTEGRATION" ] || INTEGRATION=$(grep -rl "contrib/${LIB}\b" contrib/*-cmake/CMakeLists.txt)
echo "$INTEGRATION"; [ -n "$INTEGRATION" ] || echo "no integration file found — stop and report"
grep -n "\b${LIB}\b" contrib/CMakeLists.txt rust/workspace/CMakeLists.txt   # where it is added
```

- plain CMake: `contrib/<lib>-cmake/CMakeLists.txt`;
- Rust: either `rust/workspace/<lib>/CMakeLists.txt` (`wasmtime`) or a `contrib/<lib>-cmake/` file that drives
  cargo (`chdig`, `delta-kernel-rs`) — see [rust.md](rust.md);
- shared wrapper: the fallback above; the wrapper's file list and aliases (`ch_contrib::aws_s3` for every
  `aws-c-*` library) are what steps 8, 9 and 11 must use.

Do not assume the exported target is `ch_contrib::${LIB}`. Take the actual alias names from `$INTEGRATION`
and grep for those — Rust-backed libraries export `ch_rust::*` (`ch_rust::wasmtime`, `ch_rust::chdig`,
`ch_rust::delta_kernel_rs`), a few keep upstream-style names (`OpenSSL::SSL`, `boost::filesystem`), and one
library can export several:

```bash
ALIASES=$(grep -ho 'add_library *( *[A-Za-z0-9_:.-]*::[A-Za-z0-9_.-]* *ALIAS' $INTEGRATION \
          | sed -E 's/add_library *\( *//; s/ *ALIAS$//' | sort -u)
echo "$ALIASES"
for a in $ALIASES; do grep -rl --include=CMakeLists.txt -F "$a" src programs base rust; done | sort -u   # consumers
```

Read `$INTEGRATION` once; it tells you which sources are compiled, which defines and generated headers exist, and
whether the library is header-only (`add_library(... INTERFACE)` — then nothing of it is compiled and only its
consumers can break). The consumer list is what steps 9 and 11 work from.

### 2. Determine the target version

If `$1` was provided, use it. Otherwise, find the latest release:

```bash
git -C "contrib/$LIB" tag -l --sort=-v:refname | head -20
# For ClickHouse forks, also check the upstream repo: git ls-remote --tags <upstream-url>
```

Interactive mode only: present the current and target versions to the user with `AskUserQuestion` for confirmation
before proceeding. `VERSION` is whatever was confirmed (or `$1`). Verify it matches the charset above before using it
in branch names or commits.

### 3. Check for ClickHouse patches

First decide whether the pin is a plain upstream commit. Do not rely on the `.gitmodules` URL alone: `origin` is
the upstream repo when the URL names upstream, and `ls-remote --heads origin 'ClickHouse/*'` then returns nothing
even if the pin sits on a fork branch. Check that `HEAD` is reachable from an upstream ref, and probe the fork
separately (pipeline mode: the facts block names the fork URL and branch — use them):

```bash
git -C "contrib/$LIB" fetch --tags origin '+refs/heads/*:refs/remotes/origin/*'    # full upstream refs (step 1)
git -C "contrib/$LIB" branch -r --contains HEAD | head -3                          # empty: not an upstream commit
git -C "contrib/$LIB" tag --contains HEAD | head -3

UPSTREAM_URL=$(git config --file .gitmodules --get "submodule.contrib/$LIB.url")
FORK_URL="https://github.com/ClickHouse/$(basename "$UPSTREAM_URL" .git)"        # guess; may differ (cassandra → cpp-driver)
git -C "contrib/$LIB" fetch "$FORK_URL" '+refs/heads/ClickHouse/*:refs/remotes/ch-fork/ClickHouse/*' 2>/dev/null
git -C "contrib/$LIB" branch -r --contains HEAD 'ch-fork/*'                       # non-empty: pin lives on a fork branch
```

Classify:
- `HEAD` is on an upstream branch or tag and on no `ch-fork/ClickHouse/*` branch → unpatched upstream pin; skip
  the rest of this step.
- `HEAD` is on a `ch-fork/ClickHouse/*` branch → forked, patched pin (even when `.gitmodules` names upstream; step 5
  fixes the URL). Continue below.
- `HEAD` is on neither and the fork guess failed (`fetch` errored or the repo name differs) → do not conclude
  "unpatched". Interactive mode: ask the user for the fork URL. Pipeline mode: stop and record the reason in
  `BLOCKED.md`.

For a forked pin, identify the patches that must survive the bump:

```bash
UPSTREAM_TAG=$(git -C "contrib/$LIB" describe --tags --abbrev=0)     # needs full history (step 1)
git -C "contrib/$LIB" log --oneline "$UPSTREAM_TAG"..HEAD              # ClickHouse-specific commits
```

Report the patches found. A patch is obsolete only when upstream now contains an equivalent fix — verify in the code,
not by commit subject.

### 4. Create a branch for the update

```bash
git checkout -b "bump-${LIB}-${VERSION}"     # pipeline mode: use the branch name given in the prompt
```

### 5. Update the fork (if applicable)

If the library uses a ClickHouse fork and has patches, the new branch `ClickHouse/<new-version>` must exist in the
fork with the patches rebased onto the upstream tag before the ClickHouse-side bump:

1. Interactive mode: prepare it in a clone of the fork — branch from the upstream tag, `cherry-pick -x` the patches in
   order (authors preserved), resolve conflicts from the new upstream code, drop only patches that upstream has
   absorbed. Do not push until the build is validated and the user confirms.
2. Pipeline mode: the branch is prepared by a separate job and named in the prompt; point the submodule at exactly
   that commit and never touch the fork.

If the library has no ClickHouse patches but uses a fork, switch the submodule to the upstream URL instead of
creating a new fork branch (update `.gitmodules`, see step 6). Conversely, if step 3 found the pin on a fork branch
while `.gitmodules` names upstream, fix the URL to the fork in the same commit. Never rewrite a URL to upstream
unless step 3 positively classified the pin as an unpatched upstream commit.

### 6. Update the submodule pointer

```bash
git -C "contrib/$LIB" fetch origin              # plus the fork URL / branch when the target lives there
git -C "contrib/$LIB" checkout <target-commit>
git add "contrib/$LIB"
```

If the submodule URL changes (fork → upstream, or a fork whose URL was wrong), also update `.gitmodules` and run
`git submodule sync -- "contrib/$LIB"`:

```bash
git config --file .gitmodules "submodule.contrib/$LIB.url" "<new-url>"
git add .gitmodules
```

`git submodule status` must show no `+` line for the library when you are done (a `+` means the working tree is
not at the recorded commit).

### 7. Diff the source tree between old and new versions

This is the step that determines what CMake and source changes are needed:

```bash
OLD_COMMIT=$(git rev-parse "HEAD:contrib/$LIB"); NEW_COMMIT=$(git -C "contrib/$LIB" rev-parse HEAD)
git -C "contrib/$LIB" diff --stat "$OLD_COMMIT..$NEW_COMMIT"
git -C "contrib/$LIB" diff --name-status "$OLD_COMMIT..$NEW_COMMIT" -- '*.c' '*.cpp' '*.cc' '*.h' '*.hpp' '*.in' 'CMakeLists.txt'
```

Pay attention to:
- **Added/removed/renamed source files** — the CMake file list must follow
- **New build options or defines** — may need new CMake variables or hardcoded config headers
- **Changed public headers** — may require source adaptation in `src/`
- **Changed `*.in` templates / generated headers** — regenerate the delta, never overwrite a checked-in generated
  header wholesale (diff generator(old) vs generator(new) and apply only that)

Read the changelog/release notes for API and behaviour changes (stored formats, output changes, dependency
version floors that force co-bumps).

### 8. Update build integration files

Update the file(s) in `$INTEGRATION` from step 1 (`contrib/<lib>-cmake/CMakeLists.txt`, a shared wrapper such as
`contrib/aws-cmake/CMakeLists.txt`, or the Rust files — see [rust.md](rust.md)) to reflect the new version:

1. Compare source file lists against the actual files in the updated submodule; add new files after the first
   existing sibling from the same directory, remove deleted ones.
2. Check for new required defines, include paths, or compile options.
3. Verify the result follows ClickHouse CMake conventions — **no** `find_package`/`find_path`/`find_library`,
   `check_c_compiler_flag`, `check_cxx_compiler_flag`, `check_c_source_compiles`, `check_include_file`,
   `check_symbol_exists`, `check_type_size`, `cmake_push_check_state`, `CMAKE_REQUIRED_FLAGS`, or any `Check*` CMake
   module — these are forbidden by CI style checks (`ci/jobs/scripts/check_style/check_cpp.sh`) for hermetic,
   cross-compiled builds. Use hardcoded feature flags instead of runtime detection.
4. If `contrib/<lib>-cmake/` carries config headers (`config.h.in`, hardcoded `*_config.h`), update them for new
   version strings, defines or feature flags.
5. Anything executed at build time (a code generator such as `protoc` or `flatc`) must be a native host tool: register
   it with `add_native_target`, use the `IMPORTED_LOCATION` under `native/` for cross builds, and add its directory
   from the block in `contrib/CMakeLists.txt` where `disable_dummy_launchers_if_needed` restores the real toolchain
   (clang-tidy builds otherwise produce an empty, non-executable binary).
6. If the version is read from a file at configure time (e.g. liburing's `.spec`), re-run cmake after the bump;
   otherwise incremental builds keep a stale generated version header.

### 9. Fix ClickHouse source code

Find the consumers (step 1's alias grep — `ch_contrib::*`, `ch_rust::*` or whatever the integration file exports)
and, if the public API changed, the includes:

```bash
grep -rl "#include.*[<\"]$LIB" src/ --include='*.h' --include='*.cpp'
```

Common adaptation patterns: renamed functions or classes, changed signatures, removed deprecated API, new required
initialisation calls, changed header paths, missing feature guards in copied public headers.

Build to find compilation errors, using the existing build directory (`$BUILD_DIR`, the one named in the prompt or an
existing `build*/` directory; configure one with `cmake -S . -B <dir>` only if none exists — see
`docs/resources/develop-contribute/build/build.mdx`):

```bash
ninja -C "$BUILD_DIR" <lib-target> > "$BUILD_DIR/build_bump_${LIB}_lib.log" 2>&1; echo "exit=$?"
ninja -C "$BUILD_DIR" clickhouse  > "$BUILD_DIR/build_bump_${LIB}.log" 2>&1;     echo "exit=$?"
grep ' error:' "$BUILD_DIR/build_bump_${LIB}.log" | head -20 || true   # grep exits 1 on a clean log: not a failure
```

Run builds in the foreground and wait for them; never start them in the background and poll, and never use
`pgrep -f`/`kill -0` loops on a pattern that matches your own shell. The ninja exit code decides whether the build
passed (`exit=0`); the `grep` only lists the errors to fix, and its own exit status is `1` when there are none, so
do not chain it with `&&` or run it under `set -e`. Do not spawn a sub-agent to read the log. For a header-only library the `clickhouse`
target is the only thing that can break. Fix errors iteratively, committing each logical fix separately.

Do not run unit-test or gtest binaries unless the task asks for it, and never run any binary with the repository
root as the working directory (it litters the checkout with `store/`, `access/`, `preprocessed_configs/`, `test_*`);
use the build directory or `/tmp`.

### 10. Verify the build

After all fixes, rebuild the library target and `clickhouse` once more and check the exit codes as above. Common
issues: missing source files in CMake, changed include paths, API changes requiring source adaptation,
platform-specific issues (macOS, FreeBSD, musl, cross-compilation) that only CI can show, stale generated headers.

Interactive mode: only after the build is validated ask the user whether it is ok to push. Until then, keep both
the ClickHouse branch and any fork branch local-only.

### 11. Check behaviour, not just compilation

If the library's output is user-visible (formatting, hashing, compression, geometry, text processing), compare a
few representative queries between the previous binary and the new one. Byte-identical output is required for text
formatting and hashes; for floating-point, geometry and compressor libraries a difference is acceptable only when the
decoded data round-trips identically, every difference is explained by a named upstream change, and the affected
stateless test references are updated. A stored/on-disk/wire format change (codec frame version, serialisation) is a
compatibility break: state exactly what becomes unreadable and which setting gates the feature.

Then find the stateless tests that exercise the library and run them if a server is available to you (pipeline
mode: the pipeline runs them, just list them in the report):

```bash
grep -rl "$LIB" tests/queries/0_stateless/ --include='*.sql' --include='*.sh' | head -20
```

### 12. Commit

Use the title format `Bump \`<lib>\` from <old_version> to <new_version>` for straightforward bumps, or a more
descriptive title if the update has a specific motivation. Stage every affected integration artifact, not just the
submodule pointer (`.gitmodules`, `contrib/<lib>-cmake/`, `src/` fixes, test references). Do not amend or rebase;
iterative "Fix build" / "Fix darwin" / "Fix MSan" commits are normal.

Examples of good commit messages from past PRs:
- `Bump \`curl\` to 8.12.1`
- `Update Boost from 1.83 to 1.90`
- `Update librdkafka to fix lock-order-inversion in queue refcount`

### 13. Open a draft PR (interactive mode only)

Before creating the PR, make sure every required branch has been pushed with the user's confirmation: the
ClickHouse branch, any `ClickHouse/<version>` branch in a contrib fork (opened as a PR against that fork's base
branch first), and any `rust_vendor` branch.

Use the PR template at `.github/PULL_REQUEST_TEMPLATE.md`. Keep the description short and structured: one sentence
of what and why; bullets for CVEs with their applicability; bullets for the ClickHouse-side changes; bullets for
user-visible behaviour changes (a stored-format change is a compatibility warning); for forks, which patches are
carried. Never enumerate what did not change. Changelog category for contrib bumps is
`Build/Testing/Packaging Improvement` with the entry ``Update `<lib>` to <version>.``; use `Bug Fix` only when the
PR exists to fix a specific user-visible bug.

```bash
gh pr create --draft --title "<title>" --body-file <body.md>
```

## Libraries with known dependency chains

Some libraries require co-bumping dependencies. `contrib/CMakeLists.txt` documents them in `# requires:` comments
(the list below reflects it as of 2026-09; check the file, it is the source of truth):

- `arrow` requires: `snappy`, `thrift`, `double-conversion`, `xsimd` (and regenerates its flatbuffers bindings)
- `avro` requires: `snappy`
- `AMQP-CPP` requires: `libuv`
- `cassandra` requires: `libuv`
- `librdkafka` requires: `libgsasl`
- `libhdfs3` requires: `google-protobuf`, `krb5`, `isa-l`
- `hive-metastore` requires: `thrift`, `avro`, `arrow`, `libhdfs3`
- `rocksdb` requires: `jemalloc`, `snappy`, `zlib`, `lz4`, `zstd`, `liburing`
- `mongo-c-driver` requires: `zlib`; `mongo-cxx-driver` requires `mongo-c-driver` (`libmongoc`, `libbson`) at a
  minimum version stated in its `CMakeLists.txt` — bump both in one PR when the floor moves
- `usearch` requires: `FP16`, `SimSIMD`
- `substrait` requires: `google-protobuf`
- `google-protobuf` and `grpc` are coupled (gRPC bundles its own `upb`; both copies must not end up in one binary)
- AWS SDK: `aws`, `aws-c-auth`, `aws-c-cal`, `aws-c-common`, `aws-c-compression`, `aws-c-event-stream`,
  `aws-c-http`, `aws-c-io`, `aws-c-mqtt`, `aws-c-s3`, `aws-c-sdkutils`, `aws-checksums`, `aws-crt-cpp`

If the target library appears in a chain, check whether its dependencies also need updating.

## CI Validation

After pushing, CI will automatically:
- Apply the `submodule changed` label
- Run `check_submodules.sh`: every `.gitmodules` entry has a directory, URLs start with `https://github.com/`,
  submodule name equals its path, no recursive submodules (no `[submodule` entries inside submodules)
- Run `check_cpp.sh`: `contrib/*-cmake/` must not use the forbidden CMake patterns listed in step 8
- Build on multiple platforms (x86, ARM, macOS, FreeBSD, loongarch64, s390x, riscv64) and sanitizer configurations
  (ASan, MSan, TSan, UBSan), plus clang-tidy with dummy launchers

## Notes

- Do not use rebase or amend — add new commits
- Do not commit to the master branch — always use a feature branch
- Use Allman-style braces in any C++ code changes
- The `.gitmodules` file must not use `branch = ...` tags
- Deleted upstream CMake files in `git -C contrib/<lib> status` are expected (see Background)
