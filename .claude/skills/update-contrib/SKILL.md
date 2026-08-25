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

## Background

ClickHouse vendors all third-party libraries as git submodules under `contrib/`. Each library has:
- `contrib/<lib>/` — the submodule (upstream repo or a ClickHouse fork)
- `contrib/<lib>-cmake/` — ClickHouse's own CMake build files (upstream CMake is deleted after checkout)

Submodule URLs point to either upstream directly (unpatched) or a fork at `github.com/ClickHouse/<lib>` (when patches are needed). Forks use branches named `ClickHouse/<version>` — never `ClickHouse/master` or `ClickHouse/main`.

Some contrib libraries are built through Rust rather than through `contrib/<lib>-cmake/`. In that case, the important integration points are typically:
- `rust/workspace/<lib>/Cargo.toml`
- `rust/workspace/<lib>/CMakeLists.txt`
- `rust/workspace/Cargo.lock`
- `contrib/rust_vendor/`

For these libraries, updating the submodule often requires regenerating `rust/workspace/Cargo.lock`, re-vendoring crates with `rust/vendor.sh`, and verifying the Rust/C++ bridge build rather than editing `contrib/<lib>-cmake/`.

## Process

### 1. Gather information about the library

Throughout this skill, `$LIB` refers to the library name passed as `$0` and `$VERSION` to the target version once resolved (step 2). Both must match `[A-Za-z0-9._-]+` so they are safe to use unquoted in paths and branch names; reject anything else.

Extract the submodule URL, current commit, and whether it uses a fork:

```bash
# Submodule URL
git config --file .gitmodules --get "submodule.contrib/$LIB.url"

# Current pinned commit
git -C "contrib/$LIB" rev-parse HEAD

# Current version (nearest tag)
git -C "contrib/$LIB" describe --tags --abbrev=0 2>/dev/null || echo "no tags"
```

Determine whether the URL points to a ClickHouse fork (`github.com/ClickHouse/...`) or upstream directly.

Check if a `contrib/<lib>-cmake/` directory exists:

```bash
ls contrib/${LIB}-cmake/CMakeLists.txt 2>/dev/null
```

Also check whether the library is integrated through the Rust workspace:

```bash
ls rust/workspace/${LIB}/Cargo.toml rust/workspace/${LIB}/CMakeLists.txt 2>/dev/null
grep -R "contrib/${LIB}\|${LIB}" rust/workspace/ --include='Cargo.toml' --include='CMakeLists.txt'
```

If it is a Rust-based contrib library, inspect:
- `rust/workspace/<lib>/Cargo.toml` for dependency path, crate name, and enabled features
- `rust/workspace/<lib>/CMakeLists.txt` for generated headers, copied headers, and CMake target names
- `rust/workspace/Cargo.lock` to understand what will need to be regenerated

### 2. Determine the target version

If `$1` was provided, use it. Otherwise, find the latest release:

```bash
# For upstream repos
git -C "contrib/$LIB" fetch origin --tags
git -C "contrib/$LIB" tag -l --sort=-v:refname | head -20

# For ClickHouse forks, also check the upstream remote
```

If the library uses a ClickHouse fork, also check the upstream repo for the latest release. Use `gh release list` or `git ls-remote --tags` against the upstream URL if available.

Present the current and target versions to the user with `AskUserQuestion` for confirmation before proceeding. `VERSION` is whatever the user confirmed here — either `$1` if it was passed in, or a tag picked from the fresh upstream tag list. Verify it matches the charset above before using it in branch names or commits.

### 3. Check for ClickHouse patches (forked libraries only)

If the submodule points to a ClickHouse fork, identify existing patches that need re-applying:

```bash
# Find the ClickHouse/ branch currently tracked
git -C "contrib/$LIB" branch -r | grep 'origin/ClickHouse/'

# List ClickHouse-specific commits (not in upstream)
UPSTREAM_TAG=$(git -C "contrib/$LIB" describe --tags --abbrev=0)
git -C "contrib/$LIB" log --oneline "$UPSTREAM_TAG"..HEAD
```

Report the patches found. These will need to be cherry-picked into the new `ClickHouse/` branch after the fork is updated.

### 4. Create a branch for the update

```bash
git checkout -b "bump-${LIB}-${VERSION}"
```

### 5. Update the fork (if applicable)

If the library uses a ClickHouse fork and has patches:

1. In the fork repo (clone or use `gh` API), create a new branch `ClickHouse/<new-version>` from the upstream tag
2. Cherry-pick existing ClickHouse patches from the old `ClickHouse/<old-version>` branch
3. Drop patches that were merged upstream (check if each patch commit exists in the new version)
4. Keep the new `ClickHouse/<new-version>` branch ready locally; do not push it yet

Before doing this, check previous ClickHouse PRs for the same library to understand the expected file set, commit style, and whether earlier updates touched lockfiles, vendored dependencies, or auxiliary integration code.

Do not push any branch yet. Pushes should happen only after:
- the relevant build has been validated successfully
- any required fixes have been applied
- the user explicitly confirms that it is ok to push now

If the library has no ClickHouse patches but uses a fork, consider switching to the upstream URL directly (update `.gitmodules`).

If the fork management requires manual work on GitHub (creating branches, cherry-picking in the fork), use `AskUserQuestion` to instruct the user on what to do, then wait for confirmation before proceeding.

### 6. Update the submodule pointer

```bash
cd "contrib/$LIB"
git fetch origin
git checkout <target-version-or-branch>
cd ../..
git add "contrib/$LIB"
```

If the submodule URL changed (fork to upstream, or new fork URL), also update `.gitmodules`:

```bash
git config --file .gitmodules "submodule.contrib/$LIB.url" "<new-url>"
git add .gitmodules
```

### 7. Diff the source tree between old and new versions

This is the critical step that determines what CMake and source changes are needed:

```bash
# List files added/removed between old and new version
OLD_COMMIT=$(git diff --cached --submodule=short "contrib/$LIB" | grep -oP '^\-Subproject commit \K\w+')
NEW_COMMIT=$(git diff --cached --submodule=short "contrib/$LIB" | grep -oP '^\+Subproject commit \K\w+')

# In the submodule, diff the file trees
cd "contrib/$LIB"
git diff --stat "$OLD_COMMIT..$NEW_COMMIT"
git diff --name-status "$OLD_COMMIT..$NEW_COMMIT" -- '*.c' '*.cpp' '*.cc' '*.h' '*.hpp' 'CMakeLists.txt'
cd ../..
```

Pay attention to:
- **Added/removed source files** — CMake file list must be updated
- **Renamed files** — CMake references must follow
- **New build options or defines** — may need new CMake variables
- **Changed public headers** — may require source adaptation in `src/`

For Rust-based contrib libraries, also diff the integration surface used by ClickHouse rather than only the whole upstream tree. In particular, inspect changes under paths such as:
- `crates/c-api/`
- public headers copied into the build tree
- Cargo feature lists and crate names used by `rust/workspace/<lib>/Cargo.toml`

If the public C/C++ API changes are additive only, the update may still be low-risk even if the full upstream diff is huge.

### 8. Update build integration files

If `contrib/<lib>-cmake/CMakeLists.txt` exists, update it to reflect the new version:

1. Read the current CMake file
2. Compare source file lists against the actual files in the updated submodule
3. Add new source files, remove deleted ones
4. Check for new required defines, include paths, or compile options
5. Verify the result follows ClickHouse CMake conventions:
   - **No** `find_package`, `check_c_compiler_flag`, `check_cxx_compiler_flag`, `check_c_source_compiles`, `check_include_file`, `check_symbol_exists`, `cmake_push_check_state`, `CMAKE_REQUIRED_FLAGS`, or any `Check*` CMake modules — these are forbidden by CI style checks for hermetic, cross-compiled builds
   - Use hardcoded feature flags instead of runtime detection

If `contrib/<lib>-cmake/` has config headers (e.g., `config.h.in`, `curl_config.h`), check if they need updates for new version defines or feature flags.

If the library is integrated through Rust instead of `contrib/<lib>-cmake/`:

1. Check whether `rust/workspace/<lib>/Cargo.toml` still points to the correct path/package and whether the enabled features are still valid
2. Check whether `rust/workspace/<lib>/CMakeLists.txt` still matches the upstream public headers and feature flags
3. If the library copies headers into the build directory at CMake configure time, remember that changes to submodule headers may require re-running CMake or manually copying the updated headers into the build tree for incremental verification
4. Regenerate `rust/workspace/Cargo.lock` if dependency resolution changed
5. Re-vendor crates with `rust/vendor.sh`

Important for Rust-based contrib updates:
- `rust/vendor.sh` re-vendors dependencies for the whole Rust workspace, not just the target library
- this can produce hundreds of file changes under `contrib/rust_vendor/`
- `contrib/rust_vendor` is itself a submodule pointing at `github.com/ClickHouse/rust_vendor`. Changes under it must be committed inside that submodule on a branch named `bump-${LIB}-${VERSION}`, then the submodule pointer in ClickHouse must be bumped to the new commit. Only push the `rust_vendor` branch after the user explicitly confirms.
- do not forget to include both `rust/workspace/Cargo.lock` and the updated `contrib/rust_vendor` submodule pointer in the final commit in ClickHouse

### 9. Fix ClickHouse source code

Search for usages of the library in `src/`:

```bash
# Find includes of the library's headers
grep -r "#include.*<$LIB" src/ --include='*.h' --include='*.cpp' -l
grep -r "#include.*\"$LIB" src/ --include='*.h' --include='*.cpp' -l

# Check the library's CHANGELOG or migration guide for API changes
```

Common adaptation patterns:
- Renamed functions or classes
- Changed function signatures (new required parameters, changed return types)
- Deprecated API removal
- New required initialization calls
- Changed header paths
- Missing or mismatched feature guards in copied public headers

Build the project to find compilation errors. Redirect `ninja` output to a log file in the build directory and use a subagent to analyze the results. If a build directory does not exist yet, configure one first (see `docs/en/development/build.md` — typically `cmake -S . -B build`, or use an existing `build_*` directory):

```bash
ninja -C build > build/build_bump_${LIB}.log 2>&1
```

Fix errors iteratively, committing each logical fix separately.

### 10. Verify the build

After all fixes, verify the build succeeds:

```bash
ninja -C build > build/build_bump_${LIB}.log 2>&1
```

Use a Task subagent to analyze the build log and return a concise summary.

For Rust-based contrib libraries, first discover the actual Rust/CMake target names before building them. Do not guess target names. Use:

```bash
ninja -C <build-dir> -t targets | grep -i "$LIB"
```

Examples:
- the Rust cargo target may be something like `_cargo-build__ch_rust_<lib>` rather than `_ch_rust_<lib>`
- the final ClickHouse binary target is often `clickhouse`, not `clickhouse-server`

If a build failure mentions a copied upstream header, check whether:
- a new API addition is missing the proper feature guard in the C++ wrapper header
- the C header and C++ header disagree on which features gate a declaration
- the build directory still contains stale copied headers from before the fix

If the build fails, fix the errors and repeat. Common issues:
- Missing source files in CMake
- Changed include paths
- API changes requiring source adaptation
- Platform-specific issues (macOS, FreeBSD, cross-compilation)
- Rust vendoring or lockfile drift
- Copied headers in the build tree being stale

Only after the build is validated successfully should you ask the user whether it is ok to push now. Until the user confirms, keep both the main repo branch and any fork branch local-only.

### 11. Run relevant tests

Identify and run tests related to the library. Redirect output to a log file:

```bash
# Functional tests that might exercise the library
grep -rl "$LIB" tests/queries/0_stateless/ --include='*.sql' --include='*.sh' -l | head -20
```

### 12. Commit

Create the commit. Use the title format `Bump \`<lib>\` from <old_version> to <new_version>` for straightforward bumps, or a more descriptive title if the update has a specific motivation.

Before committing, make sure you stage all affected integration artifacts, not just the submodule pointer. In particular, for Rust-based contrib updates this usually means:

```bash
git add "contrib/$LIB" rust/workspace/Cargo.lock contrib/rust_vendor
```

Here `contrib/rust_vendor` stages the bumped submodule pointer after its own branch (`bump-${LIB}-${VERSION}` in the `rust_vendor` repo) has been committed — the individual vendored files are not tracked in the ClickHouse repo directly.

If there were build integration or source fixes, stage those too.

Commit message guidelines:
- First line: `Use \`<lib>\` <version>` or `Bump \`<lib>\` to <version>` or a description of the motivation
- Body (optional): list key changes from upstream, CVEs fixed, or motivation for the update

Examples of good commit messages from past PRs:
- `Use \`simdjson\` v4.2.4`
- `Bump \`curl\` to 8.12.1`
- `Update Boost from 1.83 to 1.90`
- `Fix CVE-2023-0286 / CVE-2023-5678`
- `Update librdkafka to fix lock-order-inversion in queue refcount`

### 13. Open a draft PR

Before creating the PR, ensure any required branches have been pushed and that the user has already confirmed pushing is ok. This includes both the ClickHouse feature branch, any `ClickHouse/<version>` branch in a contrib fork, and any `bump-${LIB}-${VERSION}` branch in `rust_vendor`.

Use the PR template at `.github/PULL_REQUEST_TEMPLATE.md` — do not inline a custom structure. Fill in:
- a short description of what was updated and why
- the Changelog category (pick one from the template); typical choices for contrib bumps are `Not for changelog`, `Bug Fix` (for CVE fixes), `Build/Testing/Packaging Improvement`, or `Improvement`
- the Changelog entry
- the Documentation checkbox

Create the PR with:

```bash
gh pr create --draft --title "<title>" --body-file .github/PULL_REQUEST_TEMPLATE.md
```

Then edit the body to fill in the description, chosen changelog category, and changelog entry.

## Libraries with known dependency chains

Some libraries require co-bumping dependencies. Check `contrib/CMakeLists.txt` for documented chains:

- `arrow` requires: `snappy`, `thrift`, `double-conversion`
- `avro` requires: `snappy`
- `amqpcpp` requires: `libuv`
- `cassandra` requires: `libuv`
- `azure` requires: `curl`
- `cyrus-sasl` requires: `krb5`
- `librdkafka` requires: `libgsasl`
- `rocksdb` requires: `jemalloc`, `snappy`, `zlib`, `lz4`, `zstd`, `liburing`
- `google-cloud-cpp` requires: `grpc`, `protobuf`, `abseil-cpp`, `nlohmann-json`, `crc32c`
- `usearch` requires: `FP16`, `SimSIMD`
- `mongo-cxx-driver` requires: `mongo-c-driver`
- AWS SDK: `aws`, `aws-c-auth`, `aws-c-cal`, `aws-c-common`, `aws-c-compression`, `aws-c-event-stream`, `aws-c-http`, `aws-c-io`, `aws-c-mqtt`, `aws-c-s3`, `aws-c-sdkutils`, `aws-checksums`, `aws-crt-cpp`

If the target library appears in a chain, check whether its dependencies also need updating.

## CI Validation

After pushing, CI will automatically:
- Apply the `submodule changed` label
- Run `check_submodules.sh` which validates:
  - All `.gitmodules` entries have corresponding directories
  - All submodule URLs start with `https://github.com/`
  - Submodule name equals its path
  - No recursive submodules (no `.gitmodules` with `[submodule` entries inside submodules)
- Run `check_cpp.sh` which validates `contrib/*-cmake/` directories don't use forbidden CMake patterns
- Build on multiple platforms (x86, ARM, macOS, FreeBSD) and sanitizer configurations (ASan, MSan)

## Notes

- Do not use rebase or amend — add new commits
- Do not commit to the master branch — always use a feature branch
- Use Allman-style braces in any C++ code changes
- When building, redirect output to a log file and use a subagent to analyze it
- `contrib/update-submodules.sh` deletes upstream CMake files after checkout — ClickHouse relies exclusively on its own `*-cmake/` files
- The `.gitmodules` file must not use `branch = ...` tags
- Iterative "Fix build" / "Fix darwin" / "Fix MSan" commits are normal and expected
