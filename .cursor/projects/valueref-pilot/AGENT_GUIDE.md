# Agent Guide — how to work on the ValueRef pilot

Read this in full at the start of every session. It is the operating manual. It is written to be
executable by an agent running in the **Cursor cloud UI** or **Claude Code on a Linux console**;
the workflow is identical because all state lives in git.

## 0. Orient yourself (every session, in order)

1. Read [`README.md`](./README.md) for the one-paragraph framing.
2. Read the newest entry in [`PROGRESS_LOG.md`](./PROGRESS_LOG.md) — this is "where we left off".
3. Read [`TASKS.md`](./TASKS.md) — pick the next `TODO`/`IN PROGRESS` task.
4. Skim [`DESIGN.md`](./DESIGN.md) so your change matches the intended `ValueRef` shape.
5. Consult [`INVESTIGATION.md`](./INVESTIGATION.md) when you need background on a specific call
   site (it has file:line references gathered during the initial survey).

## 1. Golden rules (inherited from the repo `AGENTS.md`)

- **Never commit to `master`.** Create a branch `cursor/<descriptive-name>-f766` for each unit of
  work. (The `-f766` suffix and `cursor/` prefix are required by the cloud-agent branch policy.
  A human working directly may use any branch name, but keep PRs targeting `master`.)
- **Never rebase or amend** on a shared branch — add new commits instead.
- **No stacked PRs.** Every PR targets `master` directly. If work B depends on unmerged work A,
  either wait for A to merge or fold both into one PR.
- **One logical change per commit.** Descriptive messages.
- **C++ style:** Allman braces (opening brace on its own line). Enforced by CI style check.
- **Never use `sleep` in C++** to paper over races.
- **Avoid fallback paths** that silently hide errors; let errors propagate.
- Wrap literal SQL/type/class/function names in backticks in prose and commit messages; write a
  function as `f`, not `f()`, when referring to the function itself.
- Put temporary files (logs, downloads) in a `tmp/` subdirectory of the repo, never `/tmp`.

## 2. Build & test commands

The environment may still be finishing setup. Before building/testing, check
`/tmp/cursor/async-install/install-user.status` (contains exit code once done) — see the repo
rules on waiting for background setup.

Builds live in `build_*` directories (e.g. `build`, `build_debug`, `build_asan`). Create one if
absent. **Do not pass `-j` to ninja and do not use `nproc`** — let ninja decide.

```bash
# Configure a build (example: default). Only needed once per build dir.
cmake -B build -S . -G Ninja

# Build a single target and ALWAYS redirect to a log in the build dir.
ninja -C build unit_tests_dbms > build/build_unit_tests_dbms.log 2>&1
# Then have a SUBAGENT read the log and return a concise summary (per repo rules).
```

Unit tests (gtests) are the primary safety net for this project — there is already
`src/Core/tests/gtest_field.cpp` and many `src/Columns/tests/gtest_column_*.cpp`. Add a
`gtest_value_ref.cpp` alongside them.

```bash
# Run the unit test binary, redirecting to a uniquely named log in the build dir.
./build/src/unit_tests_dbms --gtest_filter='*ValueRef*' > build/test_value_ref.log 2>&1
```

Stateless SQL tests: add with `./tests/queries/0_stateless/add-test <name>` (`.sql`) or
`<name>.sh`. Prefer adding new tests over extending existing ones. Do not add `no-*` tags unless
strictly necessary.

For data-structure size/layout questions (relevant when weighing `ValueRef` vs `Field`), use
`.claude/tools/cppexpr.sh`, e.g. `.claude/tools/cppexpr.sh -i Core/Field.h 'OUT(sizeof(DB::Field))'`.

## 3. The memory-update protocol (do this before ending every session)

This is the most important habit. If you skip it, the next agent loses continuity.

1. **`TASKS.md`** — update the status of tasks you touched (`TODO` → `IN PROGRESS` → `DONE`), and
   add any new tasks you discovered. Keep the "Current focus" line at the top accurate.
2. **`PROGRESS_LOG.md`** — append a new dated entry: what you did, key decisions and their
   rationale, benchmark numbers, PR/branch links, CI report URLs, and explicit "next step"
   pointers. Never rewrite old entries; this file is append-only.
3. If the design changed, revise **`DESIGN.md`** and note the change in the log.
4. Commit these memory updates (they can share the branch/PR with the code change, or go in a
   dedicated docs commit).

## 4. PR conventions

- Use `.github/PULL_REQUEST_TEMPLATE.md` as the body template: short description + motivation, the
  Changelog category (pick one), the Changelog entry, and the Documentation checkbox. Do not
  invent a custom structure.
- For an internal refactor with no user-facing behavior change, the appropriate Changelog category
  is usually **Not for changelog (changelog entry is not required)** — state that explicitly.
- Create PRs as draft by default. Link related PRs/issues with full URLs (`Related:`, `Closes:`,
  `Caused by:` on their own lines).
- Include any CI report URL the user gives you in the commit message.

## 5. Definition of done for a migration step

A call site is "migrated" when:
1. It no longer constructs a `Field` on the hot path (verified by reading the code, and where
   feasible by assembly/alloc inspection with the repo tools).
2. Behavior is identical — covered by existing tests plus a targeted unit test.
3. A microbenchmark or perf test shows no regression (and ideally an improvement) — attach numbers
   to the `PROGRESS_LOG.md` entry and the PR.
4. Memory files updated per §3.

## 6. Safety / scope guardrails

- **Do not attempt to delete `Field`.** The goal is *reduction* on hot paths. `Field` remains the
  representation for SQL literals, settings, and on-disk/on-wire formats (see `INVESTIGATION.md`
  §C). Touching those is explicitly out of scope for the pilot.
- **Do not change any on-disk or wire format** (`partition.dat`, `minmax_*.idx`, skip-index
  `.idx2`, statistics files, `writeFieldBinary` encodings, partition-ID hashing). These are
  backward-compatibility surfaces.
- Prefer additive changes (introduce `ValueRef` and new overloads) over replacing existing APIs,
  so the change set stays reviewable and revertible.
