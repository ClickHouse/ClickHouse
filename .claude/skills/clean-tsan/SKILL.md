---
name: clean-tsan
description: Detect and fix ThreadSanitizer errors (data races, deadlocks, thread leaks, etc.) by building with TSan, running a specified test, analyzing alerts, and applying fixes iteratively.
argument-hint: <test_name>
disable-model-invocation: false
allowed-tools: Task, TaskOutput, Bash(ninja *), Bash(cd *), Bash(ls *), Bash(find *), Bash(pgrep *), Bash(ps *), Bash(pkill *), Bash(mktemp *), Bash(sleep *), Bash(python *), Bash(python3 *), Bash(export *), Bash(mkdir *), Bash(tail *), Bash(grep *), Bash(wc *), Bash(./tests/clickhouse-test *), Read, Grep, Glob, Edit, Write, AskUserQuestion
---

# Clean TSan Skill

Systematically detect and fix ThreadSanitizer (TSan) errors by running a specified test, extracting alerts, performing root cause analysis via isolated subagents, applying fixes, and iterating until clean. Handles all TSan error types: data races, lock-order-inversions (potential deadlocks), thread leaks, destroy of locked mutex, unlock of unlocked mutex, and signal-unsafe calls. Alerts are processed one at a time — the stack traces guide which source files to analyze.

## Arguments

- `$ARGUMENTS` (required): Test name or filter. The skill auto-detects the test type:
  - `test_something` → integration test (checked against `tests/integration/`)
  - `01234_some_test` or a number → stateless test (checked against `tests/queries/0_stateless/`)
  - Anything else → gtest filter for unit tests (e.g., `Scheduler`, `MergeTree`)

## Workflow Overview

1. **Setup** — Detect test type from the argument
2. **Build** — Incremental TSan build
3. **Test** — Run tests to provoke TSan errors
4. **Extract** — Extract first TSan alert from logs, save artifact
5. **RCA** — Root cause analysis with threading analysis (isolated subagent), save artifact
6. **Fix** — Apply fix based on RCA
7. **Verify** — Rebuild, retest, update progress, loop back to step 4 if more errors remain

---

## Phase 1: Setup

### 1a. Validate arguments

`$ARGUMENTS` is the test name. If empty, use `AskUserQuestion`:

**Question: "Which test should be used to find TSan errors?"**
- Option 1: "Unit test (gtest)" — Description: "A gtest filter, e.g. `Scheduler` or `MergeTree`"
- Option 2: "Integration test" — Description: "An integration test name, e.g. `test_replicated_merge_tree`"
- Option 3: "Stateless test" — Description: "A stateless test, e.g. `01234_some_test` or just `01234`"

### 1b. Auto-detect test type

Determine the test type from the test name:

1. Check if `tests/integration/$ARGUMENTS/` exists → **integration test**
2. Check if files matching `tests/queries/0_stateless/$ARGUMENTS*` exist → **stateless test**
3. Otherwise → **unit test** (treat argument as a gtest filter)

If auto-detection is ambiguous, confirm with `AskUserQuestion`.

Store the test type and test selector.

---

## Phase 2: TSan Build

**IMPORTANT:** The `build_tsan` directory must already be configured with CMake (`-DSANITIZE=thread`). This skill does NOT run cmake. All commands assume CWD is the repository root.

### 2a. Create log file

```bash
mktemp /tmp/tsan_build_XXXXXX.log
```

**IMMEDIATELY** report to the user:
- "TSan build logs: `<log_file>`"
- Provide copyable command:
  ```bash
  tail -f <log_file>
  ```

### 2b. Determine build target

- Unit tests → `unit_tests_dbms`
- Integration tests or Stateless tests → `clickhouse`

### 2c. Start build

```bash
cd build_tsan && ninja <target> > <log_file> 2>&1
```

Run with `run_in_background: true`.

Report: "TSan build started in background. Waiting for completion..."

---

## Phase 3: Run Tests

### 3a. Wait for build

Use `TaskOutput` with `block=true` to wait for the background build from Phase 2.

Check the **exit code** of the build:
- **Exit code 0**: Build succeeded. Report "Build succeeded." and proceed.
- **Non-zero exit code**: Build failed. Read the last 50 lines of the build log with Read tool to show the error. Do NOT use a subagent for this — just read the tail directly. Use `AskUserQuestion` to ask if the user wants to investigate.

### 3b. Run tests

Create a test log file:
```bash
mktemp /tmp/tsan_test_XXXXXX.log
```

Report log path and `tail -f` command to user.

#### Unit Tests

Use `--gtest_repeat` to run multiple iterations in a single process (avoids startup overhead).
`halt_on_error=1` stops the process immediately on the first TSan alert — without it the test keeps running and generates cascading failures that obscure the root alert.

```bash
TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1 history_size=7" \
  ./build_tsan/src/unit_tests_dbms \
  --gtest_filter="*<test_name>*" \
  --gtest_repeat=20 \
  --gtest_break_on_failure \
  > <log_file> 2>&1
```

`--gtest_break_on_failure` stops GTest on the first test assertion failure (separate from TSan).
`--gtest_repeat=20` keeps repeating until the first TSan hit — combined with `halt_on_error=1` the process aborts immediately when a race is found, so a larger repeat count costs nothing if a race is hit early.

TSan output goes directly to the log file (stdout/stderr of the binary). With `halt_on_error=1`, TSan calls `abort()` on error — the exit code will be non-zero (typically 134 for SIGABRT), not the default 66. Check `$?` non-zero as a fast indicator before grepping logs.

#### Integration Tests

First clean up any old instance directories to avoid stale logs:
```bash
sudo rm -rf tests/integration/<test_name>/_instances*
```

Then run:
```bash
python3 -u -m ci.praktika run "integration" --test <test_name> --path <absolute_path_to_repo>/build_tsan/programs/clickhouse > <log_file> 2>&1
```

**IMPORTANT:** The `--path` must be an **absolute path** — relative paths break inside docker containers.

**Checking results:** Do NOT grep the praktika runner output for TSan alerts — it truncates them. Instead, just check if the test passed or failed (exit code). TSan alerts are in the **instance stderr logs**.

**IMPORTANT:** When TSan detects an error, it kills the server process immediately. All subsequent test cases in that run will fail with "Connection refused" errors — this is expected and not a separate problem. Only the first TSan alert matters.

Instance directories are created by docker and are **root-owned**. Make them readable immediately after the test finishes:

```bash
sudo chmod -R o+rX tests/integration/<test_name>/_instances*/
```

The directory suffix varies by pytest-xdist worker (e.g., `_instances-gw0`, `_instances-gw1`). Each test may have multiple nodes (e.g., `node`, `node1`, `keeper1`, etc.):

- **Server nodes** have `<node>/logs/stderr.log` (TSan alerts), `clickhouse-server.log` (trace-level, useful for RCA), `clickhouse-server.err.log`
- **Keeper nodes** have `<keeper>/log/clickhouse-keeper.err.log` (TSan alerts may appear here), `clickhouse-keeper.log`

Since log file naming varies by node type, search ALL log files for TSan alerts:

```bash
grep -rl --include="*.log" "SUMMARY: ThreadSanitizer:" tests/integration/<test_name>/_instances*/
```

The `--include="*.log"` avoids searching large data files and speeds up the search significantly.

#### Stateless Tests

First verify a TSan-instrumented server is running. If not, instruct user to start the server with stderr captured to a known file:

```bash
mktemp /tmp/tsan_server_stderr_XXXXXX.log
```

```bash
./build_tsan/programs/clickhouse server --config-file ./programs/server/config.xml 2>/tmp/tsan_server_stderr_XXXXXX.log &
```

Store this server stderr log path — it is where **server-side** TSan alerts will appear.

Then run the test:
```bash
export PATH="./build_tsan/programs:$PATH" && ./tests/clickhouse-test <test_name> > <log_file> 2>&1
```

**TSan alerts for stateless tests come from two sources:**
1. **Client-side alerts**: The clickhouse-test runner collects these from `<test>.stderr-fatal*` files and includes them in the test output (captured in `<log_file>`).
2. **Server-side alerts**: These go to the server's stderr, which we redirected to the server stderr log file above.

Both files must be checked for TSan errors.

**Run all tests in background** with `run_in_background: true`. Wait with `TaskOutput(block=true)`.

**Iteration strategy by test type:**
- **Unit tests**: Use `--gtest_repeat=20` (single process, multiple iterations — fast; `halt_on_error=1` aborts immediately on first hit so a larger count costs nothing when a race is found early).
- **Integration tests**: Run **once**. Integration tests are slow under TSan and a single run is usually sufficient to trigger errors. If the first run is clean, optionally run once more to confirm.
- **Stateless tests**: Run once. If clean, optionally rerun for confidence.

### 3c. Check for TSan errors

**Unit tests:** Check exit code (non-zero = error; `halt_on_error=1` aborts with SIGABRT rather than exit 66) and grep the log:
```bash
grep -c "SUMMARY: ThreadSanitizer:" <log_file>
```

**Integration tests:** Check if the test passed or failed (exit code). If failed, search instance stderr logs as described in 3b above.

**Stateless tests:** Check both the test output and server stderr:
```bash
grep -c "SUMMARY: ThreadSanitizer:" <log_file>
grep -c "SUMMARY: ThreadSanitizer:" <server_stderr_log>
```

- If errors found: report "TSan errors detected! (N alerts)" and proceed to Phase 4
- If no errors: report "No TSan errors detected. The test appears clean." and stop

---

## Phase 4: Extract First TSan Alert

The skill processes alerts **one at a time**: extract the first alert, analyze it, fix it, rerun tests, and repeat until clean.

### Direct extraction using markers

TSan alerts have clear boundaries:
- **Start marker**: a line containing `WARNING: ThreadSanitizer:`
- **End marker**: a line containing `SUMMARY: ThreadSanitizer:`

Extract the first alert directly from the log file(s) using Grep or Bash without a subagent. This avoids feeding huge log files into an agent's context.

**For each log file containing TSan errors:**

1. Count total alerts:
   ```bash
   grep -c "SUMMARY: ThreadSanitizer:" <log_file>
   ```

2. Extract the first complete alert — find line numbers of the start and end markers:
   ```bash
   grep -n -E "WARNING: ThreadSanitizer:|SUMMARY: ThreadSanitizer:" <log_file> | head -2
   ```
   This gives the line numbers of the start and end markers. Then use Read tool with `offset` and `limit` to extract those lines.

### Save extracted alert

Create the artifact directory (once per skill invocation) and save the alert:

```bash
mkdir -p _clean-tsan/<test_name>
```

Store the directory path for reuse. Save the alert text:

```
Write alert to: <artifact_dir>/alert-NNN.txt
```

Where NNN is the iteration number (001, 002, ...). If the directory already has artifacts from a previous session, continue numbering from the last NNN.

### Progress file format

The file `<artifact_dir>/progress.md` is the shared memory across subagents — every RCA and Fix subagent reads it before starting work. Create it on the first iteration; entries are written in Phase 7c after each verify cycle.

Each entry is compact (~5 lines):

```markdown
## Iteration NNN: <OUTCOME>
- **Alert:** <type> (<key identifiers: mutex names, variable names>)
- **Root cause:** <one line>
- **Fix:** <what was changed>
- **Files:** <list of modified files>
```

`<OUTCOME>` is one of:
- `FIXED` — the alert disappeared after the fix
- `FAILED` — the same or equivalent alert reappeared after the fix (add `- **Why it failed:** <explanation>`)
- `NEW_ALERT` — a different alert appeared (the fix worked, new issue found)

### Threading model file

The file `<artifact_dir>/threading-model.md` is a cumulative knowledge base about the threading architecture of the code touched during the session. Unlike `progress.md` (which tracks what was tried), this captures the *understanding* of how the code works.

RCA subagents read it before analysis and **update it** with new discoveries. Fix subagents read it and **update it** when their changes alter invariants.

**On the first iteration** (when `<artifact_dir>/threading-model.md` does not exist): create the file with the following self-describing header. This header instructs future subagents on how to maintain the document — it lives in the file itself rather than in subagent prompts.

```markdown
# Threading Model

> **Format guide for `/clean-tsan` subagents:** This file is maintained by RCA and Fix subagents
> across iterations. Read it before analysis to understand what is already known. Update it with
> new discoveries. When a fix changes an invariant, update the relevant entry rather than appending.
> If an earlier entry was wrong, correct it.
>
> **Per-class structure:** Add a `## ClassName` section for each class involved in TSan alerts.
> Under each class, use these subsections as needed (omit empty ones):
>
> - **Mutexes**: mutex name and type, what fields/state it guards, which threads acquire it
> - **Lock Ordering**: which mutex must be acquired before which (e.g., `MutexA` → `MutexB`
>   means A is always acquired before B — never the reverse)
> - **Cross-Class Calls Under Lock**: method holds mutex X while calling into another class
>   (which acquires mutex Y)
> - **Atomics**: field name, type, access semantics
> - **Thread Roles**: which named thread types call which entry-point methods
> - **Condition Variables**: name, wait predicate, who signals it
> - **Known Safe Patterns**: things that look like races but are provably safe (and why)
>
> **Conventions:** Use actual mutex names from the code (e.g., `AllocationQueue::mutex`),
> not abstract labels. One line per item. Keep entries factual — no speculation.
```

On subsequent iterations the file already exists — RCA and Fix subagents will update it in place.

---

## Phase 5: Root Cause Analysis

Launch a Task with `subagent_type=general-purpose` for the extracted alert. Using a separate subagent prevents context pollution. The subagent reads the source files referenced in the stack traces and performs both threading analysis and root cause analysis in one pass.

**Before launching**, prepare the context:
1. Read `<artifact_dir>/progress.md` (if it exists — skip on first iteration)
2. Read `<artifact_dir>/threading-model.md` — always present (created in Phase 4 with format guide)
3. Generate the diff of all uncommitted changes: `git diff`

If a previous iteration was marked `FAILED` in `progress.md`, add to the prompt: **"IMPORTANT: A previous fix attempt for a similar alert failed. Read the progress file carefully and think deeper about the root cause. The obvious fix did not work — consider less obvious causes such as lock ordering across call chains, re-entrant paths, or indirect mutex acquisitions through callbacks."**

```
Prompt: "Perform root cause analysis of this ThreadSanitizer alert in ClickHouse.

## TSan Alert
<full alert text>

## Previous Progress
<contents of progress.md, or "First iteration — no previous context.">

## Threading Model
<contents of threading-model.md — the file header describes the format and update conventions; class sections below it are the current knowledge base. If only the header exists, no analysis has been recorded yet.>

## Changes Already Applied
<git diff output, or "No changes yet.">

## Instructions

**IMPORTANT:** The stack traces reference line numbers in the CURRENT working tree. Read source files directly — do not assume line numbers from previous iterations are still valid.

### Step 1: Identify source files from the stack traces
Extract all source file paths and line numbers from the `DB::` stack frames in the alert. These are the files you need to read.

### Step 2: Read source files and analyze threading
Read each source file. For the classes/structs involved in the racing accesses, analyze:
- Member fields: classify each as atomic, mutex-protected (`TSA_GUARDED_BY`), pointer-guarded (`TSA_PT_GUARDED_BY`), unprotected, or immutable
- Synchronization primitives: mutexes, their types, what they guard, lock ordering (`TSA_ACQUIRED_AFTER`)
- Thread access patterns: which threads call the methods in the stack traces

Use the Threading Model section above as a starting point — it may already have relevant information from previous iterations. Verify it against the current source code.

### Step 3: Root cause analysis (varies by error type)

#### For data race alerts:
1. Identify the two racing accesses: which threads, what memory location, read-write or write-write?
2. Map stack frames to exact source lines.
3. Determine why the access is unprotected:
   - Missing lock acquisition
   - Wrong mutex (field guarded by A but code locks B)
   - Lock released too early (TOCTOU)
   - Atomic mixed with non-atomic access to same field
   - Field assumed single-threaded but actually shared
   - Race in init/destruction sequence
4. Propose fix: add lock, make field atomic, extend lock scope, add TSA annotation, etc.

#### For lock-order-inversion (potential deadlock) alerts:
1. Identify the mutex cycle: which mutexes are acquired in inconsistent order?
2. Map each acquisition to exact source lines.
3. Determine why the ordering is inconsistent:
   - Two code paths acquire the same pair of mutexes in opposite order
   - Callback or virtual method call while holding a lock acquires another lock
   - Nested lock acquisition without documented ordering
4. Propose fix: enforce consistent lock ordering, reduce lock scope, use `std::lock` for simultaneous acquisition, add `TSA_ACQUIRED_AFTER` annotations, restructure to avoid nested locking.

#### For thread leak alerts:
1. Identify the leaked thread: where was it created, why wasn't it joined?
2. Map creation stack to source.
3. Determine root cause: missing join, early return skipping cleanup, exception path missing join.
4. Propose fix: add join/detach in destructor, use RAII thread wrapper, fix cleanup path.

#### For destroy of locked mutex / unlock of unlocked mutex:
1. Identify the problematic mutex operation and where it occurs.
2. Map stack to source.
3. Determine root cause: destructor called while lock held, double-unlock, unlock without matching lock.
4. Propose fix: ensure proper lock lifecycle, fix unlock/lock pairing.

#### For signal-unsafe call alerts:
1. Identify which function was called from a signal handler and why it's unsafe.
2. Map stack to source.
3. Propose fix: use only async-signal-safe functions in signal handlers, defer work to main thread.

## ClickHouse Threading Context

Key threading concepts:
- `GlobalThreadPool`: singleton for all thread creation (`src/Common/ThreadPool.h`)
- `ThreadFromGlobalPool`: wraps std::thread, used throughout codebase
- `BackgroundSchedulePool`: periodic task scheduler with TSA annotations (`src/Core/BackgroundSchedulePool.h`)
- `MergeTreeBackgroundExecutor`: preemptable merge/mutation tasks (`src/Storages/MergeTree/MergeTreeBackgroundExecutor.h`)
- `ThreadStatus`: per-thread state; `ThreadGroup` for query-level grouping (`src/Common/ThreadStatus.h`)
- `ConcurrencyControl`: CPU slot scheduling (`src/Common/ConcurrencyControl.h`)
- Thread names (15 byte limit): QueryPipelineEx, MergeMutate, BgSchPool, AsyncMetrics, etc.
- Common sync patterns: `std::mutex` + `TSA_GUARDED_BY`, `SharedMutex`, `std::atomic`
- Per-thread access via `CurrentThread::get`
- TSA macros in `base/base/defines.h`:
  - `TSA_GUARDED_BY(mutex)` — field protected by mutex
  - `TSA_PT_GUARDED_BY(mutex)` — pointer-to-data protected by mutex
  - `TSA_REQUIRES(mutex)` — function requires mutex held
  - `TSA_REQUIRES_SHARED(mutex)` — function requires shared lock
  - `TSA_NO_THREAD_SAFETY_ANALYSIS` — suppress TSA checks (may hide real issues)
  - `TSA_ACQUIRED_AFTER(mutex)` — lock ordering

## Output Format

### Alert Analysis
**Error Type:** <type>
**Threads/Mutexes Involved:** <description>
**Source Locations:** <file:line for each relevant stack frame>
**Field/Variable/Mutex:** <name of what's affected>
**Current Protection:** <what protection exists, if any>
**Root Cause:** <detailed explanation>
**Proposed Fix:**
```cpp
// Before (problematic code)
...
// After (fixed code)
...
```
**Fix Explanation:** <why this fix is correct and doesn't introduce new issues>
**TSA Annotations to Add:** <any TSA_GUARDED_BY, TSA_ACQUIRED_AFTER, etc.>

### Threading Model Update
Write updated `threading-model.md` content for the classes involved in this alert. The file's own header (in the `>` blockquote at the top) describes the required format, conventions, and update rules — follow it exactly. Only include classes involved in the alert."
```

### Save RCA report and update threading model

Save the subagent's RCA output to the artifact directory (created in Phase 4):

```
Write RCA to: <artifact_dir>/rca-NNN.txt
```

Where NNN matches the alert number from Phase 4.

Update `<artifact_dir>/threading-model.md` with the Threading Model Update section from the subagent's output.

Present the RCA summary to the user.

---

## Phase 6: Apply Fix

### 6a. Ask user how to proceed

Present the RCA summary and use `AskUserQuestion`:

**Question: "How would you like to proceed with the proposed fix?"**

| Option | Description |
|--------|-------------|
| Apply the fix | Apply the proposed fix automatically |
| Only save report | Save the RCA report without applying the fix |
| Stop | Stop the skill, keep all reports generated so far |

### 6b. Apply fix

If user chose "Apply the fix":

Launch Task with `subagent_type=general-purpose`:

```
Prompt: "Apply the following TSan fix to the ClickHouse codebase.

## Fix Details
<RCA report for this alert>

## Previous Progress
<contents of progress.md, or "First iteration — no previous context.">

## Threading Model
<contents of threading-model.md — the file header describes the format and update conventions; class sections below it are the current knowledge base.>

## Changes Already Applied
<git diff output, or "No changes yet.">

## Instructions
1. Read the target source files — use the CURRENT file contents, not cached versions. Do NOT commit changes.
2. Apply the proposed code change using Edit tool
3. If adding TSA annotations, use macros from `base/base/defines.h`
4. If adding includes, place in alphabetical order within their group
5. Follow ClickHouse code style:
   - Allman-style braces (opening brace on a new line)
   - Never use sleep to fix race conditions
   - Prefer `std::lock_guard` or `std::unique_lock` for RAII locking
   - Use TSA annotations when adding mutex protection
6. If your fix changes threading invariants (e.g., a method no longer holds a lock when calling another method), report what `threading-model.md` entries need updating — follow the format described in that file's header
7. Report what was changed"
```

After the fix is applied:
1. Show the diff to the user for review
2. Update `<artifact_dir>/threading-model.md` if the fix subagent reported changed invariants

---

## Phase 7: Verify and Iterate

### 7a. Rebuild

Repeat Phase 2 (incremental TSan build in background).

### 7b. Rerun tests

Repeat Phase 3 (run tests).

### 7c. Update progress

After checking results, write the full entry for the current iteration to `<artifact_dir>/progress.md` using the format from Phase 4. The entry includes:
- **Alert** type and key identifiers (from Phase 4 extraction)
- **Root cause** summary (from Phase 5 RCA)
- **Fix** applied (from Phase 6)
- **Files** modified
- **Outcome** based on the verify results:
  - Same/equivalent alert reappeared → `FAILED`, add `- **Why it failed:** <explanation>`
  - Different alert appeared → `NEW_ALERT` (the fix worked, new issue found)
  - No TSan errors → `FIXED`

### 7d. Check results and loop

- **If no TSan errors:** report "All TSan errors resolved! The test is clean." and stop.
- **If TSan errors remain:** extract the first alert (Phase 4), perform RCA (Phase 5), present fix (Phase 6), and loop back here.

Each iteration processes one alert at a time. The loop continues until either:
- No more TSan errors are detected
- The user chooses "Stop" in Phase 6

---

## Error Handling

### Build failure
- Report the specific error from build log
- Ask user if they want to investigate or fix manually
- Do NOT proceed to testing

### No test binary found
- Unit tests: binary at `build_tsan/src/unit_tests_dbms`. Rebuild with target `unit_tests_dbms`.
- Integration/stateless: binary at `build_tsan/programs/clickhouse`.

### Test timeout
- Integration tests: use 600000ms timeout (tests may take >10 minutes)
- Unit tests: use 300000ms timeout
- If test hangs, kill and report

### Empty test results
- Verify test selector matches existing tests
- For gtest: check `--gtest_filter` matches
- For integration: verify test directory exists under `tests/integration/`

---

## ClickHouse Threading Reference

### Thread Pool Hierarchy

- **`GlobalThreadPool`** (`src/Common/ThreadPool.h`): Singleton managing all thread creation. Configured via `max_thread_pool_size`, `max_thread_pool_free_size`, `thread_pool_queue_size`.
- **`ThreadFromGlobalPool`**: Wraps `std::thread` but pulls from the global pool. Used throughout the codebase instead of raw `std::thread`.
- **`IOThreadPool`**: Separate pool for I/O-bound jobs to avoid starving query execution.
- **`BackupsIOThreadPool`**: Dedicated pool for backup operations.
- **`BackgroundSchedulePool`** (`src/Core/BackgroundSchedulePool.h`): Periodic task scheduler. Guarantees a task does not run simultaneously from multiple workers. Uses `TSA_GUARDED_BY(tasks_mutex)` extensively.
- **`MergeTreeBackgroundExecutor`** (`src/Storages/MergeTree/MergeTreeBackgroundExecutor.h`): Preemptable tasks for merges, mutations, fetches. Tasks implement `IExecutableTask` with `executeStep` method.

### Per-Thread State

- **`ThreadStatus`** (`src/Common/ThreadStatus.h`): Thread-local object holding thread ID, performance counters, memory tracker, query context.
- **`ThreadGroup`**: Logical grouping for threads in a single operation. Created via `createForQuery`, `createForMergeMutate`, etc.
- **`CurrentThread`** (`src/Common/CurrentThread.h`): Static accessor for per-thread state. No parameter passing needed.
- **`ConcurrencyControl`** (`src/Common/ConcurrencyControl.h`): Global CPU slot scheduling across competing queries.

### Common Thread Names (15-byte limit)

QueryPipelineEx, QueryPullPipeEx, QueryPushPipeEx, MergeMutate, MERGETREE_FETCH, MERGETREE_MOVE, BgSchPool, BgBufSchPool, BgDistSchPool, AsyncMetrics, ConfigReloader, ZooKeeperRecv, ZooKeeperSend, TCPHandler, HTTPHandler, MySQLHandler, AsyncInsertQue, AsyncLogger.

### TSA Macros (`base/base/defines.h`)

```cpp
TSA_GUARDED_BY(mutex)              // Field protected by mutex
TSA_PT_GUARDED_BY(mutex)           // Pointer-to-data protected by mutex
TSA_REQUIRES(mutex)                // Function requires exclusive lock
TSA_REQUIRES_SHARED(mutex)         // Function requires shared lock
TSA_NO_THREAD_SAFETY_ANALYSIS      // Suppress TSA checks (use sparingly!)
TSA_ACQUIRED_AFTER(mutex)          // Lock ordering constraint
TSA_CAPABILITY("mutex")            // Declare a capability type
```

### TSan Alert Locations

| Test Type | TSan Output Location |
|-----------|---------------------|
| Unit tests | Direct stdout/stderr of the test binary (captured in log file) |
| Integration tests | `tests/integration/<test>/_instances*/` — use `grep -rl --include="*.log"` to find files with alerts (run `sudo chmod` first) |
| Stateless tests (client) | Test runner output (collects from `<test>.stderr-fatal*` files) |
| Stateless tests (server) | Server process stderr (must be redirected to a file at startup) |

## File Naming Conventions

| Artifact | Path |
|----------|------|
| Build logs | `/tmp/tsan_build_XXXXXX.log` |
| Test logs | `/tmp/tsan_test_XXXXXX.log` |
| Server stderr (stateless) | `/tmp/tsan_server_stderr_XXXXXX.log` |
| TSan alerts | `_clean-tsan/<test_name>/alert-NNN.txt` |
| RCA reports | `_clean-tsan/<test_name>/rca-NNN.txt` |
| Progress log | `_clean-tsan/<test_name>/progress.md` |
| Threading model | `_clean-tsan/<test_name>/threading-model.md` |

---

## Examples

- `/clean-tsan Scheduler` — unit test (gtest filter `*Scheduler*`)
- `/clean-tsan test_replicated_merge_tree` — integration test
- `/clean-tsan 01234_some_test` — stateless test

---

## Important Notes

- **All long-running operations** (build, tests) MUST run in background with `run_in_background: true`
- **Use Task subagents** for ALL analysis to avoid polluting main conversation context
- **Each RCA gets its own subagent** to prevent cross-contamination between alerts
- **Stack traces guide analysis** — the RCA subagent reads source files referenced in the alert, no upfront class search needed
- The `build_tsan` directory must already be configured with CMake. Do NOT run cmake.
- TSan slows execution ~5-15x. Tests take much longer than usual.
- `THREAD_FUZZER_*` env vars can help provoke races but are not used by default. Mention to user if races are hard to reproduce.
- Integration tests set `TSAN_OPTIONS="use_sigaltstack=0"` by default.
- When writing fixes: Allman braces, no sleep for race conditions, use TSA annotations.
- **Do NOT commit changes.** All fixes stay as uncommitted working tree changes. The user will commit when ready.
- Say "TSan" not "TSAN" in comments and messages.
