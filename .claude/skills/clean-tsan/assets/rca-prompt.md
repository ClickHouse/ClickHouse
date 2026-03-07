Perform root cause analysis of this ThreadSanitizer alert or test hang in ClickHouse.

**IMPORTANT: This is a READ-ONLY analysis task. Do NOT modify any source code files (*.cpp, *.h, etc.). Your only writable outputs are the threading model file (`threading-model.md`) and the RCA report text. A separate Fix subagent will apply source code changes based on your analysis.**

## TSan Alert or Hang Stacktraces
Read from: {{ALERT_FILE}}
(Use the Read tool to load the file contents before proceeding. This file contains either a TSan alert with stack traces, or all-thread backtraces captured from a hung process via lldb.)

{{HANG_TYPE}}

## Previous Progress
Read the progress file from: {{PROGRESS_FILE}}
(Use the Read tool. If the file does not exist, this is the first iteration — no previous context.)

## Threading Model
Read the threading model from: {{THREADING_MODEL_FILE}}
(Use the Read tool. This file accumulates threading knowledge across iterations — update it with new discoveries as part of your output.)

## Changes Already Applied
Run `git diff` to see uncommitted changes. If the output is empty, no changes have been applied yet.

## Instructions

**IMPORTANT:** The stack traces reference line numbers in the CURRENT working tree. Read source files directly — do not assume line numbers from previous iterations are still valid.

### Step 1: Identify source files from the stack traces
Extract all source file paths and line numbers from the `DB::` stack frames in the alert or stacktrace. These are the files you need to read. For lldb stacktraces, focus on frames containing `unit_tests_dbms` with source file references (ignore libc, sanitizer, and std library frames).

### Step 2: Read source files and analyze threading
Read each source file. For the classes/structs involved in the racing accesses or blocking operations, analyze:
- Member fields: classify each as atomic, mutex-protected (`TSA_GUARDED_BY`), pointer-guarded (`TSA_PT_GUARDED_BY`), unprotected, or immutable
- Synchronization primitives: mutexes, their types, what they guard, lock ordering (`TSA_ACQUIRED_AFTER`)
- Thread access patterns: which threads call the methods in the stack traces
- Condition variables: what predicate is waited on, who signals

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

#### For test hangs (deadlock or livelock detected via all-thread stacktraces):

The stacktrace file contains `bt all` output from lldb — backtraces of every thread in the hung process. Each thread shows `thread #N, name = '<name>'` followed by its stack frames.

**For deadlocks** (~0% CPU — all threads blocked):
1. Identify blocked threads — look for threads waiting on:
   - `pthread_cond_wait` / `condition_variable::wait` — what predicate are they waiting for? Who should signal?
   - `pthread_join` / `std::thread::join` — which thread are they joining? Is that thread also blocked?
   - `pthread_mutex_lock` — which mutex? Who holds it?
2. Map each blocked thread's `DB::` frames to source locations.
3. Determine the root cause:
   - Circular lock dependency (thread A holds mutex X, waits for mutex Y; thread B holds Y, waits for X)
   - Condition variable never signaled (producer finished or exited before signaling consumer)
   - Thread join waiting for a thread that is itself blocked (transitive deadlock)
   - Missing `notify_all`/`notify_one` on a code path that changes the wait predicate
4. Propose fix: fix lock ordering, add missing notification, restructure to avoid nested waits, add `TSA_ACQUIRED_AFTER` annotations.

**For livelocks** (high CPU — threads spinning but making no forward progress):
1. Identify spinning threads — look for threads in application code (not blocked in kernel):
   - Tight loops retrying a failed operation
   - CAS loops that keep failing because another thread undoes the change
   - Busy-wait loops with no backoff
2. Map spinning threads' `DB::` frames to source locations.
3. Determine root cause:
   - Two threads competing on the same atomic variable in opposing directions
   - Retry loop without exponential backoff or yield
   - Spurious wakeup loop where the condition is immediately invalidated by another thread
4. Propose fix: add proper synchronization, introduce backoff, restructure to avoid contention.

## ClickHouse Threading Reference
Read the reference from: {{CLICKHOUSE_REFERENCES_FILE}}
(Use the Read tool for background context on ClickHouse threading primitives and TSA macros.)

## Output Format

### Alert Analysis
**Error Type:** <type — e.g., data race, lock-order-inversion, deadlock, livelock, thread leak>
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
Update `{{THREADING_MODEL_FILE}}` directly (using Edit tool) with new discoveries for the classes involved in this alert or hang. The file's header describes the required format, conventions, and update rules — follow it exactly. Only add or update entries for classes involved in the alert.
