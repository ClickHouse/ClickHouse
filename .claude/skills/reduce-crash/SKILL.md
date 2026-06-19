---
name: reduce-crash
description: Reproduce a ClickHouse crash found by the AST query fuzzer and reduce it to the smallest clean, runnable query that still triggers the SAME crash on a sanitizer build — then produce a maintainer-ready issue. Use when you have a fuzzer crash (an obfuscated mutated query + a stack trace) and need a minimal reproducer good enough to file.
argument-hint: <crash.json | repro.sql> [head-sha] [sanitizer]
disable-model-invocation: false
allowed-tools: Task, Bash(curl:*), Bash(chmod:*), Bash(mkdir:*), Bash(ls:*), Bash(cat:*), Bash(/tmp/*), Bash(grep:*), Bash(timeout:*), Read, Write, Glob, Grep, AskUserQuestion
---

# Reduce a ClickHouse Crash to a Minimal Reproducer

The AST query fuzzer crashes the server with a heavily-mutated query
(`materialize(reinterpret(...))`, nested `SELECT SELECT`, `toNullable` stacks,
giant globs). That query is **not** a fileable reproducer — a maintainer should
not have to decode it. This skill reproduces the crash on a sanitizer build and
reduces it to the smallest **clean, runnable** query that still hits the SAME
crash site, then writes a maintainer-ready issue.

The non-negotiable invariant: **every reduction is VERIFIED** — it must still
crash the sanitizer build with the SAME signature (same normalized fatal block /
sanitizer SUMMARY). A reduction that stops crashing is rejected, never shipped.
Never file a guess.

## Arguments
- `$0` (required): a crash report (`crash.json` with `offending_query`,
  `stack_trace`/`server_log`, `crash_class`, `head_sha`) **or** a `repro.sql`.
- `$1` (optional): head SHA to fetch the sanitizer binary for (default: read
  from the crash report, else `HEAD`).
- `$2` (optional): sanitizer build flavour — `asan_ubsan` (default), `tsan`, `msan`.

## Step 1 — Get a sanitizer build (fail closed)
A fuzzer crash is usually a sanitizer (ASan/UBSan/TSan) abort, so a plain release
binary will NOT reproduce it. Fetch the **`$2` build for `$1`** from CI S3 (the
same source `utils/auto-bisect/helpers/download.sh` uses, but the sanitizer
artifact, not release). If no sanitizer binary exists for that SHA, say so and
stop — do **not** substitute a release/master binary (you'd get a false "cannot
reproduce"). Start the server with the sanitizer's abort options:
`ASAN_OPTIONS=halt_on_error=1:abort_on_error=1`, `UBSAN_OPTIONS=print_stacktrace=1`,
`TSAN_OPTIONS=halt_on_error=1:abort_on_error=1:history_size=7`.

## Step 2 — Pin the crash signature
Run the offending query (with any seed setup) against the build. Confirm it
crashes: the SERVER log shows a `<Fatal>` / sanitizer `SUMMARY:` block, or the
liveness probe fails. Capture that block and compute a **stable signature**:
lowercase, replace hex (`0x…`) and decimal literals with placeholders, collapse
whitespace, hash. This signature is what every later reduction must preserve —
it stops minimization from silently morphing the bug into a different one.

If the lone query does **not** crash, it's state- or timing-dependent — go to
Step 3b/3d before concluding anything.

## Step 3 — Reduce (stop at the first rung that still crashes the same signature)
1. **Strip mutation noise.** Anchor on the stack trace's culprit frame (the
   first `src/...` ClickHouse frame — NOT `contrib/llvm-project`/libcxx or the
   `ThreadPool`/`TCPHandler` entry frames, which only show where it surfaced).
   Remove dead wrappers: `materialize()`, `reinterpret()` round-trips,
   `toNullable` stacks, `SELECT SELECT`, unrelated columns/joins/settings. Keep
   the function + argument SHAPE the frame implicates. Verify after each cut.
   - Example: trace `DateLUTImpl.h:996` + `dateDiff` →
     `SELECT dateDiff('second', toDateTime64(9223372036854775807, 0), toDateTime64(-1356997800, 0))`.
2. **Drop unneeded setup.** Statement-level delta-debug the seed
   (CREATE/INSERT/SET): remove any statement whose removal preserves the crash.
3. **Replay the sequence** when the lone query won't crash alone — the crash may
   need accumulated session state (a `SET`, a temp table, a prior mutation).
   Replay the run-log mutation chain in order on ONE session, then delta-debug
   the chain (keep the last triggering statement, strip the rest).
4. **Async / background-thread crash** — the stack trace top is
   `asan_thread_start` / `ThreadPool` / `PooledThread` with no app frame, and no
   lone query reproduces it. Do NOT fake a single-query repro. File the stack
   trace as the evidence + the sequence as context, labelled honestly.

After a minimal query is found, confirm it **K-of-N** (e.g. 3-of-5 fresh server
restarts) → classify `deterministic` (≥K) / `intermittent` (≥1, <K) /
`unreproduced` (0).

## Step 4 — Attribute (optional but valuable)
Run the minimal repro on the **same-flavour merge-base** build. Crashes there too
→ `pre_existing` (a master bug, not the PR's). Clean there (and the base is the
same sanitizer flavour) → `introduced by this PR`. Base unavailable / only a
release-master fallback → `unknown` (say so; don't claim master-clean).

## Step 5 — Emit a maintainer-ready issue
- **Title**: the sanitizer `SUMMARY:` if present (e.g.
  `UBSan: undefined-behavior in DateLUTImpl.h:996`), else `<class> in <culprit src frame>`.
- **Body**: the sanitizer report / stack trace (fenced); the minimal repro
  (fenced `sql`); reproducibility (`deterministic`/`intermittent`/`unreproduced`)
  stated plainly; version + build flavour + how it was found; attribution.
- **Strip local paths** from the trace (`/home/.../clickhouse` → `clickhouse`).
- Never claim "confirmed on master" unless you actually ran it on master.

## Pitfalls (learned the hard way)
- **The crash log is ephemeral.** Save the full server log + the minimal repro to
  a durable file BEFORE tearing down the server / re-running anything — a re-run
  overwrites it and the trace is gone.
- **Line numbers are build-specific.** The trace's `file:NNN` is from the
  sanitizer build; don't assert it against master tip as a source citation.
- **`0/5` on the raw fuzzed query ≠ not-a-bug.** It often means the obfuscated
  query is itself flaky; a *cleaner* query may reproduce deterministically where
  the blob didn't. Always try the reduction before giving up.
