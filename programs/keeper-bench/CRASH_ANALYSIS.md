# Keeper-bench replay crash – analysis status

## What to fix (summary)

### 1. Stats.cpp – **fixed**

**Bug:** `Stats::StatsCollector::getThroughput()` used `assert(requests != 0)`. When `Stats::report()` or `Stats::writeJSON()` runs with one collector having 0 requests (e.g. read_requests > 0 but write_requests == 0), it still calls `getThroughput()` on both collectors and hits the assert → SIGABRT.

**Fix:** In `Stats.cpp`, `getThroughput()` now returns `{0.0, 0.0}` when `requests == 0` (or when `seconds <= 0`) instead of asserting. Callers are unchanged.

### 2. Nested Multi (op_num=22 as subrequest) – **identified and fixed**

**Root cause (from fine-grained logs):** The last `[keeper-bench-dbg]` line before the crash was `op_num=22 switch_entry` with `for_multi=1`. So we were reading a **Multi/MultiRead row (op_num=22)** as a **subrequest** of another Multi. ZooKeeper disallows nested Multi; the log is invalid at that point.

**Observed in run:** Row **2693** is Multi/MultiRead but appears as **subrequest index 32** of a parent Multi. So the parquet has misaligned data: either the parent’s `requests_size` is wrong, or truncation/row order put a Multi header in the middle of another Multi’s subrequest block.

**Code fix (done):** When `getNextRequest(for_multi=true)` sees op_num Multi/MultiRead, it **returns `std::nullopt`** instead of throwing. The caller then throws `BAD_ARGUMENTS: "Failed to fetch subrequest for MultiRead, subrequest index K"`. Main catches and returns non-zero. **No more SIGABRT** – keeper-bench exits cleanly with a clear exception.

**Data fix (to make replay succeed):** The parquet must not have a Multi/MultiRead row where a subrequest is expected. Options:
- In the **sanitizer**: when truncating, never cut between a Multi header and its last subrequest row (e.g. truncate only at Multi boundaries, or skip Multi that would be split).
- Or fix the **source** (prod log / pipeline) so a Multi header never appears in the middle of another Multi’s subrequest block.

---

### 3. Subrequest-row crash (other getters) – **identify, then fix**

**Observed:** The crash can also happen **inside `getNextRequest(for_multi=true)`** while reading a subrequest row, in one of:

- Loading a new chunk (`input_format->generate()`, `cloneWithColumns`)
- `getError`, `getSessionId`, `hasWatch`, `getExecutorId`, `getRequestEventTime`, `getResponseEventTime`, `getOpNum`
- Or inside the `op_num` switch: `getPath`, `getData`, `getVersion` for Create/Set/Remove/Check/Get/Exists/Sync/List

**How to identify the exact line:** Rebuild keeper-bench (it now has fine-grained `[keeper-bench-dbg]` logs inside `getNextRequest` when `for_multi=true`). Run the test again. The **last** `[keeper-bench-dbg]` line before the crash tells you the exact step:

| Last line | What to fix |
|-----------|-------------|
| `getNextRequest(for_multi=1) loading_new_chunk` | Chunk load or block build; check `input_format->generate()` / `cloneWithColumns`. |
| `getNextRequest(for_multi=1) row=X before_getError` | **getError** or the Error column for that row (type, null, bounds). |
| `after_getError before_getSessionId` | **getSessionId** or the session_id column. |
| `after_getExecutorId before_getRequestEventTime` | **getRequestEventTime** or the request_event_time column. |
| `after_times before_getOpNum` | **getOpNum** or the op_num column. |
| `op_num=N switch_entry` | Op type known; crash is in the next getter. |
| `Create/Set/Remove/Get/Exists/Sync/List/Check before_getPath` | **getPath** or the path column for that op. |
| `Create after_getPath before_getData` | **getData** or the data column. |
| `Set/Remove/Check before_getVersion` | **getVersion** or the version column. |

**Fix:** Once you know the line, either (a) harden that getter (e.g. handle null/wrong type like `getError`/`getVersion`), or (b) fix the data/schema so that column has the expected type and values for subrequest rows.

---

## Diagnostic logging (how to prove root cause)

The Runner is instrumented with `[keeper-bench-dbg]` logs. When you run keeper-bench and get SIGABRT (or any crash), **grep stderr for `[keeper-bench-dbg]`**. The **last line** printed before the crash is the failing step.

| Last line before crash | Failure is in |
|------------------------|----------------|
| `Multi row ... about_to_call_getRequestsSize` | About to call getRequestsSize; crash may be in logging or right after. |
| `getRequestsSize row=... before_getField` | **Inside getField** (column->get) or in bounds checks before it. |
| `getRequestsSize after_safeGet value=X` | **In safeGet** or in caller right after return. If X is huge, value came from storage. |
| `requests_size=X` (X huge) | Value returned; crash is in guards (we’d throw) or in **reserve(X)**. |
| `about_to_reserve(X)` | **In std::vector::reserve(X)** or allocator. |
| `reserve_done entering_subrequest_loop n=X` | In **move_row_iterator()** or about to enter loop. |
| `subrequest i=K/N` | In **getNextRequest(true)** while fetching the K-th subrequest. |

When `for_multi=true`, finer-grained logs narrow it down:

| Last line | Failure is in |
|-----------|----------------|
| `getNextRequest(for_multi=1) loading_new_chunk` | Before/during `input_format->generate()` or block build. |
| `getNextRequest(for_multi=1) row=X before_getError` | **getError** (or Error column read). |
| `after_getError before_getSessionId` | **getSessionId**. |
| `after_getExecutorId before_getRequestEventTime` | **getRequestEventTime** / **getResponseEventTime**. |
| `after_times before_getOpNum` | **getOpNum**. |
| `op_num=N switch_entry` | Right after getOpNum; crash in the chosen op_num branch. |
| `Create/Set/... before_getPath` | **getPath** for that op. |
| `Create after_getPath before_getData` | **getData**. |
| `Set/Remove/Check before_getVersion` | **getVersion**. |

Also, the first time each chunk is loaded you get:

`block_schema columns=N 0:name1 1:name2 ... requests_size_idx=I`

Use this to confirm column order and that `requests_size_idx` points at a column named `requests_size`. If the crash is “before_getField” or “after_safeGet” with a crazy value, compare this schema to the parquet file’s column order.

**To capture:** run e.g.  
`keeper-bench --input-request-log ... --config ... 2>&1 | tee run.log`  
then `grep '\[keeper-bench-dbg\]' run.log` and inspect the last line.

### Observed result (CHA-01-REPLAY run)

keeper-bench was run with `zookeeper_log.parquet`; it **aborted (core dumped**, exit 134 = SIGABRT).

**Last `[keeper-bench-dbg]` line before crash:**
```
[keeper-bench-dbg] subrequest i=0/48
```

**Interpretation:** The crash is **not** in `getRequestsSize` or in `requests.reserve()`. All Multi/MultiRead rows up to that point showed sane `requests_size` (17, 48, 1, 48, …), and we logged `reserve_done entering_subrequest_loop` and `subrequest i=0/48`. The abort happens **inside `getNextRequest(for_multi=true)`** while fetching the **first subrequest** (i=0) of the Multi at block row 2660 (which has 48 subrequests). So the failing path is: reading/parsing the **subrequest row** (row 2661)—either in a getter (`getError`, `getSessionId`, `getOpNum`, `getPath`, etc.) or when loading the next chunk. Next step: add logging at the top of `getNextRequest` and before each getter for the subrequest row, or run under GDB and break right after `subrequest i=0/48`.

---

## What has been verified vs assumed

### Verified
- **SIGABRT** occurs when replaying the (sanitized) prod parquet.
- **GDB showed** `requests_size = 281474976697496` at some point during the run (e.g. in the Multi/MultiRead path).

### Assumed (not proven)
- The crash is **caused by** that `requests_size` value.
- The crash happens in **`requests.reserve(requests_size)`** (or immediately after) in the Multi/MultiRead branch.

So far we have **correlation** (huge value present when things go wrong), not **causation** (proof that this value and this line are the only cause of SIGABRT).

---

## All possible failure points in the replay path

Anything that runs when reading a Multi/MultiRead row or its subrequests can contribute to a crash or bad behavior.

### 1. Before we read `requests_size` (same row, same `idx_in_block`)

These run first for every row, including Multi/MultiRead:

| Call | Can throw? | Can crash/abort? |
|------|------------|------------------|
| `getError(idx_in_block)` | Yes (type mismatch, null) | No – throws BAD_GET or returns |
| `getSessionId(idx_in_block)` | Yes (bounds, type) | No |
| `hasWatch(idx_in_block)` | Yes | No |
| `getExecutorId` → `getSessionId` | Yes | No |
| `getRequestEventTime(idx_in_block)` | Yes | No |
| `getResponseEventTime(idx_in_block)` | Yes | No |
| `getOpNum(idx_in_block)` | Yes | No |

If the process **aborts** (SIGABRT) rather than throwing, it is unlikely to come from these, unless something in the exception path or in the format/column layer aborts.

### 2. When we read `requests_size` and use it

| Step | Code | Possible failure |
|------|------|------------------|
| Read | `getRequestsSize(idx_in_block)` → `getField(requests_size_idx, row).safeGet<UInt64>()` | Type mismatch → **throws** BAD_GET. Wrong column/row → wrong value (e.g. 281474976697496). |
| Check | New guards: if `requests_size > 1e6` or `> 500k` → throw | Prevents using huge value in reserve; turns into **exception**, not SIGABRT. |
| Allocate | `requests.reserve(requests_size)` | Huge `requests_size` → huge allocation → allocator failure or OOM killer → **SIGKILL/SIGABRT** possible. |
| Loop | `for (i = 0; i < requests_size; ++i) { getNextRequest(true); ... }` | If `requests_size` is huge, we call `getNextRequest` a huge number of times; could hit stack exhaustion, OOM, or timeout before we ever “finish” the Multi. |

So:
- **If** the crash is in `reserve(requests_size)` with a huge value → the Runner.cpp guards (throw instead of reserve) **should** turn that into an exception. If you still see SIGABRT after that, either the binary wasn’t rebuilt, or the crash is **elsewhere**.
- **If** the crash is in the **loop** (e.g. deep recursion, OOM in subrequest handling), then the root cause is “huge `requests_size`” but the **actual abort** is in a different line (e.g. inside `getNextRequest` or the executor).

### 3. While consuming subrequests

| Step | Code | Possible failure |
|------|------|------------------|
| Next row | `getNextRequest(for_multi=true)` | Loads new chunk when block is exhausted. Builds `ZooKeeperRequestBlock(header_block.cloneWithColumns(chunk.detachColumns()))`. |
| Block build | `getPositionByName("requests_size")` etc. | Wrong column order or missing column in chunk → **exception** (or wrong indices → wrong data). |
| Per-subrequest | `getError`, `getSessionId`, `getPath`, … | Type/range issues → exception. |
| “Failed to fetch subrequest” | `if (!subrequest_from_log)` | We **throw**; no abort from this. |
| Session/executor mismatch | Same-row checks | We **throw**; no abort. |

So crashes during subrequest reading are more likely from:
- Format/block construction (e.g. chunk with wrong number or order of columns), or
- Some code path that **aborts** on bad data instead of throwing (e.g. in the Parquet/Arrow/CH stack).

### 4. Outside the reader (execution path)

- **Pushing/executing** the Multi request (ZooKeeper client, network, server): a bug there could abort in another thread or in a library (e.g. SSL, allocator). That would **not** be “because of `requests_size`” in the sense of our reader logic, but the **trigger** could still be “we constructed and sent a Multi with wrong size” due to a bad `requests_size`.

### 5. Where the bad value can come from

Even if we agree “something is wrong with `requests_size`,” we have **not** proven **where** it is wrong:

| Hypothesis | Meaning | How to check |
|------------|--------|--------------|
| **On-disk** | The parquet file actually contains 281474976697496 in the `requests_size` column for some row | Inspect parquet (e.g. `pyarrow.parquet`, pandas) and scan `requests_size`; check max and distribution. |
| **Wrong column** | Block columns don’t match expectations; `requests_size_idx` points at another column (e.g. high bytes of a string or of another int) | Log block column names and positions when opening the file; log raw field type/value at `requests_size_idx` for the crashing row. |
| **Wrong row** | We’re reading a row that isn’t the Multi header (e.g. desync after a previous bug, or block boundary cutting through a Multi) | Log row index, op_num, and requests_size for each Multi/MultiRead; confirm we’re at the intended row. |
| **Reader/format bug** | Parquet reader or block assembly returns wrong data for the right (name, row) | Compare: same file read in Python vs keeper-bench; or add a minimal C++ reader that dumps the same (row, column) and compare. |
| **Type confusion** | Column is e.g. Float64 or Nullable; we misinterpret bytes as UInt64 | `safeGet<UInt64>()` on non‑integer type **throws** in CH; we’d see BAD_GET, not a silent wrong value. So “garbage UInt64” is more likely wrong column or wrong row than type mismatch. |

---

## Steps to confirm root cause

1. **Get exact crash location**
   - Run under GDB, reproduce SIGABRT, run `bt full`.
   - Check whether the top frame is:
     - inside `reserve()` / allocator, or
     - inside the Multi loop (e.g. `getNextRequest`), or
     - inside format/block code, or
     - elsewhere (e.g. ZooKeeper client).

2. **Check that the guard is active**
   - Rebuild keeper-bench with the current Runner.cpp (with the `requests_size` checks).
   - Run again: do you get **“Suspiciously large requests_size value: …”** (or “exceeds maximum allowed”) **instead of** SIGABRT?  
   - If yes → the crash was very likely from using that huge value (e.g. in `reserve` or in the loop).  
   - If you still get SIGABRT before any such message → the abort is either in another path, or before we ever call `getRequestsSize()` for that row.

3. **Inspect the parquet file**
   - For the **exact** file used in the failing run, compute in Python:
     - `df["requests_size"].max()`, distribution, and which rows have `requests_size > 1000`.
   - If max is ≤ ~115k and no row has 281474976697496 → the value is **not** on disk; it appears only at read time (wrong column, wrong row, or reader bug).

4. **Check block layout vs Multi boundaries**
   - Log (row_in_block, block_size, op_num, requests_size) for each Multi/MultiRead.
   - See if any Multi’s “header row” and its subrequest rows span two different blocks; if the reader ever hands us a block that doesn’t preserve row order or that splits a Multi across chunks, that could cause wrong row / wrong value.

5. **Narrow down by simplifying input**
   - Replay a **small** parquet that contains only one or a few Multi/MultiRead ops with known `requests_size` (e.g. ≤ 100).
   - If that never crashes, gradually add more data or row groups; see when SIGABRT (or the “Suspiciously large” exception) first appears.

---

## Short answers to “are we sure it’s requests_size?” and “has there been full analysis?”

- **Are we sure the error is because of `requests_size`?**  
  **No.** We have a strong **hypothesis**: GDB showed a huge `requests_size` and we know that using it in `reserve()` or in a huge loop can cause SIGABRT or other bad behavior. We have **not** completed a full analysis that rules out other causes or proves this one.

- **Has there been a full analysis?**  
  **No.** So far we have:
  - Documented one plausible cause (corrupted `requests_size` → `reserve` / loop).
  - Added a **safety check** that turns that case into a clear exception.
  - Listed **other** possible failure points (reader, block boundaries, execution path) and **not** yet confirmed or excluded them with experiments.

This document is the “full analysis” of **what we know, what we assume, and what to do next** so that the root cause can be identified with confidence.
