# Re-review: Runner.cpp Fixes — What Bugs They Solve and When Things Work

## 1. What the Code Changes Do

### 1.1 Constants (lines 54–61)
- `MAX_REQUESTS_IN_MULTI = 500000`
- `SUSPICIOUSLY_LARGE_REQUESTS_SIZE = 1000000`

Used only for validation in `getRequestsSize()`.

### 1.2 `getRequestsSize()` (lines 497–520)
- Reads `requests_size` via `getField(requests_size_idx, row).safeGet<UInt64>()`.
- If `requests_size > 1_000_000` → **throw** `BAD_ARGUMENTS` ("Suspiciously large...").
- If `requests_size > 500_000` → **throw** `BAD_ARGUMENTS` ("exceeds maximum allowed").
- Otherwise returns the value.

### 1.3 `getVersion()` (lines 522–530)
- If `field.getType() == Float64` → use `safeGet<Float64>()` then cast to Int32.
- Else → `safeGet<Int64>()` then cast to Int32.
- No other behavior change.

### 1.4 `getError()` (lines 532–540)
- If `field.getType() == Float64` → use `safeGet<Float64>()` then cast to Coordination::Error.
- Else → `safeGet<Int64>()` then cast.
- No other behavior change.

### 1.5 `getField()` (lines 542–565)
- Before `column->get(row, field)`:
  - If `row >= block.rows()` → **throw** `LOGICAL_ERROR` ("Row index ... out of bounds").
  - If `position >= block.columns()` → **throw** ("Column position ... out of bounds").
- So every use of `getField()` is guarded by row/column bounds.

---

## 2. What Bugs This Addresses

### 2.1 Bug: Corrupted `requests_size` (e.g. 281474976697496) → SIGABRT

- **Observed:** GDB showed `requests_size = 281474976697496` and later SIGABRT (likely from `requests.reserve(requests_size)` or related logic).
- **Cause:** That value is not in the parquet; it appears at runtime when reading the log (wrong column, wrong row, or bad data from the format layer).
- **What the fix does:**  
  - For any call that reaches `getRequestsSize()` and returns a value, that value is checked.  
  - If it is `> SUSPICIOUSLY_LARGE_REQUESTS_SIZE` or `> MAX_REQUESTS_IN_MULTI`, we **throw** instead of continuing.  
  - So we never call `requests.reserve(requests_size)` with that corrupted value.
- **Effect:** The program fails with a clear `BAD_ARGUMENTS` exception (“Suspiciously large requests_size value: …”) instead of SIGABRT. The “bug” from the user’s perspective (random crash) is replaced by a defined failure with a clear message.

So this fix **does** “solve” the bug in the sense: **no more SIGABRT from this path; you get a clean, understandable error**.

### 2.2 Bug: `error` / `version` columns as Float64 → “Bad get” / type errors

- **Observed:** Exceptions like “Bad get: has Float64, requested Int64” when the parquet had Float64 for `error` or `version`.
- **What the fix does:**  
  - `getError()` and `getVersion()` branch on `field.getType() == Float64` and, in that case, use `safeGet<Float64>()` then cast to the expected type.
- **Effect:** Logs that store `error` or `version` as Float64 can be replayed without that type error. So this **does** fix that class of bugs for those two columns.

### 2.3 Bug: Row or column index out of range → reading garbage

- **Risk:** `row` or `position` out of range could lead to undefined behavior or garbage (e.g. fake `requests_size`).
- **What the fix does:**  
  - `getField()` checks `row` and `position` before `column->get(...)`.  
  - All getters (including `getRequestsSize()` via `getField()`) go through this.
- **Effect:** Out-of-bounds access is turned into a well-defined `LOGICAL_ERROR` instead of random corruption or crash. So this **does** address that class of bugs.

---

## 3. What the Fix Does *Not* Solve

### 3.1 Root cause of the corrupted value

- The fix **does not** find or fix *why* `requests_size` sometimes becomes 281474976697496 (wrong column, wrong row, parquet/block handling, etc.).
- It only **rejects** such values and fails fast with a clear exception.
- Fixing the root cause would require more investigation (e.g. format, block layout, or how the parquet is written/read).

### 3.2 Test “success” when data is corrupted

- If the parquet (or the way it’s read) still produces a corrupted `requests_size` for some row, the **test will still fail**.
- The only change is **how** it fails: exception with message instead of SIGABRT.
- So the fix does **not** make the test pass when the input is actually corrupted; it makes the failure **predictable and debuggable**.

---

## 4. When Will Things “Actually Work”?

Things will “actually work” (bench runs and test can pass) only if **all** of the following hold:

1. **Parquet and schema**
   - `zookeeper_log.parquet` has the columns and types keeper-bench expects (including `requests_size` as a numeric type that maps to UInt64).
   - For `error` / `version`, either they are already Int-like, or the Float64 handling in `getError()` / `getVersion()` covers how they are actually stored.

2. **No corrupted `requests_size` in the stream**
   - For every Multi/MultiRead row, the value read for `requests_size` is correct and in [0, 500_000].
   - If the current crash comes from a *bug in the reader or block layout* that produces 281474976697496, then:
     - With the **new** code: you get a clear exception and no SIGABRT.
     - The test still fails until that reader/layout bug is fixed or the parquet is produced in a way that avoids it (e.g. sanitization script, different row groups, etc.).

3. **Sanitization and limits**
   - Your sanitization keeps `requests_size` in a valid range (e.g. ≤ 500k, and no nonsensical huge values).
   - You do not rely on Multi/MultiRead with more than 500k subrequests in this test.

4. **Binary includes the new code**
   - The binary under `CLICKHOUSE_BINARY` must be built **after** these Runner.cpp changes.
   - The run you did used a binary from Dec 21, so it did **not** include this fix. A successful test of “our” behavior requires a rebuild.

---

## 5. How to Check That It “Actually Works”

1. **Rebuild**
   - Fix any environment/build issues (e.g. missing curl source).
   - Rebuild the ClickHouse binary so it contains the new Runner.cpp logic.

2. **Run keeper-bench with the same parquet**
   - Use the same `zookeeper_log.parquet` and config as in the stress test.
   - Two outcomes:
     - **Clean exception:**  
       “Suspiciously large requests_size value: 281474976697496 (likely corrupted). Expected < 1000000”  
       → Validation is active; the old SIGABRT path is avoided. Remaining problem is why that value appears (data or reader).
     - **Bench completes:**  
       → No corrupted `requests_size` was seen; the fix did not need to trigger. Replay is consistent with the parquet.

3. **Run the stress test**
   - Set `CLICKHOUSE_BINARY` to the **new** binary.
   - Run:
     - `PYTHONPATH=.:tests/stress pytest -p no:cacheprovider --durations=0 -vv -s tests/stress/keeper/tests/test_scenarios.py -k 'CHA-01-REPLAY' --duration 180 --matrix-backends=default --faults=false`
   - Interpret:
     - **Exit 0:** Test passed; replay worked for this parquet and config.
     - **Failure with “Suspiciously large requests_size” (or similar):** Fix is working (no crash), but something still feeds a bad value; next step is data/reader investigation.
     - **SIGABRT or other crash:** Either the crash is in another code path, or the binary was not rebuilt.

---

## 6. Short Summary

| Bug / behavior | Solved? | Notes |
|----------------|--------|--------|
| SIGABRT when `requests_size` is corrupted (e.g. 281474976697496) | **Yes** | Replaced by BAD_ARGUMENTS exception; no reserve with huge size. |
| “Bad get” / type errors when `error` or `version` is Float64 | **Yes** | getError/getVersion handle Float64 explicitly. |
| Out-of-bounds row/column in getField | **Yes** | Bounds checks in getField() throw LOGICAL_ERROR. |
| Root cause of corrupted `requests_size` | **No** | Only detection and fail-fast; cause still unknown. |
| Test always passes | **Only if** | Parquet and reader yield valid `requests_size` and you use a binary built with this fix. |

So: the changes **do** fix the concrete bugs above (crash → clear error, type errors, out-of-bounds). Whether the test “actually works” in the sense of passing depends on using a **rebuilt** binary and on **data/reader** not producing corrupted `requests_size` for the current parquet.
