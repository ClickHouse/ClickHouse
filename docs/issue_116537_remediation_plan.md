# In-Depth Remediation Plan: Issue #116537 (Typed `DateTime64` `JSON` Leaf AST Serialization Precision Loss and Scale Corruption)

**Date:** 2026-08-26  
**Target Branch:** `fix-json-datetime64-leaf-serialization` (branched from `master`)  
**Related Issue:** [ClickHouse/ClickHouse#116537](https://github.com/ClickHouse/ClickHouse/issues/116537)  
**Related PR:** [ClickHouse/ClickHouse#95055](https://github.com/ClickHouse/ClickHouse/pull/95055)  

---

## 1. Executive Summary

When executing distributed queries that contain `JSON` (or `Object`) constants with typed `DateTime64` paths (e.g. `CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')`), ClickHouse serializes constant expressions to AST representations to push them to remote shards. 

In recent serialization changes introduced in PR #95055, typed `DateTime64` leaves inside `JSON` constants were changed to render as bare, unquoted numeric values in the serialized JSON text. When pushed to remote shards, this causes two severe failure modes:

1. **Sub-second Sub-Nanosecond Precision Loss (Scales 7–9):**  
   Fractional bare numbers in JSON text are parsed by the DOM JSON parser into `Float64` values. Because `Float64` only provides 53 bits of precision (~15–17 decimal digits), high-scale timestamps (such as `2023-10-29 01:30:00.123456789`, which requires 19 decimal digits) suffer precision truncation when re-serialized by `DateTime64Node` in `JSONExtractTree`. For example, `...123456789` degrades silently to `...123456700` on the shard.

2. **Severe Date Shift / Off-by-Scale Corruption:**  
   When the remote shard runs with `compatibility <= '26.7'` or `input_format_read_datetime_number_as_raw_value = 1`, `JSONExtractTree` interprets bare integer JSON numbers not as Unix timestamp seconds, but as *raw tick counts*. For a `DateTime64(3)` value representing `1698543000` seconds (`2023-10-29`), the remote shard interprets `1698543000` as `1698543000` milliseconds, shifting the parsed date by a factor of 1000 to `1970-01-20`.

This plan details the technical root cause, affected files, exact fix implementation, edge case safety, and test plan.

---

## 2. Technical Deep-Dive & Root Cause Analysis

### 2.1 The Constant Serialization Flow

During distributed query planning, constant nodes in the query tree are converted to AST literals via `ConstantNode::toASTImpl` (in `src/Analyzer/ConstantNode.cpp`), which invokes `getFieldFromColumnForASTLiteral` (defined in `src/Analyzer/Utils.cpp`).

When `getFieldFromColumnForASTLiteralImpl` encounters a `TypeIndex::Object` column:
1. It iterates over `object_column.getTypedPaths()`.
2. For each typed path of type `DataTypeDateTime64`, PR #95055 passed `datetime64_as_numbers=true`.
3. This produced a raw numeric `Decimal` `Field` representing the scaled/unscaled value.
4. When `convertObjectToString` (in `src/Common/FieldVisitorToString.cpp`) formats the resulting `Object` into JSON text, it uses `FieldVisitorToJSONElement`.
5. `FieldVisitorToJSONElement` formats numeric/decimal fields as unquoted JSON numbers (e.g. `{"a": 1698543000.123456789}` or `{"a": 1698543000}`).

### 2.2 The Shard Deserialization Pipeline

On the remote receiving node, the pushed query AST is parsed and evaluated. For `JSON` column types with typed paths, reading JSON values does **not** invoke `SerializationDateTime64::deserializeTextJSON`. Instead, typed leaf extraction goes through `JSONExtractTree` (`src/Formats/JSONExtractTree.cpp`):

```
                       JSON Constant Text AST Literal
                                     │
                                     ▼
                          JSONExtractTree.cpp
                                     │
                        ┌────────────┴────────────┐
                        ▼                         ▼
              ElementType::DOUBLE        ElementType::UINT64
                        │                         │
             Parsed into Float64         Checked for raw ticks setting
           (53-bit mantissa limit)       read_datetime_number_as_raw_value
                        │                         │
                        ▼                         ▼
             Precision lost for         Interpreted as raw ticks instead of
              scales 7, 8, 9            seconds under compatibility <= '26.7'
             (...789 -> ...700)         (1698543000 ms -> 1970-01-20)
```

1. **`ElementType::DOUBLE` Arm (`src/Formats/JSONExtractTree.cpp:982-996`):**
   The DOM parser (`simdjson` / `rapidjson`) reads `1698543000.123456789` as `Float64`. When `jsonElementToString` and `tryReadDateTime64AsNumber` re-serialize and parse it, sub-second digits beyond `Float64` precision are lost.

2. **`ElementType::UINT64` Arm (`src/Formats/JSONExtractTree.cpp:997-1017`):**
   If `format_settings.read_datetime_number_as_raw_value` is set (which is enabled by default under `compatibility <= '26.7'`), `JSONExtractTree` treats `ElementType::UINT64` as raw tick counts:
   ```cpp
   if (format_settings.read_datetime_number_as_raw_value)
   {
       ...
       value.value = static_cast<DateTime64::NativeType>(raw);
   }
   ```
   Because the sender emitted `1698543000` (seconds), the receiver assigns `1698543000` as the raw tick value for `DateTime64(3)` (milliseconds), resulting in a timestamp off by a scale factor of 1000.

### 2.3 Flawed Code Assumption

The comment at `src/Analyzer/Utils.cpp:1366-1376` claimed:
> *"Unquoted number for a DateTime64 in the typed-JSON path is a Unix timestamp in seconds parsed exactly (readDateTime64AsNumber), not the raw scaled value..."*

This claim is invalid because:
1. `JSON` typed paths do not use `SerializationDateTime64::deserializeTextJSON`.
2. The receiving parser is `JSONExtractTree`, which routes unquoted numbers through DOM `DOUBLE` or `UINT64` parsing.

---

## 3. Detailed Fix Strategy

To preserve exact sub-second precision and remain setting-independent across compatibility modes, typed `DateTime64` leaves in `JSON` constants must be formatted as quoted text strings (e.g. `"2023-10-29 01:30:00.123456789"`) in the JSON text.

### 3.1 Code Changes in `src/Analyzer/Utils.cpp`

1. **Revert numeric leaf emission for `DateTime64` in `getFieldFromColumnForASTLiteralImpl`:**
   Ensure typed `DateTime64` paths inside `JSON` / `Object` constants return text fields (via `data_type->getDefaultSerialization()->serializeText(*column, row, buf, {})`).

2. **Quoted String Representation:**
   When formatted by `convertObjectToString` / `FieldVisitorToJSONElement`, a `Field` of type `String` is emitted as a quoted JSON string: `{"a": "2023-10-29 01:30:00.123456789"}`.

3. **Handling DST Ambiguity:**
   To ensure that string-formatted `DateTime64` values do not suffer from local timezone Daylight Saving Time (DST) ambiguities when parsed by remote nodes:
   - Format the `DateTime64` text in UTC or with explicit UTC timestamp formatting (`ISO` format or UTC timezone context) when serializing `DateTime64` leaves for AST literals.

4. **Correcting Developer Documentation Comments:**
   Update comments at `src/Analyzer/Utils.cpp:1366-1376` to explicitly document that `JSONExtractTree` is the receiving parser for `JSON` object constants and explain why quoted text form is required.

5. **`Time64` Path Verification:**
   Verify that `Time64` leaves inside `JSON` constants continue to round-trip correctly through `Time64Node` in `JSONExtractTree`.

---

## 4. Affected Source Files

| File | Responsibilities / Changes |
| :--- | :--- |
| [`src/Analyzer/Utils.cpp`](file:///home/msohail22/Github/ClickHouse/src/Analyzer/Utils.cpp#L1363-L1505) | Update `getFieldFromColumnForASTLiteralImpl` to format typed `DateTime64` JSON leaves as exact quoted text strings instead of bare numbers; update doc comments. |
| [`src/Formats/JSONExtractTree.cpp`](file:///home/msohail22/Github/ClickHouse/src/Formats/JSONExtractTree.cpp#L980-L1020) | Audit `DateTime64Node` handling to ensure quoted string text parsing losslessly restores `DateTime64` values up to scale 9. |
| [`tests/queries/0_stateless/03381_remote_constants.sql`](file:///home/msohail22/Github/ClickHouse/tests/queries/0_stateless/03381_remote_constants.sql) | Update or add assertions for typed `DateTime64` JSON leaves in distributed queries. |
| New Stateless Test | Create `tests/queries/0_stateless/03400_json_datetime64_remote_exact.sql` reproducing all cases from Issue #116537. |

---

## 5. Verification & Test Plan

### 5.1 Test Cases to Add

A new stateless test (`03400_json_datetime64_remote_exact.sql`) will be added using `./tests/queries/0_stateless/add-test`:

```sql
SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

-- 1. High-precision DateTime64(9) leaf in JSON over remote shard
SELECT toUnixTimestamp64Nano(json.a) AS v
FROM (
    SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')) AS json 
    FROM remote('127.0.0.1', system.one)
)
ORDER BY v;
-- Expected output: 1698543000123456789

-- 2. Whole-second DateTime64(3) leaf in JSON under compatibility = '26.7'
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (
    SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json 
    FROM remote('127.0.0.1', system.one)
)
ORDER BY v
SETTINGS compatibility = '26.7';
-- Expected output: 1698543000000

-- 3. Non-UTC timezone DateTime64 leaf at DST transition
SELECT toUnixTimestamp64Nano(json.a) AS v
FROM (
    SELECT materialize(CAST('{"a":"2023-10-29 02:30:00.123456789"}', 'JSON(a DateTime64(9, \'Europe/Berlin\'))')) AS json 
    FROM remote('127.0.0.1', system.one)
)
ORDER BY v;
-- Expected output: exact UTC nanosecond instant
```

### 5.2 Build & Test Execution

1. Build ClickHouse using Ninja:
   ```bash
   ninja -C build clickhouse
   ```
2. Run stateless tests:
   ```bash
   ./dbms/tests/queries/0_stateless/03381_remote_constants.sh
   ./dbms/tests/queries/0_stateless/03400_json_datetime64_remote_exact.sh
   ```

---

## 6. Summary of Compliance with Project Guidelines

- **Branching:** Created feature branch `fix-json-datetime64-leaf-serialization` targeting `master`. No rebase or amend will be used.
- **Code Style:** Allman-style braces, functions written as `f` (e.g. `convertObjectToString`), language elements in backticks (e.g. `DateTime64`, `MergeTree`).
- **Terminology:** Errors referred to as exceptions (no use of "crash").
