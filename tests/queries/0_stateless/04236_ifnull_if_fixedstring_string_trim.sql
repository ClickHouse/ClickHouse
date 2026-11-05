-- `if` produced different results for `FixedString` operands depending on whether the
-- condition was constant: the const-condition fast path goes through `castColumn`, which
-- trims `FixedString` trailing NUL padding when converting to `String`, while the
-- non-const path copies raw bytes through `FixedStringSource`/`StringSink` and keeps
-- the padding. `ifNull`/`nullIf` rewrite to `if` and inherited the same bug.

SET session_timezone = 'UTC';

-- ifNull: constness of the first argument flipped trimming.
SELECT length(ifNull(CAST(toFixedString('xkE', 100) AS Nullable(FixedString(100))), materialize(CAST('fallback' AS String))));
SELECT length(ifNull(materialize(CAST(toFixedString('xkE', 100) AS Nullable(FixedString(100)))), materialize(CAST('fallback' AS String))));
SELECT ifNull(materialize(CAST(toFixedString('xkE', 100) AS Nullable(FixedString(100)))), materialize(CAST('fallback' AS String)));

-- ifNull where the alternative is a `FixedString`. The first arg is non-null, but
-- the materialization mismatch still bit through the rewrite to `if`.
SELECT length(ifNull(CAST(toFixedString('xkE', 8) AS Nullable(FixedString(8))), materialize(CAST(toFixedString('alt', 16) AS FixedString(16)))));
SELECT length(ifNull(materialize(CAST(toFixedString('xkE', 8) AS Nullable(FixedString(8)))), materialize(CAST(toFixedString('alt', 16) AS FixedString(16)))));

-- if(cond, FixedString, LowCardinality(Nullable(String))): result type is Nullable(String).
SELECT length(if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('', 28) AS FixedString(28))), materialize(CAST('60' AS LowCardinality(Nullable(String))))));
SELECT length(if(CAST(1 AS UInt8), materialize(CAST(toFixedString('', 28) AS FixedString(28))), materialize(CAST('60' AS LowCardinality(Nullable(String))))));
SELECT if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('', 28) AS FixedString(28))), materialize(CAST('60' AS LowCardinality(Nullable(String)))));

-- if where then is materialized FixedString and else is a regular String.
SELECT length(if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('abc', 16) AS FixedString(16))), materialize(CAST('zz' AS String))));
SELECT length(if(materialize(CAST(0 AS UInt8)), materialize(CAST(toFixedString('abc', 16) AS FixedString(16))), materialize(CAST('zz' AS String))));

-- if with both branches FixedString of different N. Result is `String`.
SELECT length(if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('x', 5) AS FixedString(5))), materialize(CAST(toFixedString('y', 9) AS FixedString(9)))));
SELECT length(if(materialize(CAST(0 AS UInt8)), materialize(CAST(toFixedString('x', 5) AS FixedString(5))), materialize(CAST(toFixedString('y', 9) AS FixedString(9)))));

-- if with both branches FixedString of the same N. Result is `FixedString(N)` and trailing
-- NULs must be preserved (no trimming).
SELECT length(if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('abc', 5) AS FixedString(5))), materialize(CAST(toFixedString('xy', 5) AS FixedString(5)))));

-- nullIf: this case is already covered by the comparison-side fix, but check it stays
-- consistent end-to-end.
SELECT isNull(nullIf(materialize(CAST('' AS String)), CAST(toFixedString('', 135) AS FixedString(135))));
SELECT isNull(nullIf(materialize(CAST('' AS String)), materialize(CAST(toFixedString('', 135) AS FixedString(135)))));

-- `executeForNullThenElse`: when one branch is `NULL` and the other is `FixedString`,
-- the const-condition path returns the FixedString directly (preserved) but the
-- materialized-condition path used `castColumn` which trimmed.
SELECT length(if(CAST(0 AS UInt8), CAST(NULL AS Nullable(String)), materialize(CAST(toFixedString('abc', 10) AS FixedString(10)))));
SELECT length(if(materialize(CAST(0 AS UInt8)), CAST(NULL AS Nullable(String)), materialize(CAST(toFixedString('abc', 10) AS FixedString(10)))));
SELECT length(if(CAST(1 AS UInt8), materialize(CAST(toFixedString('abc', 10) AS FixedString(10))), CAST(NULL AS Nullable(String))));
SELECT length(if(materialize(CAST(1 AS UInt8)), materialize(CAST(toFixedString('abc', 10) AS FixedString(10))), CAST(NULL AS Nullable(String))));

-- `getConstantResultForNonConstArguments`: with a constant condition and a constant
-- `FixedString` branch the result was folded through `castColumn` at DAG-build time,
-- which trimmed the padding; the materialized-condition runtime path preserves it.
SELECT length(if(CAST(1 AS UInt8), CAST(toFixedString('abc', 10) AS FixedString(10)), materialize(CAST('zz' AS String))));
SELECT length(if(materialize(CAST(1 AS UInt8)), CAST(toFixedString('abc', 10) AS FixedString(10)), materialize(CAST('zz' AS String))));
SELECT length(if(CAST(0 AS UInt8), materialize(CAST('zz' AS String)), CAST(toFixedString('abc', 10) AS FixedString(10))));
SELECT length(if(materialize(CAST(0 AS UInt8)), materialize(CAST('zz' AS String)), CAST(toFixedString('abc', 10) AS FixedString(10))));
