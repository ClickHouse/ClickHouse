-- Regression test for convertToFullColumnIfSparse in executeExpressionAndGetColumn.
--
-- When a TTL expression directly references a column (e.g. TTL ts), and that column
-- uses sparse serialization, executeExpressionAndGetColumn returns it as ColumnSparse.
-- The fix converts it to a dense column at the source, so no downstream TTL algorithm
-- needs to handle ColumnSparse individually.
--
-- executeExpressionAndGetColumn has two paths:
--   Path A: result column already in block (e.g. TTL ts) → returns it directly
--   Path B: result column not in block (e.g. TTL ts + INTERVAL 1 SECOND) → evaluates expression
-- Both paths need convertToFullColumnIfSparse.
--
-- Tests 1-3: Path A (direct column reference), no TTL expiry (ts=0 sentinel)
-- Tests 4-6: Path B (expression evaluation), mixed TTL expiry (some expired, some not)

-- Test 1: Row TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_row;

CREATE TABLE t_ttl_sparse_row
(
    id UInt64,
    ts DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL ts
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_row;

-- All ts=toDateTime(0): 100% defaults -> column stored as ColumnSparse.
-- table_ttl.min=0 -> force_ttl=true on merge, so TTL transform runs.
INSERT INTO t_ttl_sparse_row SELECT number, toDateTime(0) FROM numbers(100);
INSERT INTO t_ttl_sparse_row SELECT number + 100, toDateTime(0) FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_row;
OPTIMIZE TABLE t_ttl_sparse_row FINAL;

-- isTTLExpired(0)=false (0 is "no expiry" sentinel) -> all 200 rows survive.
SELECT count() FROM t_ttl_sparse_row;

DROP TABLE t_ttl_sparse_row;


-- Test 2: Column TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_col;

CREATE TABLE t_ttl_sparse_col
(
    id UInt64,
    ts DateTime,
    val String TTL ts
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_col;

INSERT INTO t_ttl_sparse_col SELECT number, toDateTime(0), 'hello' FROM numbers(100);
INSERT INTO t_ttl_sparse_col SELECT number + 100, toDateTime(0), 'world' FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_col;
OPTIMIZE TABLE t_ttl_sparse_col FINAL;

-- ts=0 -> column TTL not applied -> val preserved for all rows.
SELECT count(), countIf(val != '') FROM t_ttl_sparse_col;

DROP TABLE t_ttl_sparse_col;


-- Test 3: GROUP BY TTL with sparse DateTime column
DROP TABLE IF EXISTS t_ttl_sparse_agg;

CREATE TABLE t_ttl_sparse_agg
(
    id UInt64,
    ts DateTime,
    val UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL ts GROUP BY id SET val = max(val)
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_agg;

INSERT INTO t_ttl_sparse_agg SELECT number, toDateTime(0), number FROM numbers(100);
INSERT INTO t_ttl_sparse_agg SELECT number + 100, toDateTime(0), number + 100 FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_agg;
OPTIMIZE TABLE t_ttl_sparse_agg FINAL;

-- ts=0 -> no aggregation triggered -> all 200 rows survive.
SELECT count() FROM t_ttl_sparse_agg;

DROP TABLE t_ttl_sparse_agg;


-- Test 4: Row TTL with expression evaluation (Path B), mixed expiry
-- TTL ts + INTERVAL 1 SECOND → result column NOT in block → expression must be evaluated.
-- Some rows have ts=0 → TTL=1 (epoch+1s, far in past) → isTTLExpired=true → deleted.
-- Some rows have ts='2099-01-01' → TTL=2099-01-01+1s → isTTLExpired=false → survive.
DROP TABLE IF EXISTS t_ttl_sparse_expr_row;

CREATE TABLE t_ttl_sparse_expr_row
(
    id UInt64,
    ts DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL ts + INTERVAL 1 SECOND
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_expr_row;

-- 100 rows with ts=0 (default → sparse). TTL evaluates to toDateTime(1) → expired.
INSERT INTO t_ttl_sparse_expr_row SELECT number, toDateTime(0) FROM numbers(100);
-- 100 rows with ts far in future → not expired.
INSERT INTO t_ttl_sparse_expr_row SELECT number + 100, toDateTime('2099-01-01 00:00:00', 'UTC') FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_expr_row;
OPTIMIZE TABLE t_ttl_sparse_expr_row FINAL;

-- 100 expired rows deleted, 100 future rows survive.
SELECT count() FROM t_ttl_sparse_expr_row;

DROP TABLE t_ttl_sparse_expr_row;


-- Test 5: Column TTL with expression evaluation (Path B), mixed expiry
DROP TABLE IF EXISTS t_ttl_sparse_expr_col;

CREATE TABLE t_ttl_sparse_expr_col
(
    id UInt64,
    ts DateTime,
    val String TTL ts + INTERVAL 1 SECOND
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_expr_col;

-- 100 rows: ts=0 → TTL=1 → expired → val cleared to default ('').
INSERT INTO t_ttl_sparse_expr_col SELECT number, toDateTime(0), 'hello' FROM numbers(100);
-- 100 rows: ts=future → TTL=future+1s → not expired → val preserved.
INSERT INTO t_ttl_sparse_expr_col SELECT number + 100, toDateTime('2099-01-01 00:00:00', 'UTC'), 'world' FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_expr_col;
OPTIMIZE TABLE t_ttl_sparse_expr_col FINAL;

-- All 200 rows exist. 100 expired rows have val='', 100 future rows have val='world'.
SELECT count(), countIf(val != '') FROM t_ttl_sparse_expr_col;

DROP TABLE t_ttl_sparse_expr_col;


-- Test 6: GROUP BY TTL with expression evaluation (Path B), mixed expiry
DROP TABLE IF EXISTS t_ttl_sparse_expr_agg;

CREATE TABLE t_ttl_sparse_expr_agg
(
    id UInt64,
    ts DateTime,
    val UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL ts + INTERVAL 1 SECOND GROUP BY id SET val = max(val)
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_expr_agg;

-- 100 rows: ts=0 → expired → aggregated (each id unique → 100 aggregated rows).
INSERT INTO t_ttl_sparse_expr_agg SELECT number, toDateTime(0), number FROM numbers(100);
-- 100 rows: ts=future → not expired → survive unchanged.
INSERT INTO t_ttl_sparse_expr_agg SELECT number + 100, toDateTime('2099-01-01 00:00:00', 'UTC'), number + 100 FROM numbers(100);

SYSTEM START MERGES t_ttl_sparse_expr_agg;
OPTIMIZE TABLE t_ttl_sparse_expr_agg FINAL;

-- 100 aggregated (expired) + 100 not expired = 200 rows.
SELECT count() FROM t_ttl_sparse_expr_agg;

DROP TABLE t_ttl_sparse_expr_agg;
