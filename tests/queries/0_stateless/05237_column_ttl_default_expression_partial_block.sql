-- A column TTL on a column with a per-row DDL DEFAULT: after the merge every expired row must read
-- the DEFAULT evaluated for its own row, and live rows in the same block must keep their values.

DROP TABLE IF EXISTS t_column_ttl_default_expr;

CREATE TABLE t_column_ttl_default_expr
(
    d DateTime,
    k UInt32,
    v Int32 DEFAULT k + 1000 TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree
ORDER BY k
SETTINGS merge_max_block_size = 8192;

-- Even k stays live, odd k expires, so live rows precede expired ones inside the merged block.
INSERT INTO t_column_ttl_default_expr (d, k, v)
SELECT if(number % 2 = 0, toDateTime('2100-01-01 00:00:00'), toDateTime('2000-01-01 00:00:00')), number, number
FROM numbers(1000);

OPTIMIZE TABLE t_column_ttl_default_expr FINAL;

SELECT count(), countIf(v = k), countIf(v = k + 1000) FROM t_column_ttl_default_expr;
SELECT k, v FROM t_column_ttl_default_expr WHERE k < 4 ORDER BY k;

DROP TABLE t_column_ttl_default_expr;
