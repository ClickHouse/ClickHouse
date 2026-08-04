-- A projection's `COLUMNS(...) APPLY f` transformer keeps the function name exactly as it was
-- spelled, so `APPLY SUM` and `APPLY sum` were stored as different definitions and compared
-- unequal. Stored table definitions are compared as ASTs after normalizing function names, and the
-- normalization must reach the `APPLY` transformer as well: its function lives in the non-child
-- `func_name` string, and its parameters and lambda are outside `children`.
--
-- The normalization is comparison-only: the stored definition must keep the transformer exactly
-- as written, because older replicas compare the serialized `projections` metadata field
-- byte-for-byte, and every released version stores the `APPLY` spelling as written.

DROP TABLE IF EXISTS t_apply_src_04693;
DROP TABLE IF EXISTS t_apply_dst_04693;
DROP TABLE IF EXISTS t_apply_lambda_src_04693;
DROP TABLE IF EXISTS t_apply_lambda_dst_04693;
DROP TABLE IF EXISTS t_apply_params_src_04693;
DROP TABLE IF EXISTS t_apply_params_dst_04693;
DROP TABLE IF EXISTS t_apply_neg_src_04693;
DROP TABLE IF EXISTS t_apply_neg_dst_04693;

-- `APPLY SUM` vs `APPLY sum`.
CREATE TABLE t_apply_src_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY SUM GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_apply_dst_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY sum GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_apply_src_04693 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3);
ALTER TABLE t_apply_dst_04693 ATTACH PARTITION 1 FROM t_apply_src_04693;
SELECT a, b, c FROM t_apply_dst_04693 ORDER BY a, b, c;

-- The lambda form `APPLY (x -> f(x))` is not in `children` either.
CREATE TABLE t_apply_lambda_src_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY (x -> SuM(x)) GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_apply_lambda_dst_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY (x -> sum(x)) GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_apply_lambda_src_04693 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3);
ALTER TABLE t_apply_lambda_dst_04693 ATTACH PARTITION 1 FROM t_apply_lambda_src_04693;
SELECT a, b, c FROM t_apply_lambda_dst_04693 ORDER BY a, b, c;

-- The parameters of a parametric `APPLY` are outside `children` too, so the function names inside
-- them are normalized as well.
CREATE TABLE t_apply_params_src_04693 (a UInt32, b Float64, c Float64,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY quantile(ABS(-0.9)) GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_apply_params_dst_04693 (a UInt32, b Float64, c Float64,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY quantile(abs(-0.9)) GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_apply_params_src_04693 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3);
ALTER TABLE t_apply_params_dst_04693 ATTACH PARTITION 1 FROM t_apply_params_src_04693;
SELECT a, b, c FROM t_apply_params_dst_04693 ORDER BY a, b, c;

-- The comparison is insensitive to the spelling, but the stored definition preserves it: the
-- `projections` metadata field is compared byte-for-byte by older replicas, so the write path
-- must not rewrite what the user wrote.
SELECT
    create_table_query LIKE '%APPLY SUM%',
    create_table_query LIKE '%APPLY sum%'
    FROM system.tables WHERE database = currentDatabase() AND name = 't_apply_src_04693';
SELECT
    create_table_query LIKE '%SuM(x)%',
    create_table_query LIKE '%sum(x)%'
    FROM system.tables WHERE database = currentDatabase() AND name = 't_apply_lambda_src_04693';
SELECT
    create_table_query LIKE '%quantile(ABS(-0.9))%',
    create_table_query LIKE '%quantile(abs(-0.9))%'
    FROM system.tables WHERE database = currentDatabase() AND name = 't_apply_params_src_04693';

-- Genuinely different functions must still be rejected: only the spelling is canonicalized.
CREATE TABLE t_apply_neg_src_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY sum GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_apply_neg_dst_04693 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (SELECT a, COLUMNS('b|c') APPLY max GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_apply_neg_src_04693 VALUES (1, 1, 1);
ALTER TABLE t_apply_neg_dst_04693 ATTACH PARTITION 1 FROM t_apply_neg_src_04693; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_apply_src_04693;
DROP TABLE t_apply_dst_04693;
DROP TABLE t_apply_lambda_src_04693;
DROP TABLE t_apply_lambda_dst_04693;
DROP TABLE t_apply_params_src_04693;
DROP TABLE t_apply_params_dst_04693;
DROP TABLE t_apply_neg_src_04693;
DROP TABLE t_apply_neg_dst_04693;
