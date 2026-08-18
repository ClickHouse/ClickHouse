-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

SET allow_experimental_codecs = 1;

SELECT 'CREATE, rejected';

CREATE TABLE t_key (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY i; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_stacked (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01), LZ4), f Int64)
ENGINE = MergeTree ORDER BY i; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_expression_key (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY round(i); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_array_key (c Array(Float64) CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY c; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_subcolumn_key (t Tuple(Float64, Float64) CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY t.1; -- { serverError BAD_ARGUMENTS }

SELECT 'CREATE, accepted';

CREATE TABLE t_non_key (k Int64, i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)))
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_lossless_key (i Float64 CODEC(Delta, LZ4), f Int64) ENGINE = MergeTree ORDER BY i;

CREATE TABLE t_lossless_array_key (c Array(Int64) CODEC(T64), f Int64) ENGINE = MergeTree ORDER BY c;

SELECT 'ALTER, rejected';

CREATE TABLE t_alter (k Int64, i Float64) ENGINE = MergeTree ORDER BY (k, i);
ALTER TABLE t_alter MODIFY COLUMN i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_alter ADD COLUMN n Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), MODIFY ORDER BY (k, i, n); -- { serverError BAD_ARGUMENTS }

SELECT 'ALTER, accepted';

ALTER TABLE t_non_key ADD COLUMN z Int64;
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_non_key';

-- Only what an ALTER introduces is validated, so re-stating a codec the column already carries
-- is accepted.
ALTER TABLE t_non_key MODIFY COLUMN i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01));

DROP TABLE t_alter;
DROP TABLE t_non_key;
DROP TABLE t_lossless_key;
DROP TABLE t_lossless_array_key;
