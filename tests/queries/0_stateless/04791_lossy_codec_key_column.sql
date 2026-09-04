-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

-- Test that the table primary key and partition key cannot be compressed by a lossy codec like SZ3.

SET enable_sz3_codec = 1;

SELECT 'CREATE, rejected';

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY i; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01), LZ4), f Int64)
ENGINE = MergeTree ORDER BY i; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY round(i); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), name String)
ENGINE = MergeTree ORDER BY (i + 1, name); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64, m Float64 MATERIALIZED i + 1 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY m; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (c Array(Float64) CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY c; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (t Tuple(Float64, Float64) CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY t.1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY f PARTITION BY intDiv(toInt64(i), 100); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (c Array(Float64) CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY f PARTITION BY toInt64(c[1]); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab (i Float64, m Float64 MATERIALIZED i + 1 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY f PARTITION BY intDiv(toInt64(m), 100); -- { serverError BAD_ARGUMENTS }

SELECT 'CREATE, accepted';

CREATE TABLE tab (k Int64, i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)))
ENGINE = MergeTree ORDER BY k;
DROP TABLE tab;

CREATE TABLE tab (i Float64 CODEC(Delta, LZ4), f Int64) ENGINE = MergeTree ORDER BY i;
DROP TABLE tab;

CREATE TABLE tab (c Array(Int64) CODEC(T64), f Int64) ENGINE = MergeTree ORDER BY c;
DROP TABLE tab;

CREATE TABLE tab (i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), f Int64)
ENGINE = MergeTree ORDER BY f PARTITION BY f;
DROP TABLE tab;

CREATE TABLE tab (i Float64 CODEC(Delta, LZ4), f Int64)
ENGINE = MergeTree ORDER BY f PARTITION BY intDiv(toInt64(i), 100);
DROP TABLE tab;

SELECT 'ALTER, rejected';

CREATE TABLE tab (k Int64, i Float64) ENGINE = MergeTree ORDER BY (k, i);

ALTER TABLE tab MODIFY COLUMN i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)); -- { serverError BAD_ARGUMENTS }

ALTER TABLE tab ADD COLUMN n Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), MODIFY ORDER BY (k, i, n); -- { serverError BAD_ARGUMENTS }

SELECT 'ALTER, accepted';

ALTER TABLE tab ADD COLUMN z Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01));
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'tab';

DROP TABLE tab;

SELECT 'ALTER on the partition key, rejected';

CREATE TABLE tab (k Int64, i Float64) ENGINE = MergeTree ORDER BY k PARTITION BY intDiv(toInt64(i), 100);

ALTER TABLE tab MODIFY COLUMN i Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)); -- { serverError BAD_ARGUMENTS }

SELECT 'ALTER on the partition key, accepted';

ALTER TABLE tab ADD COLUMN z Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01));
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'tab';

DROP TABLE tab;
