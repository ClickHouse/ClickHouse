-- ALTER MODIFY ORDER BY must not change the sort direction of a retained sorting key column:
-- the parts on disk stay physically sorted the old way, so primary key index analysis would
-- prune the wrong marks and the table would return wrong results. The ASC -> DESC direction is
-- only reachable through the `clickhouse_json` dialect and is covered by the sibling `.sh` test.

DROP TABLE IF EXISTS t_desc_key;
DROP TABLE IF EXISTS t_desc_minimal;
DROP TABLE IF EXISTS t_asc_key;
DROP TABLE IF EXISTS t_desc_trailing;
DROP TABLE IF EXISTS t_attach_src;

-- A descending key column cannot lose its direction while parts exist.
CREATE TABLE t_desc_key (a UInt64, v String) ENGINE = MergeTree PRIMARY KEY a ORDER BY a DESC
SETTINGS index_granularity = 128;
INSERT INTO t_desc_key SELECT number, 'foo' FROM numbers(1000);

ALTER TABLE t_desc_key ADD COLUMN b UInt64, MODIFY ORDER BY (a, b); -- { serverError BAD_ARGUMENTS }

-- The refusal keeps the table readable and the index correct.
SELECT sum(a) FROM t_desc_key WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT sum(a) FROM t_desc_key WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_desc_key WHERE 500 <=> a SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_desc_key WHERE 9223372036854775807 <=> a SETTINGS use_lightweight_primary_key_index_analysis = 0;

-- The minimal shape needs no schema change: rewriting the same key without `DESC` drops the
-- direction on its own, because the command is not recognised as a no-op.
CREATE TABLE t_desc_minimal (a UInt64, v String) ENGINE = MergeTree PRIMARY KEY a ORDER BY a DESC
SETTINGS index_granularity = 128;
INSERT INTO t_desc_minimal SELECT number, 'foo' FROM numbers(1000);

ALTER TABLE t_desc_minimal MODIFY ORDER BY a; -- { serverError BAD_ARGUMENTS }

SELECT sum(a) FROM t_desc_minimal WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- A descending column outside the primary key is equally unsafe: merges and read-in-order use the
-- directions of the whole sorting key, not only of its primary key prefix.
CREATE TABLE t_desc_trailing (a UInt64, b UInt64, v String) ENGINE = MergeTree
PRIMARY KEY a ORDER BY (a, b DESC) SETTINGS index_granularity = 128;
INSERT INTO t_desc_trailing SELECT 1, number, 'foo' FROM numbers(1000);

ALTER TABLE t_desc_trailing ADD COLUMN c UInt64, MODIFY ORDER BY (a, b, c); -- { serverError BAD_ARGUMENTS }

SELECT sum(b) FROM t_desc_trailing WHERE b >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 0;

-- Dropping the reversed column from the sorting key stays allowed: parts sorted by (a, b DESC) are
-- also sorted by (a), so the shorter key still describes an order the data has.
ALTER TABLE t_desc_trailing MODIFY ORDER BY a;

SELECT sum(b) FROM t_desc_trailing WHERE b >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 0;

-- An all-ascending key is untouched: extending it stays allowed and keeps reading correctly.
CREATE TABLE t_asc_key (a UInt64, v String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 128;
INSERT INTO t_asc_key SELECT number, 'foo' FROM numbers(1000);

ALTER TABLE t_asc_key ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);

SELECT sum(a) FROM t_asc_key WHERE a >= 500 SETTINGS use_lightweight_primary_key_index_analysis = 0;

-- Validations that run after this check are still reached on a MODIFY ORDER BY that the check
-- lets through: the sorting key extension is accepted, and the sampling expression is rejected
-- later in the same statement.
ALTER TABLE t_asc_key ADD COLUMN c UInt64, MODIFY ORDER BY (a, b, c),
    MODIFY SAMPLE BY c; -- { serverError BAD_ARGUMENTS }

-- An ALTER that never enters the check is unaffected.
ALTER TABLE t_desc_key MODIFY COLUMN a String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A direction mismatch between two tables is already refused by ATTACH PARTITION.
CREATE TABLE t_attach_src (a UInt64, v String) ENGINE = MergeTree PRIMARY KEY a ORDER BY a
SETTINGS index_granularity = 128;
INSERT INTO t_attach_src SELECT number, 'foo' FROM numbers(128);

ALTER TABLE t_desc_key ATTACH PARTITION tuple() FROM t_attach_src; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_desc_key;
DROP TABLE t_desc_minimal;
DROP TABLE t_asc_key;
DROP TABLE t_desc_trailing;
DROP TABLE t_attach_src;
