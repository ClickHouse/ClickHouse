-- A `bloom_filter` index over `Array(Nullable(T))` used to pass DDL validation and then fail every
-- insert, merge and mutation of the table, with `DROP INDEX` refused while such a mutation was pending.

DROP TABLE IF EXISTS t_bloom_array_nullable;

CREATE TABLE t_bloom_array_nullable (id UInt32, a Array(Nullable(String)), INDEX ix a TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_bloom_array_nullable (id UInt32, a Array(Nullable(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_bloom_array_nullable VALUES (1, ['x', NULL]);
ALTER TABLE t_bloom_array_nullable ADD INDEX ix a TYPE bloom_filter GRANULARITY 1; -- { serverError ILLEGAL_COLUMN }

-- The table keeps working.
INSERT INTO t_bloom_array_nullable VALUES (2, ['y']);
OPTIMIZE TABLE t_bloom_array_nullable FINAL;
SELECT count() FROM t_bloom_array_nullable;

DROP TABLE t_bloom_array_nullable;

-- The shapes the index does support are unaffected.
DROP TABLE IF EXISTS t_bloom_supported;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_bloom_supported
(
    id UInt32,
    s Nullable(String),
    a Array(String),
    lc Array(LowCardinality(Nullable(String))),
    INDEX ix_s s TYPE bloom_filter GRANULARITY 1,
    INDEX ix_a a TYPE bloom_filter GRANULARITY 1,
    INDEX ix_lc lc TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_bloom_supported VALUES (1, 'x', ['x'], ['x', NULL]), (2, NULL, ['y'], ['y']);
SELECT count() FROM t_bloom_supported WHERE s = 'x';
SELECT count() FROM t_bloom_supported WHERE has(a, 'y');
SELECT count() FROM t_bloom_supported WHERE has(lc, 'x');

DROP TABLE t_bloom_supported;
