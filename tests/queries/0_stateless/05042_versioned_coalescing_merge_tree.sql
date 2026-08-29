-- VersionedCoalescingMergeTree: like CoalescingMergeTree, but the row with the maximum version
-- (not the last inserted row) provides the non-NULL values.

SET optimize_on_insert = 0;

SELECT 'basic: out-of-order upserts resolved by version';

DROP TABLE IF EXISTS t_vcmt_basic;

CREATE TABLE t_vcmt_basic
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_vcmt_basic';

-- The rows with the higher versions are inserted first.
INSERT INTO t_vcmt_basic VALUES (1, 2, 42, NULL), (2, 1, NULL, 'x');
INSERT INTO t_vcmt_basic VALUES (1, 1, 10, 'first'), (2, 2, NULL, NULL);

SELECT 'FINAL before merge';
SELECT * FROM t_vcmt_basic FINAL ORDER BY key;

SELECT 'FINAL with a subset of columns';
SELECT key, a FROM t_vcmt_basic FINAL ORDER BY key;

OPTIMIZE TABLE t_vcmt_basic FINAL;

SELECT 'after OPTIMIZE FINAL';
SELECT * FROM t_vcmt_basic ORDER BY key;

DROP TABLE t_vcmt_basic;

SELECT 'ties: on equal versions the later inserted row wins';

DROP TABLE IF EXISTS t_vcmt_ties;

CREATE TABLE t_vcmt_ties
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

INSERT INTO t_vcmt_ties VALUES (1, 1, 1, 'one');
INSERT INTO t_vcmt_ties VALUES (1, 1, 2, NULL);

OPTIMIZE TABLE t_vcmt_ties FINAL;
SELECT * FROM t_vcmt_ties;

DROP TABLE t_vcmt_ties;

SELECT 'multiple parts with interleaved versions';

DROP TABLE IF EXISTS t_vcmt_parts;

CREATE TABLE t_vcmt_parts
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

INSERT INTO t_vcmt_parts VALUES (1, 3, NULL, 'v3');
INSERT INTO t_vcmt_parts VALUES (1, 1, 11, 'v1');
INSERT INTO t_vcmt_parts VALUES (1, 2, 22, NULL);

OPTIMIZE TABLE t_vcmt_parts FINAL;
SELECT * FROM t_vcmt_parts;

DROP TABLE t_vcmt_parts;

SELECT 'explicit list of columns to coalesce';

DROP TABLE IF EXISTS t_vcmt_columns;

CREATE TABLE t_vcmt_columns
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version, (a))
ORDER BY key;

INSERT INTO t_vcmt_columns VALUES (1, 2, NULL, 'from v2');
INSERT INTO t_vcmt_columns VALUES (1, 1, 7, 'from v1');

-- `a` is coalesced by version, `b` keeps a value of one of the rows, the version is the maximum one.
OPTIMIZE TABLE t_vcmt_columns FINAL;
SELECT key, version, a FROM t_vcmt_columns;

DROP TABLE t_vcmt_columns;

SELECT 'non-Nullable columns keep the value of the row with the maximum version';

DROP TABLE IF EXISTS t_vcmt_plain;

CREATE TABLE t_vcmt_plain
(
    key UInt64,
    version UInt64,
    v String
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

INSERT INTO t_vcmt_plain VALUES (1, 5, 'v5');
INSERT INTO t_vcmt_plain VALUES (1, 3, 'v3');

OPTIMIZE TABLE t_vcmt_plain FINAL;
SELECT * FROM t_vcmt_plain;

DROP TABLE t_vcmt_plain;

SELECT 'rows of a single INSERT are not merged at insert time';

DROP TABLE IF EXISTS t_vcmt_insert;

CREATE TABLE t_vcmt_insert
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

SET optimize_on_insert = 1;
INSERT INTO t_vcmt_insert VALUES (1, 2, 42, NULL), (1, 1, 10, 'first');
SET optimize_on_insert = 0;

SELECT count() FROM t_vcmt_insert;
OPTIMIZE TABLE t_vcmt_insert FINAL;
SELECT * FROM t_vcmt_insert;

DROP TABLE t_vcmt_insert;

SELECT 'tuple element aggregation';

DROP TABLE IF EXISTS t_vcmt_tuple;

CREATE TABLE t_vcmt_tuple
(
    key UInt64,
    version UInt64,
    data Tuple(x Nullable(UInt64), y Nullable(String))
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_vcmt_tuple VALUES (1, 2, (42, NULL));
INSERT INTO t_vcmt_tuple VALUES (1, 1, (10, 'y1'));

OPTIMIZE TABLE t_vcmt_tuple FINAL;
SELECT * FROM t_vcmt_tuple;

DROP TABLE t_vcmt_tuple;
