-- The version column accepts the same types as the ver parameter of ReplacingMergeTree.
-- Each section inserts the row with the higher version first, so the merge must resolve by value.

SET optimize_on_insert = 0;

SELECT 'Int64 with negative versions';
DROP TABLE IF EXISTS t_vcmt_ver;
CREATE TABLE t_vcmt_ver (key UInt64, version Int64, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, -1, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -2, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Float64 with negative versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Float64, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, -1.5, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -2.5, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
-- A late insert between the two merged versions must win over the older value of b.
INSERT INTO t_vcmt_ver VALUES (1, -2.0, NULL, 'mid');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Int8 with mixed-sign versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Int8, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 1, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -2, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Float32 with negative versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Float32, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, -1.5, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -2.5, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'UInt128 with versions above 2^64, including a late arrival';
CREATE TABLE t_vcmt_ver (key UInt64, version UInt128, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 18446744073709551617, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 5, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
-- The late version is above the persisted version of b, but below the version of a.
INSERT INTO t_vcmt_ver VALUES (1, 18446744073709551616, NULL, 'mid');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Int128 with mixed-sign versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Int128, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 5, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -100000000000000000000, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'UInt256';
CREATE TABLE t_vcmt_ver (key UInt64, version UInt256, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 115792089237316195423570985008687907853269984665640564039457584007913129639935, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 7, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Int256 with mixed-sign versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Int256, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 5, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, -57896044618658097711785492504343953926634992332820282019728792003956564819968, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'DateTime64';
CREATE TABLE t_vcmt_ver (key UInt64, version DateTime64(3, 'UTC'), a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '2024-01-02 00:00:00.500', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '2024-01-01 00:00:00.100', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Date';
CREATE TABLE t_vcmt_ver (key UInt64, version Date, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '2024-02-01', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '2024-01-01', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Date32 with versions around 1970';
CREATE TABLE t_vcmt_ver (key UInt64, version Date32, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '1970-06-01', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '1969-01-01', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'DateTime';
CREATE TABLE t_vcmt_ver (key UInt64, version DateTime('UTC'), a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '2024-01-02 00:00:00', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '2024-01-01 00:00:00', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Time with negative versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Time, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '10:00:00', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '-10:00:00', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'Time64 with negative versions';
CREATE TABLE t_vcmt_ver (key UInt64, version Time64(3), a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, '10:00:00.000', 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, '-10:00:00.000', 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'LowCardinality(UInt64)';
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_vcmt_ver (key UInt64, version LowCardinality(UInt64), a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 2, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 1, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'BFloat16';
CREATE TABLE t_vcmt_ver (key UInt64, version BFloat16, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 2, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 1, 10, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'widening ALTER keeps the persisted versions comparable';
CREATE TABLE t_vcmt_ver (key UInt64, version Int64, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 100, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 50, NULL, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
ALTER TABLE t_vcmt_ver MODIFY COLUMN version Int128;
-- The late row has a version below the persisted version of b, so it must lose.
INSERT INTO t_vcmt_ver VALUES (1, 40, NULL, 'stale');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

CREATE TABLE t_vcmt_ver (key UInt64, version UInt64, a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
INSERT INTO t_vcmt_ver VALUES (1, 100, 42, NULL);
INSERT INTO t_vcmt_ver VALUES (1, 50, NULL, 'low');
OPTIMIZE TABLE t_vcmt_ver FINAL;
ALTER TABLE t_vcmt_ver MODIFY COLUMN version UInt256;
INSERT INTO t_vcmt_ver VALUES (1, 40, NULL, 'stale');
OPTIMIZE TABLE t_vcmt_ver FINAL;
SELECT * FROM t_vcmt_ver;
DROP TABLE t_vcmt_ver;

SELECT 'scale-changing ALTER of the version column is rejected';
CREATE TABLE t_vcmt_ver (key UInt64, version DateTime64(3, 'UTC'), a Nullable(UInt64), b Nullable(String))
ENGINE = VersionedCoalescingMergeTree(version) ORDER BY key;
ALTER TABLE t_vcmt_ver MODIFY COLUMN version DateTime64(6, 'UTC'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vcmt_ver MODIFY COLUMN version Time64(3); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vcmt_ver MODIFY COLUMN version Int64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_vcmt_ver;
