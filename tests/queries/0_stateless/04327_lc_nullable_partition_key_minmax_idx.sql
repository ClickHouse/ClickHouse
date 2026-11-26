-- Regression test for PR #103952: MinMaxIndex::store() skipped writing the minmax idx file
-- for a LowCardinality(Nullable) partition key column when min/max are NULL, because
-- IDataType::isNullable() is false for LowCardinality(Nullable). On the next part load
-- (DETACH/ATTACH or server restart) checkConsistencyBase() threw NO_FILE_IN_DATA_PART
-- and the part was detached as broken.

SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- LowCardinality(Nullable(Int32)), compact part';

DROP TABLE IF EXISTS t_lc_nullable_partition;

CREATE TABLE t_lc_nullable_partition (c0 LowCardinality(Nullable(Int32)))
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_lc_nullable_partition (c0) VALUES (1), (NULL);

DETACH TABLE t_lc_nullable_partition SYNC;
ATTACH TABLE t_lc_nullable_partition;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_nullable_partition' AND startsWith(name, 'broken');

SELECT c0 FROM t_lc_nullable_partition ORDER BY c0 ASC NULLS LAST;

DROP TABLE t_lc_nullable_partition;

SELECT '-- LowCardinality(Nullable(String)), wide part';

DROP TABLE IF EXISTS t_lc_nullable_string;

CREATE TABLE t_lc_nullable_string (c0 LowCardinality(Nullable(String)))
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0)
SETTINGS allow_nullable_key = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_lc_nullable_string (c0) VALUES ('a'), (NULL);

DETACH TABLE t_lc_nullable_string SYNC;
ATTACH TABLE t_lc_nullable_string;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_nullable_string' AND startsWith(name, 'broken');

SELECT c0 FROM t_lc_nullable_string ORDER BY c0 ASC NULLS LAST;

DROP TABLE t_lc_nullable_string;

SELECT '-- plain Nullable(Int32) control';

DROP TABLE IF EXISTS t_nullable_partition;

CREATE TABLE t_nullable_partition (c0 Nullable(Int32))
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_nullable_partition (c0) VALUES (1), (NULL);

DETACH TABLE t_nullable_partition SYNC;
ATTACH TABLE t_nullable_partition;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_nullable_partition' AND startsWith(name, 'broken');

SELECT c0 FROM t_nullable_partition ORDER BY c0 ASC NULLS LAST;

DROP TABLE t_nullable_partition;

SELECT '-- plain Int32 control';

DROP TABLE IF EXISTS t_plain_partition;

CREATE TABLE t_plain_partition (c0 Int32)
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0);

INSERT INTO t_plain_partition (c0) VALUES (1), (2);

DETACH TABLE t_plain_partition SYNC;
ATTACH TABLE t_plain_partition;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_plain_partition' AND startsWith(name, 'broken');

SELECT c0 FROM t_plain_partition ORDER BY c0 ASC;

DROP TABLE t_plain_partition;

SELECT '-- LowCardinality(Int32) control';

DROP TABLE IF EXISTS t_lc_partition;

CREATE TABLE t_lc_partition (c0 LowCardinality(Int32))
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0);

INSERT INTO t_lc_partition (c0) VALUES (1), (2);

DETACH TABLE t_lc_partition SYNC;
ATTACH TABLE t_lc_partition;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_partition' AND startsWith(name, 'broken');

SELECT c0 FROM t_lc_partition ORDER BY c0 ASC;

DROP TABLE t_lc_partition;

SELECT '-- composite partition key (LowCardinality(Nullable(Int32)), Int32)';

DROP TABLE IF EXISTS t_lc_nullable_composite;

CREATE TABLE t_lc_nullable_composite (c0 LowCardinality(Nullable(Int32)), c1 Int32)
ENGINE = MergeTree() ORDER BY (c1) PARTITION BY (c0, c1)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_lc_nullable_composite (c0, c1) VALUES (1, 10), (NULL, 20);

DETACH TABLE t_lc_nullable_composite SYNC;
ATTACH TABLE t_lc_nullable_composite;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_nullable_composite' AND startsWith(name, 'broken');

SELECT c0, c1 FROM t_lc_nullable_composite ORDER BY c0 ASC NULLS LAST, c1 ASC;

DROP TABLE t_lc_nullable_composite;

SELECT '-- only NULL values';

DROP TABLE IF EXISTS t_lc_nullable_all_null;

CREATE TABLE t_lc_nullable_all_null (c0 LowCardinality(Nullable(Int32)))
ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_lc_nullable_all_null (c0) VALUES (NULL);
INSERT INTO t_lc_nullable_all_null (c0) VALUES (NULL);

DETACH TABLE t_lc_nullable_all_null SYNC;
ATTACH TABLE t_lc_nullable_all_null;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_nullable_all_null' AND startsWith(name, 'broken');

SELECT c0 FROM t_lc_nullable_all_null ORDER BY c0 ASC NULLS LAST;

SELECT '-- only NULL values, after merge';

OPTIMIZE TABLE t_lc_nullable_all_null FINAL;

DETACH TABLE t_lc_nullable_all_null SYNC;
ATTACH TABLE t_lc_nullable_all_null;

SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lc_nullable_all_null' AND startsWith(name, 'broken');

SELECT c0 FROM t_lc_nullable_all_null ORDER BY c0 ASC NULLS LAST;

DROP TABLE t_lc_nullable_all_null;
