DROP TABLE IF EXISTS t_drop_partition_key;

CREATE TABLE t_drop_partition_key
(
    d Date,
    id UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id;

SYSTEM STOP MERGES t_drop_partition_key;

INSERT INTO t_drop_partition_key VALUES ('2024-01-01', 1), ('2024-02-01', 2);

SELECT count() FROM t_drop_partition_key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_partition_key' AND active;

ALTER TABLE t_drop_partition_key DROP PARTITION KEY;

SELECT count() FROM t_drop_partition_key;
SELECT positionCaseInsensitive(create_table_query, 'PARTITION BY') FROM system.tables
WHERE database = currentDatabase() AND name = 't_drop_partition_key';
SELECT _partition_value FROM t_drop_partition_key LIMIT 1; -- { serverError UNKNOWN_IDENTIFIER }

DETACH TABLE t_drop_partition_key;
ATTACH TABLE t_drop_partition_key;

SELECT count() FROM t_drop_partition_key;

INSERT INTO t_drop_partition_key VALUES ('2024-03-01', 3);
SELECT count(), uniqExact(_partition_id) FROM t_drop_partition_key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_partition_key' AND active;

ALTER TABLE t_drop_partition_key DROP PARTITION KEY; -- { serverError BAD_ARGUMENTS }

SYSTEM START MERGES t_drop_partition_key;
DROP TABLE t_drop_partition_key;

CREATE TABLE t_drop_partition_key_no_key (id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_drop_partition_key_no_key DROP PARTITION KEY; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_drop_partition_key_no_key;
