-- Tags: no-replicated-database, no-ordinary-database

SET mutations_sync = 1;
SET check_query_single_value_result = 0;

DROP TABLE IF EXISTS t_source_part_is_intact;

CREATE TABLE t_source_part_is_intact (id UInt64, u UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=1, ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_source_part_is_intact SELECT
    number,
    if (number % 11 = 0, number, 0)
FROM numbers(2000);

CHECK TABLE t_source_part_is_intact SETTINGS max_threads = 1;
SELECT 1, count() FROM t_source_part_is_intact;

BEGIN TRANSACTION;
-- size of the file serialization.json is the same in the new part but checksum is different
ALTER TABLE t_source_part_is_intact update u = 0 where u != 0;
ROLLBACK;

CHECK TABLE t_source_part_is_intact SETTINGS max_threads = 1;

BEGIN TRANSACTION;
-- size of the file serialization.json is different in the new part
ALTER TABLE t_source_part_is_intact update u = 1 WHERE 1;
ROLLBACK;

CHECK TABLE t_source_part_is_intact SETTINGS max_threads = 1;

DETACH TABLE t_source_part_is_intact;
ATTACH TABLE t_source_part_is_intact;

CHECK TABLE t_source_part_is_intact SETTINGS max_threads = 1;

DROP TABLE t_source_part_is_intact;
