DROP TABLE IF EXISTS t_delete_empty_part;

CREATE TABLE t_delete_empty_part (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY a;

INSERT INTO t_delete_empty_part SELECT 1, number FROM numbers(1000);
INSERT INTO t_delete_empty_part SELECT 2, number FROM numbers(1000);
INSERT INTO t_delete_empty_part SELECT 3, number FROM numbers(2000, 1000);

SET mutations_sync = 2;
ALTER TABLE t_delete_empty_part DELETE WHERE a = 2 OR b < 500;

SELECT count() FROM t_delete_empty_part;

SYSTEM FLUSH LOGS part_log;

SELECT
    part_name,
    ProfileEvents['MutationTotalParts'],
    ProfileEvents['MutationUntouchedParts'],
    ProfileEvents['MutationCreatedEmptyParts']
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_delete_empty_part' AND event_type = 'MutatePart'
ORDER BY part_name;

DROP TABLE t_delete_empty_part;
