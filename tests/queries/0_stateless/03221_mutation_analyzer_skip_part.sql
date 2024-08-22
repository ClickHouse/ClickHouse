DROP TABLE IF EXISTS t_mutate_skip_part;

CREATE TABLE t_mutate_skip_part (key UInt64, id UInt64, v1 UInt64, v2 UInt64) ENGINE = MergeTree ORDER BY id PARTITION BY key;

INSERT INTO t_mutate_skip_part SELECT 1, number, number, number FROM numbers(10000);
INSERT INTO t_mutate_skip_part SELECT 2, number, number, number FROM numbers(10000);

SET mutations_sync = 2;

ALTER TABLE t_mutate_skip_part UPDATE v1 = 1000 WHERE key = 1;
ALTER TABLE t_mutate_skip_part DELETE WHERE key = 2 AND v2 % 10 = 0;

SYSTEM FLUSH LOGS;

-- If part is skipped in mutation and hardlinked then read_rows must be 0.
SELECT part_name, read_rows
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mutate_skip_part' AND event_type = 'MutatePart'
ORDER BY part_name;

DROP TABLE IF EXISTS t_mutate_skip_part;
