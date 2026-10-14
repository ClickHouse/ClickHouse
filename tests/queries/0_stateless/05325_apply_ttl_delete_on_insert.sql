-- The table setting `apply_ttl_delete_on_insert` removes the rows already expired by the table `TTL ... DELETE` on `INSERT`.

DROP TABLE IF EXISTS t_ttl_insert;
DROP TABLE IF EXISTS t_ttl_insert_partitions;
DROP TABLE IF EXISTS t_ttl_insert_replicated;

CREATE TABLE t_ttl_insert (d DateTime, x UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY x TTL d + INTERVAL 1 DAY;

-- Without the setting the expired rows are written, one part per partition.
SYSTEM STOP MERGES t_ttl_insert;
INSERT INTO t_ttl_insert SELECT now() - INTERVAL number DAY, number FROM numbers(5);
SELECT 'disabled', count(), (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active) FROM t_ttl_insert;
TRUNCATE TABLE t_ttl_insert;

ALTER TABLE t_ttl_insert MODIFY SETTING apply_ttl_delete_on_insert = 1;

-- With the setting only the row which is not expired is written, and no part is created for the expired partitions.
INSERT INTO t_ttl_insert SELECT now() - INTERVAL number DAY, number FROM numbers(5);
SELECT 'enabled', x FROM t_ttl_insert ORDER BY x;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active;

-- The TTL info of the written part describes the kept row only, so the part is not selected for a TTL merge before its time.
SELECT 'ttl_info', delete_ttl_info_min > now(), delete_ttl_info_min = delete_ttl_info_max FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active;

-- A block of only expired rows writes nothing.
INSERT INTO t_ttl_insert SELECT now() - INTERVAL 10 + number DAY, number FROM numbers(3);
SELECT 'all expired', count() FROM t_ttl_insert;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active;

-- A partition whose rows are all expired does not count towards `max_partitions_per_insert_block`.
CREATE TABLE t_ttl_insert_partitions (d DateTime, x UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY x TTL d + INTERVAL 1 DAY
SETTINGS apply_ttl_delete_on_insert = 1;

INSERT INTO t_ttl_insert_partitions SELECT now() - INTERVAL number DAY, number FROM numbers(6) SETTINGS max_partitions_per_insert_block = 2;
SELECT 'partitions', arraySort(groupArray(x)) FROM t_ttl_insert_partitions;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert_partitions' AND active;

-- The limit still applies to the partitions of the rows which are not expired.
INSERT INTO t_ttl_insert_partitions SELECT now() + INTERVAL number DAY, number FROM numbers(3) SETTINGS max_partitions_per_insert_block = 2; -- { serverError TOO_MANY_PARTS }

-- The same for `ReplicatedMergeTree`.
CREATE TABLE t_ttl_insert_replicated (d DateTime, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_ttl_insert_replicated', 'r1') PARTITION BY toYYYYMMDD(d) ORDER BY x TTL d + INTERVAL 1 DAY
SETTINGS apply_ttl_delete_on_insert = 1;

INSERT INTO t_ttl_insert_replicated SELECT now() - INTERVAL number DAY, number FROM numbers(5);
SELECT 'replicated', arraySort(groupArray(x)) FROM t_ttl_insert_replicated;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert_replicated' AND active;

-- The deduplication hash is computed from the inserted block before the expired rows are removed,
-- so a repeated insert of the same block is deduplicated.
INSERT INTO t_ttl_insert_replicated VALUES ('2000-01-01 00:00:00', 10), ('2100-01-01 00:00:00', 11);
INSERT INTO t_ttl_insert_replicated VALUES ('2000-01-01 00:00:00', 10), ('2100-01-01 00:00:00', 11);
SELECT 'deduplicated', arraySort(groupArray(x)) FROM t_ttl_insert_replicated;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert_replicated' AND active;

DROP TABLE t_ttl_insert;
DROP TABLE t_ttl_insert_partitions;
DROP TABLE t_ttl_insert_replicated;
