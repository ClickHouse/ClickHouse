-- The setting `apply_ttl_delete_on_insert` removes the rows already expired by a table-level `TTL ... DELETE` on `INSERT`.

DROP TABLE IF EXISTS t_ttl_insert;
DROP TABLE IF EXISTS t_ttl_insert_where;
DROP TABLE IF EXISTS t_ttl_insert_replicated;

CREATE TABLE t_ttl_insert (d DateTime, x UInt64, c UInt64 TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY x TTL d + INTERVAL 1 DAY;

-- Without the setting the expired rows are written, one part per partition.
SYSTEM STOP MERGES t_ttl_insert;
INSERT INTO t_ttl_insert SELECT now() - INTERVAL number DAY, number, number FROM numbers(5) SETTINGS apply_ttl_delete_on_insert = 0;
SELECT 'disabled', count(), (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active) FROM t_ttl_insert;
TRUNCATE TABLE t_ttl_insert;

-- With the setting only the row which is not expired is written, and no part is created for the expired partitions.
-- The column-level TTL is not applied.
INSERT INTO t_ttl_insert SELECT now() - INTERVAL number DAY, number, number + 100 FROM numbers(5) SETTINGS apply_ttl_delete_on_insert = 1;
SELECT 'enabled', x, c FROM t_ttl_insert ORDER BY x;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active;

-- A block of only expired rows writes nothing.
INSERT INTO t_ttl_insert SELECT now() - INTERVAL 10 + number DAY, number, number FROM numbers(3) SETTINGS apply_ttl_delete_on_insert = 1;
SELECT 'all expired', count() FROM t_ttl_insert;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert' AND active;

-- `TTL ... DELETE WHERE` removes only the expired rows matching the condition.
CREATE TABLE t_ttl_insert_where (d DateTime, x UInt64)
ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE x % 2 = 0;

INSERT INTO t_ttl_insert_where SELECT now() - INTERVAL number DAY, number FROM numbers(6) SETTINGS apply_ttl_delete_on_insert = 1;
SELECT 'where', groupArray(x) FROM (SELECT x FROM t_ttl_insert_where ORDER BY x);

-- The same for `ReplicatedMergeTree`.
CREATE TABLE t_ttl_insert_replicated (d DateTime, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_ttl_insert_replicated', 'r1') PARTITION BY toYYYYMMDD(d) ORDER BY x TTL d + INTERVAL 1 DAY;

INSERT INTO t_ttl_insert_replicated SELECT now() - INTERVAL number DAY, number FROM numbers(5) SETTINGS apply_ttl_delete_on_insert = 1;
SELECT 'replicated', groupArray(x) FROM t_ttl_insert_replicated;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_insert_replicated' AND active;

DROP TABLE t_ttl_insert;
DROP TABLE t_ttl_insert_where;
DROP TABLE t_ttl_insert_replicated;
