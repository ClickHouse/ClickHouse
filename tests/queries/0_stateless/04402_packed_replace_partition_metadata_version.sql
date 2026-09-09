-- Tags: no-replicated-database
-- REPLACE PARTITION into a ReplicatedMergeTree table sets metadata_version_to_write, so
-- cloneAndLoadDataPart writes metadata_version.txt into the cloned part after freeze. For packed
-- part storage this write needs an active packed transaction; otherwise writeFile throws
-- NOT_INITIALIZED. This is a regression test for that path.

DROP TABLE IF EXISTS t_packed_src SYNC;
DROP TABLE IF EXISTS t_packed_dst SYNC;

CREATE TABLE t_packed_src (k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY (k % 2) ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '100G';

CREATE TABLE t_packed_dst (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_packed_dst', '1')
PARTITION BY (k % 2) ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '100G';

INSERT INTO t_packed_src SELECT number, number FROM numbers(1000);

ALTER TABLE t_packed_dst REPLACE PARTITION 0 FROM t_packed_src;

SELECT part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_dst' AND active
GROUP BY ALL;

SELECT count(), sum(k), sum(v) FROM t_packed_dst;

DROP TABLE t_packed_src SYNC;
DROP TABLE t_packed_dst SYNC;
