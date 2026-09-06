-- Tags: replica, no-tsan, no-parallel
-- Tag no-tsan: RESTART REPLICAS can acquire too much locks, while only 64 is possible from one thread under TSan

DROP TABLE IF EXISTS data_01646;
CREATE TABLE data_01646 (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01646/data_01646', 'r') ORDER BY s PARTITION BY x;
SYSTEM RESTART REPLICAS;
DESCRIBE TABLE data_01646;
DROP TABLE data_01646;

-- A restart reparses the stored definition, which must load like stored metadata and not be
-- revalidated as fresh user input: otherwise a definition that only a permissive session could
-- create is refused and the table is left detached.
CREATE TABLE data_01646_ttl (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01646/data_01646_ttl', 'r')
    ORDER BY s TTL x + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1), LZ4) SETTINGS allow_suspicious_ttl_expressions = 1;
SYSTEM RESTART REPLICA data_01646_ttl;
DESCRIBE TABLE data_01646_ttl;
DROP TABLE data_01646_ttl;
