-- Tags: shard


DROP TABLE IF EXISTS data_04070;
DROP VIEW IF EXISTS inner_view_04070;
DROP VIEW IF EXISTS outer_view_04070;
DROP VIEW IF EXISTS union_view_04070;
DROP VIEW IF EXISTS renamed_view_04070;
DROP VIEW IF EXISTS agg_view_04070;
DROP VIEW IF EXISTS where_view_04070;

CREATE TABLE data_04070 (key Int64, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO data_04070 SELECT number, toString(number) FROM numbers(10);

SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 1;

-- Nested views: outer_view -> inner_view -> remote()
CREATE VIEW inner_view_04070 AS SELECT * FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2);
CREATE VIEW outer_view_04070 AS SELECT * FROM inner_view_04070;
SELECT key, value FROM outer_view_04070 WHERE key = 0 ORDER BY key, value;

-- UNION ALL view
CREATE VIEW union_view_04070 AS
    SELECT * FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2)
    UNION ALL
    SELECT * FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2);
SELECT key, value FROM union_view_04070 WHERE key = 0 ORDER BY key, value;

-- View with column rename
CREATE VIEW renamed_view_04070 AS SELECT key AS k, value AS v FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2);
SELECT k, v FROM renamed_view_04070 WHERE k = 0 ORDER BY k, v;

-- View with aggregation
CREATE VIEW agg_view_04070 AS SELECT key, count() AS cnt FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2) GROUP BY key;
SELECT key, cnt FROM agg_view_04070 WHERE key = 0 ORDER BY key;

-- View with existing WHERE clause
CREATE VIEW where_view_04070 AS SELECT * FROM remote('127.0.0.1:19000,127.0.0.1:19000', currentDatabase(), data_04070, key % 2) WHERE value != '';
SELECT key, value FROM where_view_04070 WHERE key = 0 ORDER BY key, value;

DROP VIEW where_view_04070;
DROP VIEW agg_view_04070;
DROP VIEW renamed_view_04070;
DROP VIEW union_view_04070;
DROP VIEW outer_view_04070;
DROP VIEW inner_view_04070;
DROP TABLE data_04070;
