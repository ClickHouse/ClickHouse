DROP TABLE IF EXISTS test.with_deduplication;
DROP TABLE IF EXISTS test.without_deduplication;
DROP TABLE IF EXISTS test.with_deduplication_mv;
DROP TABLE IF EXISTS test.without_deduplication_mv;

CREATE TABLE test.with_deduplication(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/with_deduplication', 'r1') ORDER BY x;
CREATE TABLE test.without_deduplication(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/without_deduplication', 'r1') ORDER BY x SETTINGS replicated_deduplication_window = 0;

CREATE MATERIALIZED VIEW test.with_deduplication_mv
    ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/with_deduplication_mv', 'r1') ORDER BY dummy
    AS SELECT 0 AS dummy, countState(x) AS cnt FROM test.with_deduplication;
CREATE MATERIALIZED VIEW test.without_deduplication_mv
    ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/without_deduplication_mv', 'r1') ORDER BY dummy
    AS SELECT 0 AS dummy, countState(x) AS cnt FROM test.without_deduplication;

INSERT INTO test.with_deduplication VALUES (42);
INSERT INTO test.with_deduplication VALUES (42);
INSERT INTO test.with_deduplication VALUES (43);

INSERT INTO test.without_deduplication VALUES (42);
INSERT INTO test.without_deduplication VALUES (42);
INSERT INTO test.without_deduplication VALUES (43);

SELECT count() FROM test.with_deduplication;
SELECT count() FROM test.without_deduplication;

-- Implicit insert isn't deduplicated
SELECT '';
SELECT countMerge(cnt) FROM test.with_deduplication_mv;
SELECT countMerge(cnt) FROM test.without_deduplication_mv;

-- Explicit insert is deduplicated
ALTER TABLE test.`.inner.with_deduplication_mv` DROP PARTITION ID 'all';
ALTER TABLE test.`.inner.without_deduplication_mv` DROP PARTITION ID 'all';
INSERT INTO test.`.inner.with_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO test.`.inner.with_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO test.`.inner.without_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO test.`.inner.without_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;

SELECT '';
SELECT countMerge(cnt) FROM test.with_deduplication_mv;
SELECT countMerge(cnt) FROM test.without_deduplication_mv;

DROP TABLE IF EXISTS test.with_deduplication;
DROP TABLE IF EXISTS test.without_deduplication;
DROP TABLE IF EXISTS test.with_deduplication_mv;
DROP TABLE IF EXISTS test.without_deduplication_mv;
