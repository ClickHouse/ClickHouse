DROP TABLE IF EXISTS with_deduplication;
DROP TABLE IF EXISTS without_deduplication;
DROP TABLE IF EXISTS with_deduplication_mv;
DROP TABLE IF EXISTS without_deduplication_mv;

CREATE TABLE with_deduplication(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/with_deduplication', 'r1') ORDER BY x;
CREATE TABLE without_deduplication(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/test/without_deduplication', 'r1') ORDER BY x SETTINGS replicated_deduplication_window = 0;

CREATE MATERIALIZED VIEW with_deduplication_mv
    ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/with_deduplication_mv', 'r1') ORDER BY dummy
    AS SELECT 0 AS dummy, countState(x) AS cnt FROM with_deduplication;
CREATE MATERIALIZED VIEW without_deduplication_mv
    ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/test/without_deduplication_mv', 'r1') ORDER BY dummy
    AS SELECT 0 AS dummy, countState(x) AS cnt FROM without_deduplication;

INSERT INTO with_deduplication VALUES (42);
INSERT INTO with_deduplication VALUES (42);
INSERT INTO with_deduplication VALUES (43);

INSERT INTO without_deduplication VALUES (42);
INSERT INTO without_deduplication VALUES (42);
INSERT INTO without_deduplication VALUES (43);

SELECT count() FROM with_deduplication;
SELECT count() FROM without_deduplication;

-- Implicit insert isn't deduplicated
SELECT '';
SELECT countMerge(cnt) FROM with_deduplication_mv;
SELECT countMerge(cnt) FROM without_deduplication_mv;

-- Explicit insert is deduplicated
ALTER TABLE `.inner.with_deduplication_mv` DROP PARTITION ID 'all';
ALTER TABLE `.inner.without_deduplication_mv` DROP PARTITION ID 'all';
INSERT INTO `.inner.with_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO `.inner.with_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO `.inner.without_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;
INSERT INTO `.inner.without_deduplication_mv` SELECT 0 AS dummy, arrayReduce('countState', [toUInt32(42)]) AS cnt;

SELECT '';
SELECT countMerge(cnt) FROM with_deduplication_mv;
SELECT countMerge(cnt) FROM without_deduplication_mv;

DROP TABLE IF EXISTS with_deduplication;
DROP TABLE IF EXISTS without_deduplication;
DROP TABLE IF EXISTS with_deduplication_mv;
DROP TABLE IF EXISTS without_deduplication_mv;
