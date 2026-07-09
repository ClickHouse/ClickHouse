-- Tags: no-parallel-replicas
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104263
--
-- PR #98242 (commit ebc5cb49baa594b87f4631850ae9902424414113, 26.3) introduced an
-- unconditional `skip_partition_pruning = !exprs_match && !columns_match` branch in
-- `ReadFromMergeTree::deferFiltersAfterFinalIfNeeded`. The intent was correctness for
-- FINAL queries that may need to deduplicate same-primary-key rows across partitions.
-- Side effect: all FINAL queries whose partition-key column is not in the sorting key
-- stop using partition pruning, even when the WHERE references only the partition column.
--
-- Setting `defer_partition_pruning_after_final` (default 1) gates the new branch:
--   1 -> deferred pruning, correctness-safe (default; 26.3+ behavior)
--   0 -> pre-26.3 pruning before FINAL, valid when same-PK rows cannot span partitions

DROP TABLE IF EXISTS repro_104263 SYNC;

CREATE TABLE repro_104263
(
    pk         UUID,
    event_time DateTime64(3, 'UTC'),
    version    UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY pk;

INSERT INTO repro_104263 SELECT generateUUIDv4(), toDateTime64('2026-01-15 00:00:00', 3, 'UTC'), 1 FROM numbers(1);
INSERT INTO repro_104263 SELECT generateUUIDv4(), toDateTime64('2026-02-15 00:00:00', 3, 'UTC'), 1 FROM numbers(1);
INSERT INTO repro_104263 SELECT generateUUIDv4(), toDateTime64('2026-03-15 00:00:00', 3, 'UTC'), 1 FROM numbers(1);
INSERT INTO repro_104263 SELECT generateUUIDv4(), toDateTime64('2026-04-15 00:00:00', 3, 'UTC'), 1 FROM numbers(1);
INSERT INTO repro_104263 SELECT generateUUIDv4(), toDateTime64('2026-05-15 00:00:00', 3, 'UTC'), 1 FROM numbers(1);

-- Both modes return the same row count (one PK in March matches the filter).
SELECT 'default-defer count', count() FROM repro_104263 FINAL
WHERE event_time >= '2026-03-01' AND event_time <= '2026-03-31';

SELECT 'opt-out count', count() FROM repro_104263 FINAL
WHERE event_time >= '2026-03-01' AND event_time <= '2026-03-31'
SETTINGS defer_partition_pruning_after_final = 0;

-- The behavioral difference shows up in the `Partition Min-Max` index step of EXPLAIN.
-- Default reads all 5 parts (pruning disabled). Opt-out prunes to 1.
SELECT 'default-defer partition pruning',
       countIf(explain LIKE '%Partition Min-Max%') AS has_partition_step,
       countIf(explain LIKE '%Parts: 5/5%')        AS no_pruning,
       countIf(explain LIKE '%Parts: 1/5%')        AS pruned
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM repro_104263 FINAL
    WHERE event_time >= '2026-03-01' AND event_time <= '2026-03-31'
);

SELECT 'opt-out partition pruning',
       countIf(explain LIKE '%Partition Min-Max%') AS has_partition_step,
       countIf(explain LIKE '%Parts: 5/5%')        AS no_pruning,
       countIf(explain LIKE '%Parts: 1/5%')        AS pruned
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM repro_104263 FINAL
    WHERE event_time >= '2026-03-01' AND event_time <= '2026-03-31'
    SETTINGS defer_partition_pruning_after_final = 0
);

DROP TABLE repro_104263 SYNC;


-- Pathological case from the issue discussion: same primary key in two partitions with
-- reversed versions. Strict semantics (default) requires reading both partitions so
-- FINAL can pick the cross-partition winner. Opt-out trusts the user's assertion that
-- same-PK rows do not span partitions and prunes before FINAL.
--
-- Default (defer = 1):
--   read both -> FINAL picks (Jan, v=2) -> filter `event_time >= Feb` rejects -> 0 rows.
-- Opt-out (defer = 0):
--   prune to Feb only -> FINAL on {(Feb, v=1)} -> filter accepts -> 1 row.

DROP TABLE IF EXISTS repro_104263_path SYNC;

CREATE TABLE repro_104263_path
(
    pk         UInt64,
    event_time DateTime,
    version    UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY pk;

INSERT INTO repro_104263_path VALUES (1, '2026-01-15 00:00:00', 2);
INSERT INTO repro_104263_path VALUES (1, '2026-02-15 00:00:00', 1);

SELECT 'pathological default-defer', count()
FROM repro_104263_path FINAL
WHERE event_time >= '2026-02-01';

SELECT 'pathological opt-out', count()
FROM repro_104263_path FINAL
WHERE event_time >= '2026-02-01'
SETTINGS defer_partition_pruning_after_final = 0;

DROP TABLE repro_104263_path SYNC;
