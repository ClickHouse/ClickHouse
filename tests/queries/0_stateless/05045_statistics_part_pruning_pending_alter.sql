-- Statistics-based part pruning must not use a part's stale statistics while a pending ALTER
-- changes what a column's values (or its name) mean without rewriting the part: reads already
-- apply the change on the fly, while the part's min/max statistics still describe the original
-- data. Covered: MODIFY COLUMN type change, DROP + RENAME into the freed name, DROP + re-ADD.

DROP TABLE IF EXISTS stats_pending;

SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';

CREATE TABLE stats_pending
(
    x Float64 STATISTICS(basic),
    s String,
    u Int64 STATISTICS(basic),
    rx Int64 STATISTICS(basic),
    ry Int64 STATISTICS(basic),
    dx Int64 STATISTICS(basic),
    sr String
)
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES stats_pending; -- keep the alter mutation pending
INSERT INTO stats_pending VALUES (10.5, 'a', 10, 1, 100, 1, 'a'), (11.5, 'b', 20, 2, 200, 2, 'b');
INSERT INTO stats_pending VALUES (100.5, 'c', 100, 3, 300, 3, 'c');

ALTER TABLE stats_pending
    MODIFY COLUMN x Int64,
    MODIFY COLUMN s LowCardinality(String),
    DROP COLUMN rx, RENAME COLUMN ry TO rx,
    DROP COLUMN dx, ADD COLUMN dx Int64 DEFAULT 42 STATISTICS(basic),
    RENAME COLUMN sr TO sr2
SETTINGS alter_sync = 0, mutations_sync = 0;

-- Reads already apply the pending alter on the fly: converted values, the renamed column's
-- data, and the re-added column's default.
SELECT x FROM stats_pending ORDER BY x;
SELECT rx FROM stats_pending ORDER BY rx;
SELECT dx FROM stats_pending ORDER BY dx;

-- The stale Float64 statistics [10.5, 11.5] and [100.5, 100.5] must not prune these parts.
SELECT count() FROM stats_pending WHERE x = 10;
SELECT count() FROM stats_pending WHERE x = 100;
SELECT count() FROM stats_pending WHERE x = 11;

-- Statistics of a column the alter does not touch stay usable for pruning. Under parallel
-- replicas the plan nests ReadFromMergeTree one level deeper, so strip that extra indent.
SELECT count() FROM stats_pending WHERE u = 55;
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM stats_pending WHERE u = 55) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM stats_pending WHERE u = 55) WHERE explain LIKE '%Parts:%';

-- rx reads ry's data, while the statistics under it belong to the dropped column.
SELECT count() FROM stats_pending WHERE rx = 100;
SELECT count() FROM stats_pending WHERE rx = 200;

-- dx reads the re-added column's default; the dropped column's statistics describe data the
-- query cannot see.
SELECT count() FROM stats_pending WHERE dx = 42;

SYSTEM START MERGES stats_pending;
ALTER TABLE stats_pending MATERIALIZE STATISTICS x SETTINGS mutations_sync = 2;

-- After the mutations materialize, the statistics describe the converted values
-- ([10, 11] and [100, 100]) and pruning still returns correct results.
SELECT count() FROM stats_pending WHERE x = 10;
SELECT count() FROM stats_pending WHERE x = 100;
SELECT count() FROM stats_pending WHERE x = 55;

DROP TABLE stats_pending;
