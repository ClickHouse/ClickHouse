-- A materialized CTE read from inside a nested IN subquery over a Distributed table.
-- The set subquery's plan is executed as a standalone pipeline, so it has to carry its own
-- materialization gate: nothing above it can gate its readers.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t_gate;
DROP TABLE IF EXISTS dist_gate;
DROP TABLE IF EXISTS t_empty;
DROP TABLE IF EXISTS dist_empty;
DROP TABLE IF EXISTS mid_empty;
DROP TABLE IF EXISTS mid_pop;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS dist_idx;

CREATE TABLE t_gate (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_gate SELECT number + 1 FROM numbers(3);

CREATE TABLE dist_gate AS t_gate ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_gate);

-- index_granularity is pinned so the plan below keeps several granules to prune.
CREATE TABLE t_idx (c Int32) ENGINE = MergeTree ORDER BY c SETTINGS index_granularity = 4;
INSERT INTO t_idx SELECT number FROM numbers(64);

CREATE TABLE dist_idx AS t_idx ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_idx);

CREATE TABLE t_empty (c Int32) ENGINE = MergeTree ORDER BY c;
CREATE TABLE dist_empty AS t_empty ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_empty);

CREATE TABLE mid_empty (c Int32) ENGINE = MergeTree ORDER BY c;

CREATE TABLE mid_pop (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO mid_pop SELECT 1;

SELECT '-- nested IN over Distributed, empty middle table';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_gate WHERE c IN (SELECT c FROM mid_empty WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

SELECT '-- nested IN over Distributed, populated middle table';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_gate WHERE c IN (SELECT c FROM mid_pop WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

SELECT '-- same shape over a local table';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM t_gate WHERE c IN (SELECT c FROM mid_empty WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM t_gate WHERE c IN (SELECT c FROM mid_pop WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

SELECT '-- PREWHERE over an empty Distributed table, CTE declared after its reader';
WITH rs AS MATERIALIZED (SELECT * FROM dist_empty PREWHERE c IN (SELECT c FROM ct)),
     ct AS MATERIALIZED (SELECT 1 AS c)
SELECT count() FROM rs AS a, rs AS b;

SELECT '-- without the in-with-subqueries index path';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_gate WHERE c IN (SELECT c FROM mid_empty WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b
SETTINGS use_index_for_in_with_subqueries = 0;

SELECT '-- without a local replica preference';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_gate WHERE c IN (SELECT c FROM mid_empty WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b
SETTINGS prefer_localhost_replica = 0;

SELECT '-- GLOBAL IN, and a GLOBAL JOIN reading the CTE';
WITH ct AS MATERIALIZED (SELECT 1 AS c),
     rs AS MATERIALIZED (SELECT * FROM dist_gate WHERE c GLOBAL IN (SELECT c FROM mid_pop WHERE c IN (SELECT c FROM ct)))
SELECT count() FROM rs AS a, rs AS b;

-- The CTE is named twice so it stays materialized instead of being inlined, and the second
-- column is fed by the joined right-hand side, so it drops to 0 if the CTE contributes no rows.
WITH ct AS MATERIALIZED (SELECT 1 AS c)
SELECT count(), sum(j1.c) FROM dist_gate
GLOBAL ANY LEFT JOIN ct AS j1 USING (c)
GLOBAL ANY LEFT JOIN ct AS j2 USING (c);

-- ... and that arm reads a materialized CTE rather than an inlined subquery.
SELECT count() > 0 FROM (
    EXPLAIN
    WITH ct AS MATERIALIZED (SELECT 1 AS c)
    SELECT count(), sum(j1.c) FROM dist_gate
    GLOBAL ANY LEFT JOIN ct AS j1 USING (c)
    GLOBAL ANY LEFT JOIN ct AS j2 USING (c)
) WHERE explain ILIKE '%MaterializingCTEs%';

-- The primary key still prunes granules in the gated plan: one line names the index path that
-- ran, the other requires the selected granule count to be strictly below the total. Both need a
-- local MergeTree read in the plan, so the shard is read locally and index_granularity is pinned.
SELECT '-- the primary key still prunes granules under the gate';
SELECT
    countIf(explain LIKE '%Condition: (c in 1-element set)%') AS pk_condition_lines,
    countIf(match(explain, 'Granules: [0-9]+/[0-9]+')
            AND toUInt32(extract(explain, 'Granules: ([0-9]+)/'))
              < toUInt32(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) AS strictly_pruned_lines
FROM (
    EXPLAIN indexes = 1
    WITH ct AS MATERIALIZED (SELECT 1 AS c),
         rs AS MATERIALIZED (SELECT * FROM dist_idx WHERE c IN (SELECT c FROM mid_pop WHERE c IN (SELECT c FROM ct)))
    SELECT count() FROM rs AS a, rs AS b
    SETTINGS prefer_localhost_replica = 1
);

SELECT '-- and it does not prune when the in-with-subqueries index path is off';
SELECT
    countIf(explain LIKE '%Condition: (c in 1-element set)%') AS pk_condition_lines,
    countIf(match(explain, 'Granules: [0-9]+/[0-9]+')
            AND toUInt32(extract(explain, 'Granules: ([0-9]+)/'))
              < toUInt32(extract(explain, 'Granules: [0-9]+/([0-9]+)'))) AS strictly_pruned_lines
FROM (
    EXPLAIN indexes = 1
    WITH ct AS MATERIALIZED (SELECT 1 AS c),
         rs AS MATERIALIZED (SELECT * FROM dist_idx WHERE c IN (SELECT c FROM mid_pop WHERE c IN (SELECT c FROM ct)))
    SELECT count() FROM rs AS a, rs AS b
    SETTINGS prefer_localhost_replica = 1, use_index_for_in_with_subqueries = 0
);

DROP TABLE t_gate;
DROP TABLE dist_gate;
DROP TABLE t_empty;
DROP TABLE dist_empty;
DROP TABLE mid_empty;
DROP TABLE mid_pop;
DROP TABLE t_idx;
DROP TABLE dist_idx;
