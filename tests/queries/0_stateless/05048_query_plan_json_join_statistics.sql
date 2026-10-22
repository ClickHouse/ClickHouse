-- Tags: no-old-analyzer

-- A join reports metrics of its own -- how many rows each side saw, how large the hash table grew --
-- only when the query runs with the join analyze mode on. Until `log_query_plans` turned that mode
-- on, the plan stored for a query with a join carried its I/O and timings but none of that, so the
-- whole `Left` / `Right` / `HashTable` vocabulary was unreachable through `system.query_log`.
--
-- The mode has to be set before the interpreter is built, because every join reads it while the
-- planner constructs it; a join built without it does not even allocate the counters. That makes
-- this easy to break silently, and the assertions below are what would catch it.

DROP TABLE IF EXISTS j_left;
DROP TABLE IF EXISTS j_right;

CREATE TABLE j_left (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE j_right (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO j_left SELECT number FROM numbers(20000);
INSERT INTO j_right SELECT number * 2 FROM numbers(5000);

SET log_query_plans = 1;
SELECT count() FROM j_left JOIN j_right USING (k)
SETTINGS log_comment = '05048_hash', join_algorithm = 'hash' FORMAT Null;
SET log_query_plans = 0;

-- The same query without plan logging must not pay for the counters.
SELECT count() FROM j_left JOIN j_right USING (k)
SETTINGS log_comment = '05048_off', join_algorithm = 'hash' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    'join_stats',
    count(),
    -- The join's own groups, which are what this is about: none of them appeared before the
    -- analyze mode was turned on for a logged query.
    anyLast(JSONHas(join_stats, 'Left')),
    anyLast(JSONHas(join_stats, 'Right')),
    anyLast(JSONHas(join_stats, 'HashTable')),
    -- Counts are asserted as invariants rather than as values. Which table ends up on the build
    -- side is the optimiser's choice, and the test harness randomises settings that influence it,
    -- so pinning `Right.Rows` to the size of one table passes or fails depending on the draw.
    anyLast(JSONExtractUInt(join_stats, 'Right', 'Rows')) > 0,
    anyLast(JSONExtractUInt(join_stats, 'HashTable', 'UniqueKeys')) > 0,
    anyLast(JSONExtractUInt(join_stats, 'HashTable', 'Memory')) > 0,
    -- How many rows the probe side sees is not pinned: a runtime filter built from the right side
    -- prunes the left scan first, so the count depends on an optimisation, not on the join. What
    -- must hold is that the join cannot match more rows than it probed.
    anyLast(JSONExtractUInt(join_stats, 'Left', 'Matched'))
        <= anyLast(JSONExtractUInt(join_stats, 'Left', 'Rows')),
    -- A hash join splits into two named stages, which are also only labelled in analyze mode.
    anyLast(position(JSONExtractRaw(join_stats, 'Stages'), '"build"')) > 0,
    anyLast(position(JSONExtractRaw(join_stats, 'Stages'), '"probe"')) > 0
FROM
(
    SELECT JSONExtractRaw(
        arrayFilter(n -> JSONExtractString(n, 'Node Type') = 'Join',
                    JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes'))[1],
        'Statistics') AS join_stats
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05048_hash'
);

-- Nothing is captured at all with the setting off, so no counters were collected either.
SELECT 'off', count(), anyLast(toJSONString(query_plan))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05048_off';

DROP TABLE j_left;
DROP TABLE j_right;
