-- `enable_sharing_sets_for_mutations` promises that the part tasks of one mutation share an
-- `IN (subquery)` set instead of each building its own, and that a part task which reused the set
-- still gets primary key analysis from it. The sibling `02581_share_big_sets_*` tests assert only
-- that at least one part built the set and at least one part reused it, which stays true even when
-- the set is rebuilt for every part.
--
-- The cache is per mutation and kept alive only by the part tasks holding it
-- (`StorageMergeTree::getPreparedSetsCache` keeps a `weak_ptr`), so it is shared by part tasks that
-- overlap in time. The subquery below deliberately holds its builder long enough for sibling tasks
-- to exercise that cache-hit path, but nothing asserted here requires them to actually overlap; see
-- the `cache_hit_tasks_still_prune` comment below.

DROP TABLE IF EXISTS t_share_sets;

CREATE TABLE t_share_sets (id UInt32, description String, PRIMARY KEY id)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

-- Four parts, each spanning several granules so that index analysis is observable.
INSERT INTO t_share_sets SELECT number,           '' FROM numbers(100000);
INSERT INTO t_share_sets SELECT number+10000000,  '' FROM numbers(100000);
INSERT INTO t_share_sets SELECT number+20000000,  '' FROM numbers(100000);
INSERT INTO t_share_sets SELECT number+30000000,  '' FROM numbers(100000);

-- Every value of the set lies inside the first part, so a part task holding the set can prune
-- every granule of the other three.
-- `enable_sharing_sets_for_mutations` is not pinned here: a mutation task builds its context from
-- the server's background context (`MutatePlainMergeTreeTask::createTaskContext`), so a setting
-- given to the `ALTER` never reaches it. The test relies on the default, which is 1.
ALTER TABLE t_share_sets UPDATE description = 'x'
-- Keep each block below the three second `sleepEachRow` limit while the builder is parked long
-- enough for sibling part tasks to reach the shared set.
WHERE id IN (SELECT (number * 5)::UInt32 FROM numbers(200) WHERE sleepEachRow(0.01) = 0)
SETTINGS mutations_sync = 2;

SET max_rows_to_read = 0; -- system.text_log can be really big
SYSTEM FLUSH LOGS text_log;

-- All assertions require a live per-mutation `PreparedSetsCache`, which is what
-- `enable_sharing_sets_for_mutations` installs.
--
-- The query id of a mutation part task is '<table uuid>::all_1_1_0_<mutation version>', which is
-- what keys these rows to this table and makes the test safe to run in parallel with itself. The
-- booleans are computed per part task first, so that the cache-hit pruning assertion is correlated:
-- an independent pruning count would pass if the tasks that reused the set lost their index
-- analysis.
WITH (
        SELECT uuid
        FROM system.tables
        WHERE (database = currentDatabase()) AND (name = 't_share_sets')
    ) AS table_uuid
SELECT
    -- No part task materializes the same set key twice. 'Creating set, key: %' is logged by a part
    -- task that really materializes the set, on the cached and the uncached path alike, so counting
    -- it per key and per part task states exactly that. Before sharing the speculative build, the
    -- part task that ran the speculative pass materialized the mutation's read key a second time for
    -- the deferred build, which this catches while staying independent of how many part tasks
    -- overlap in time. How many distinct keys the mutation materializes is not asserted:
    -- `enable_analyzer = 0` and `enable_analyzer = 1` disagree on whether the
    -- `isStorageTouchedByMutations` count probe and the mutation's own read get the same cache key.
    -- The `>= 1` keeps the assertion from passing vacuously when nothing was materialized at all.
    (
        SELECT (count() >= 1) AND (max(materializations_by_one_part_task) = 1)
        FROM
        (
            SELECT
                extract(message, 'key: (__set_[0-9_]+)') AS set_key,
                count() AS materializations_by_one_part_task
            FROM system.text_log
            WHERE
                query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_%')
                AND message LIKE 'Creating set, key: %'
                AND (event_date >= yesterday() AND event_time >= now() - 600)
            GROUP BY set_key, query_id
        )
    ) AS no_part_task_materializes_a_key_twice,
    -- A part task that took the set from the cache must still use it for primary-key pruning;
    -- parts 2 to 4 contain no matching id, so such a task has to prune every granule.
    --
    -- The non-vacuity guard is `countIf(reused_the_set) >= 1`, over all part tasks rather than over
    -- the siblings only. Whether a sibling reaches the cache while the slowed builder still holds it
    -- depends on the background pool running the part tasks of one mutation concurrently, which is
    -- not guaranteed: under load they can be fully serialized, each one starting only after the
    -- previous one renamed its part, and then every part task builds the set for itself. That is how
    -- this test failed once in fifty runs of the flaky check:
    -- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=112941&sha=1e125acf0a03cc6f7dc4004acc1f934d914a253e&name_0=PR&name_1=Stateless%20tests%20%28amd_tsan%2C%20flaky%20check%29
    -- The part task holding the matching rows always reuses the set, because its deferred runtime
    -- build takes the set its own speculative pass put into the cache, so the guard stays honest
    -- without depending on how the part tasks interleave.
    countIf(reused_the_set) >= 1
        AND countIf(reused_the_set AND NOT holds_the_matching_rows AND pruned_everything)
            = countIf(reused_the_set AND NOT holds_the_matching_rows)
        AS cache_hit_tasks_still_prune
FROM
(
    SELECT
        query_id,
        maxIf(1, message_format_string LIKE 'Got set from cache%') = 1          AS reused_the_set,
        maxIf(1, message LIKE 'Selected %0 marks to read from 0 ranges') = 1    AS pruned_everything,
        query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_1\\_1\\_%')   AS holds_the_matching_rows
    FROM system.text_log
    WHERE
        query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_%')
        AND (event_date >= yesterday() AND event_time >= now() - 600)
    GROUP BY query_id
)
FORMAT TSVWithNames;

DROP TABLE t_share_sets;
