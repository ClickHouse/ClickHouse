-- `enable_sharing_sets_for_mutations` promises that the part tasks of one mutation share an
-- `IN (subquery)` set instead of each building its own, and that a part task which reused the set
-- still gets primary key analysis from it. The sibling `02581_share_big_sets_*` tests assert only
-- that at least one part built the set and at least one part reused it, which stays true even when
-- the set is rebuilt for every part.
--
-- The cache is per mutation and kept alive only by the part tasks holding it
-- (`StorageMergeTree::getPreparedSetsCache` keeps a `weak_ptr`), so a key is materialized once per
-- group of part tasks that overlap in time. How many such groups a mutation is split into is a
-- scheduling property of the background pool, not something the feature promises, so asserting one
-- materialization per key across the whole mutation reddens on a loaded runner for a legitimate
-- reason. The assertions below hold under any such split.

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
WHERE id IN (SELECT (number * 5)::UInt32 FROM numbers(2000))
SETTINGS mutations_sync = 2;

SET max_rows_to_read = 0; -- system.text_log can be really big
SYSTEM FLUSH LOGS text_log;

-- All three assertions require a live per-mutation `PreparedSetsCache`, which is what
-- `enable_sharing_sets_for_mutations` installs.
--
-- The query id of a mutation part task is '<table uuid>::all_1_1_0_<mutation version>', which is
-- what keys these rows to this table and makes the test safe to run in parallel with itself. The
-- booleans are computed per part task first, so that the two per-task assertions below can be
-- correlated: asserting them independently would pass even when the tasks that reused the set are
-- exactly the ones that lost their index analysis.
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
    -- overlap in time. How many distinct keys the mutation materializes is not asserted: the old and
    -- the new analyzer disagree on whether the `isStorageTouchedByMutations` count probe and the
    -- mutation's own read get the same cache key. The `>= 1` keeps the assertion from passing
    -- vacuously when nothing was materialized at all.
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
    -- Fewer part tasks materialize the set than there are part tasks, i.e. it is shared rather
    -- than rebuilt per part. Without sharing every one of the part tasks builds its own.
    countIf(built_the_set) < count()
        AS set_built_fewer_times_than_parts,
    -- Every part task that reused the set and cannot match it must still use it to filter marks:
    -- parts 2 to 4 contain no matching id, so such a part task reads no marks at all. Part 1 holds
    -- every value of the set and legitimately reads marks, so it is excluded. Stated as an
    -- implication over those tasks rather than as a count of them: how many part tasks reuse the set
    -- depends on how many of them overlap in time, while losing index analysis after a reuse is a
    -- defect for any single one. The `>= 1` keeps the implication from passing vacuously.
    countIf(reused_the_set AND NOT holds_the_matching_rows) >= 1
        AND countIf(reused_the_set AND NOT holds_the_matching_rows AND pruned_everything)
            = countIf(reused_the_set AND NOT holds_the_matching_rows)
        AS part_tasks_reusing_the_set_still_prune
FROM
(
    SELECT
        query_id,
        maxIf(1, message_format_string LIKE 'Created Set with % entries%') = 1  AS built_the_set,
        maxIf(1, message_format_string LIKE 'Got set from cache%') = 1          AS reused_the_set,
        -- Matched on `message`, not on `message_format_string`: the format string is the same for
        -- every such line and carries no counts.
        maxIf(1, message LIKE 'Selected %0 marks to read from 0 ranges') = 1    AS pruned_everything,
        -- Part 1 is the only part whose range covers the set's values, so it is the only one that
        -- may legitimately read marks. Its part name is the prefix of the part task's query id.
        query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_1\\_1\\_%')   AS holds_the_matching_rows
    FROM system.text_log
    WHERE
        query_id LIKE concat(CAST(table_uuid, 'String'), '::all\\_%')
        AND (event_date >= yesterday() AND event_time >= now() - 600)
    GROUP BY query_id
)
FORMAT TSVWithNames;

DROP TABLE t_share_sets;
