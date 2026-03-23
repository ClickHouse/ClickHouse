-- Tags: replica, no-shared-merge-tree
-- Tag no-shared-merge-tree: detaching on one replica should not affect other replica.

-- Regression test for: https://github.com/ClickHouse/ClickHouse/issues/100413
--
-- CHECK TABLE on a replica that fetched a part whose checksums.txt references
-- an unknown projection must not throw NO_FILE_IN_DATA_PART. Before the fix,
-- checkDataPart would load checksums.txt (containing the pp.proj entry written
-- verbatim by the sender), find no corresponding directory in checksums_data
-- (since getProjectionParts() skips unknown projections so the directory was
-- never transferred), and fail, causing an infinite re-fetch loop.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_fetch_unknown_proj_1 SYNC;
DROP TABLE IF EXISTS t_fetch_unknown_proj_2 SYNC;

CREATE TABLE t_fetch_unknown_proj_1 (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch_unknown_proj', '1')
    PARTITION BY intDiv(y, 100) ORDER BY y
    SETTINGS max_parts_to_merge_at_once = 1;

CREATE TABLE t_fetch_unknown_proj_2 (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch_unknown_proj', '2')
    PARTITION BY intDiv(y, 100) ORDER BY y
    SETTINGS max_parts_to_merge_at_once = 1;

INSERT INTO t_fetch_unknown_proj_1 SELECT number, number FROM numbers(7);

ALTER TABLE t_fetch_unknown_proj_1 ADD PROJECTION pp (SELECT x, count() GROUP BY x);
ALTER TABLE t_fetch_unknown_proj_1 MATERIALIZE PROJECTION pp SETTINGS mutations_sync = 2;

-- Detach on replica 1 so the part (with pp.proj on disk) goes to detached/.
-- Replication propagates the detach to replica 2 as well.
ALTER TABLE t_fetch_unknown_proj_1 DETACH PARTITION 0;

-- Drop the detached copy from replica 2 so it is forced to re-fetch the part
-- from replica 1 when the partition is re-attached.
ALTER TABLE t_fetch_unknown_proj_2 DROP DETACHED PARTITION 0 SETTINGS allow_drop_detached = 1;

-- Drop projection pp from the table metadata while the partition is detached.
-- The part on replica 1's disk still has pp.proj and checksums.txt still
-- references it, but when replica 2 fetches the part the pp.proj directory
-- will NOT be transferred (getProjectionParts() returns nothing for pp).
ALTER TABLE t_fetch_unknown_proj_1 DROP PROJECTION pp;
ALTER TABLE t_fetch_unknown_proj_1 ATTACH PARTITION 0;

-- Wait for replica 2 to fetch the part. Data must be intact.
SYSTEM SYNC REPLICA t_fetch_unknown_proj_2;
SELECT count() FROM t_fetch_unknown_proj_2;   -- 7

-- CHECK TABLE must pass: checkDataPart must strip the orphaned pp.proj entry
-- from checksums_txt before the checkEqual comparison.
CHECK TABLE t_fetch_unknown_proj_2;   -- 1 (all parts ok)

DROP TABLE t_fetch_unknown_proj_1 SYNC;
DROP TABLE t_fetch_unknown_proj_2 SYNC;
