-- Tags: no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: the test is about the covering part that plain `MergeTree` writes on `REPLACE PARTITION`.
-- no-replicated-database: the test relies on the block numbers of a single, freshly created table.

-- `REPLACE PARTITION` writes an empty part that covers the replaced ones, so that a restart before
-- they are unlinked does not resurrect them. Parts that were outdated earlier are still on disk as
-- well, and the covering part has to contain those too - otherwise the part loader finds a pair of
-- parts that neither contain one another nor are disjoint, and the table fails to attach with
-- "Part ... intersects previous part ...".

DROP TABLE IF EXISTS t_replace_cover_dst;
DROP TABLE IF EXISTS t_replace_cover_src;

-- Keep the outdated parts on disk for the whole test, as they would be right after a `REPLACE PARTITION` in production.
CREATE TABLE t_replace_cover_dst (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000, remove_empty_parts = 1;

CREATE TABLE t_replace_cover_src (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

INSERT INTO t_replace_cover_dst SETTINGS async_insert = 0 VALUES (1, 1);
INSERT INTO t_replace_cover_dst SETTINGS async_insert = 0 VALUES (1, 2);

-- `DROP PART` covers the part with an empty part of level 1, which is then dropped for being empty.
-- That leaves `1_2_2_1` outdated on disk with no active part covering it.
ALTER TABLE t_replace_cover_dst DROP PART '1_2_2_0';

INSERT INTO t_replace_cover_src SETTINGS async_insert = 0 VALUES (1, 3);

ALTER TABLE t_replace_cover_dst REPLACE PARTITION 1 FROM t_replace_cover_src;

DETACH TABLE t_replace_cover_dst;
ATTACH TABLE t_replace_cover_dst;

SELECT * FROM t_replace_cover_dst ORDER BY k;

DROP TABLE t_replace_cover_dst;
DROP TABLE t_replace_cover_src;
