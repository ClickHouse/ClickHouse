-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- ATTACH ... AS REPLICATED must validate engine-feature support (UNIQUE KEY is not
-- supported by ReplicatedMergeTree) BEFORE rewriting the on-disk metadata. Otherwise the
-- rejected conversion leaves the persisted metadata pointing at an unloadable
-- ReplicatedMergeTree + UNIQUE KEY combination, so the table fails to load after restart.
-- See issue #110854.

SET allow_experimental_unique_key = 1;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple() UNIQUE KEY (c0);

DETACH TABLE t0;

-- Rejected: the target engine ReplicatedMergeTree does not support UNIQUE KEY.
ATTACH TABLE t0 AS REPLICATED; -- { serverError BAD_ARGUMENTS }

-- The on-disk metadata must be untouched, so re-attaching with the stored definition works
-- and the table is queryable (this is the load path that failed after restart before the fix).
ATTACH TABLE t0;
INSERT INTO t0 VALUES (1), (2);
SELECT count() FROM t0;

DROP TABLE t0;

-- Same class of check for the `table_readonly` setting: ReplicatedMergeTree rejects it in its
-- constructor, so the conversion must be refused before the metadata is rewritten.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS table_readonly = 1;

DETACH TABLE t1;

-- Rejected: the target engine ReplicatedMergeTree does not support table_readonly = 1.
ATTACH TABLE t1 AS REPLICATED; -- { serverError BAD_ARGUMENTS }

-- The stored definition must still be MergeTree, so a plain re-attach loads and the table is
-- queryable (a table_readonly = 1 table cannot be inserted into, so we only check it loads).
ATTACH TABLE t1;
SELECT count() FROM t1;

DROP TABLE t1;
