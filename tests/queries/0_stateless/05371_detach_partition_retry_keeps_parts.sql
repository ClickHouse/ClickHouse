-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-fasttest, no-async-insert
-- no-ordinary-database, no-fasttest: needs transactions.
-- no-shared-merge-tree: the non-transactional DETACH of plain MergeTree.

-- A DETACH whose empty covering part has the name of an Outdated part retries with PART_IS_TEMPORARILY_LOCKED
-- after covering the parts before it. detached/ must hold exactly the parts the DETACH removed.

DROP TABLE IF EXISTS t;

CREATE TABLE t (key UInt64, value String) ENGINE = MergeTree PARTITION BY key % 2 ORDER BY key
SETTINGS merge_tree_clear_old_parts_interval_seconds = 30;

INSERT INTO t VALUES (2, 'a');
INSERT INTO t VALUES (1, 'b');

-- The rolled-back merge leaves the Outdated 1_2_2_1, the name of the empty part that covers 1_2_2_0.
BEGIN TRANSACTION;
OPTIMIZE TABLE t PARTITION 1 FINAL;
ROLLBACK;

SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 't' ORDER BY name;

ALTER TABLE t DETACH PARTITION ALL;

SELECT count() FROM t;
SELECT name, reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't' ORDER BY name;

ALTER TABLE t ATTACH PARTITION ALL;
SELECT * FROM t ORDER BY key;

DROP TABLE t;
