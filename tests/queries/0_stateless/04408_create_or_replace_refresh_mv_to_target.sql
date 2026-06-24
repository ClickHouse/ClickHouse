-- Tags: no-ordinary-database, no-replicated-database
-- Refreshable MVs with non-replicated target tables are refused on a Replicated database
-- (the refresh runs on one replica but the table is replaced on others), so we restrict
-- this case to Atomic databases.

SET allow_experimental_refreshable_materialized_view = 1;

DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS tgt SYNC;
DROP TABLE IF EXISTS rmv SYNC;
DROP TABLE IF EXISTS rmv_other SYNC;
DROP TABLE IF EXISTS rmv_plain SYNC;

CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE tgt (x UInt32) ENGINE = MergeTree ORDER BY x;

-- First create of a non-APPEND refreshable MV with an explicit TO target.
CREATE OR REPLACE MATERIALIZED VIEW rmv REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'rmv';

-- Replacing it used to fail: the target was still owned by the view being replaced.
CREATE OR REPLACE MATERIALIZED VIEW rmv REFRESH EVERY 2 HOUR TO tgt AS SELECT x * 2 AS x FROM src;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'rmv';

-- A different view must not take over a target that another refreshable MV owns,
-- both via a plain CREATE and via CREATE OR REPLACE.
CREATE MATERIALIZED VIEW rmv_plain REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE MATERIALIZED VIEW rmv_other REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

-- A plain CREATE whose name happens to look like a temporary replacement name still owns its
-- target exclusively, so it must not bypass the check either.
CREATE MATERIALIZED VIEW `_tmp_replace_a_b` REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS rmv SYNC;
DROP TABLE IF EXISTS tgt SYNC;
DROP TABLE IF EXISTS src SYNC;
