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
DROP TABLE IF EXISTS dep SYNC;
DROP TABLE IF EXISTS dep_tgt SYNC;
DROP TABLE IF EXISTS mv_dep SYNC;
DROP TABLE IF EXISTS tgt_dep SYNC;
DROP TABLE IF EXISTS plain_mv SYNC;
DROP TABLE IF EXISTS append_mv SYNC;

CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (1), (2), (3);
CREATE TABLE tgt (x UInt32) ENGINE = MergeTree ORDER BY x;

-- First create of a non-APPEND refreshable MV with an explicit TO target. The replacement view is
-- built empty, so the target is only populated by the first refresh after the rename.
CREATE OR REPLACE MATERIALIZED VIEW rmv REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src;
SYSTEM WAIT VIEW rmv;
SELECT 'first', sum(x) FROM tgt;

-- Replacing it used to fail: the target was still owned by the view being replaced. The new
-- definition must take effect in the target.
CREATE OR REPLACE MATERIALIZED VIEW rmv REFRESH EVERY 2 HOUR TO tgt AS SELECT x * 2 AS x FROM src;
SYSTEM WAIT VIEW rmv;
SELECT 'replace', sum(x) FROM tgt;

-- A dependent replacement refreshes through normal scheduling after the rename, like a plain
-- CREATE: the dependency already refreshed, so the dependent refreshes too (not delayed).
CREATE TABLE dep_tgt (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW dep REFRESH EVERY 1 YEAR TO dep_tgt AS SELECT x FROM src;
SYSTEM WAIT VIEW dep;
CREATE TABLE tgt_dep (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE OR REPLACE MATERIALIZED VIEW mv_dep REFRESH EVERY 1 YEAR DEPENDS ON dep TO tgt_dep AS SELECT x FROM src;
SYSTEM WAIT VIEW mv_dep;
SELECT 'deps', count() FROM tgt_dep;

-- A different view must not take over a target that another refreshable MV owns,
-- both via a plain CREATE and via CREATE OR REPLACE.
CREATE MATERIALIZED VIEW rmv_plain REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE MATERIALIZED VIEW rmv_other REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

-- A plain CREATE whose name happens to look like a temporary replacement name still owns its
-- target exclusively, so it must not bypass the check either.
CREATE MATERIALIZED VIEW `_tmp_replace_a_b` REFRESH EVERY 1 HOUR TO tgt AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

-- A plain (non-refreshable) MV and an APPEND refreshable MV do not exclusively own their target, so
-- replacing them succeeds even while a non-APPEND refreshable view owns it, just like a plain CREATE.
CREATE OR REPLACE MATERIALIZED VIEW plain_mv TO tgt AS SELECT x FROM src;
SELECT 'plain', name FROM system.tables WHERE database = currentDatabase() AND name = 'plain_mv';
CREATE OR REPLACE MATERIALIZED VIEW append_mv REFRESH EVERY 1 HOUR APPEND TO tgt AS SELECT x FROM src;
SELECT 'append', name FROM system.tables WHERE database = currentDatabase() AND name = 'append_mv';

DROP TABLE IF EXISTS append_mv SYNC;
DROP TABLE IF EXISTS plain_mv SYNC;
DROP TABLE IF EXISTS mv_dep SYNC;
DROP TABLE IF EXISTS tgt_dep SYNC;
DROP TABLE IF EXISTS dep SYNC;
DROP TABLE IF EXISTS dep_tgt SYNC;
DROP TABLE IF EXISTS rmv SYNC;
DROP TABLE IF EXISTS tgt SYNC;
DROP TABLE IF EXISTS src SYNC;
