-- Tags: no-fasttest
-- Test: DETACH TABLE ... PERMANENTLY honours `check_referential_table_dependencies=1`
-- and leaves the source table fully usable after the failed dependency check.
DROP TABLE IF EXISTS mv_ref;
DROP TABLE IF EXISTS dst_ref;
DROP TABLE IF EXISTS src_ref;

CREATE TABLE src_ref (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_ref (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_ref TO dst_ref AS SELECT x FROM src_ref;
INSERT INTO src_ref VALUES (1), (2), (3);

SET check_referential_table_dependencies = 1;

-- Referential dependency must block the permanent detach.
DETACH TABLE src_ref PERMANENTLY; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- After the failed detach the source table must stay in the catalog and readable.
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'src_ref';
SELECT count() FROM src_ref;

DROP TABLE mv_ref;
DROP TABLE src_ref;
DROP TABLE dst_ref;
