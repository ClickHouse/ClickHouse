-- A materialized view can depend on a writable regular view. An INSERT into the view must
-- reach both the underlying target table (through the inner INSERT pipeline) and the
-- materialized view's target table (through the chunk that `SinkToStorage::onGenerate`
-- forwards to dependent views). Before the fix the sink detached the chunk's columns and
-- forwarded an empty chunk downstream, so the materialized view silently missed every
-- inserted row.

DROP TABLE IF EXISTS t_dmv;
DROP VIEW IF EXISTS v_dmv;
DROP TABLE IF EXISTS dst_dmv;
DROP VIEW IF EXISTS mv_dmv;

CREATE TABLE t_dmv (a Int32, b String, c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_dmv AS SELECT a, b, c FROM t_dmv;
CREATE TABLE dst_dmv (a Int32, b String, c Float64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_dmv TO dst_dmv AS SELECT a, b, c FROM v_dmv;

-- Full insert: the row must appear in both the target table and the materialized view target.
INSERT INTO v_dmv VALUES (1, 'x', 1.5);

-- Partial insert: the omitted column `c` is filled with the target table's DEFAULT (0.5).
-- That materialized value must reach the materialized view as well, not the type default 0.
INSERT INTO v_dmv (a, b) VALUES (2, 'y');

SELECT 'target:', a, b, c FROM t_dmv ORDER BY a;
SELECT 'mv:', a, b, c FROM dst_dmv ORDER BY a;

DROP VIEW mv_dmv;
DROP TABLE dst_dmv;
DROP VIEW v_dmv;
DROP TABLE t_dmv;
