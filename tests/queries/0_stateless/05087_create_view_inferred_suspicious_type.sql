-- A view stores no data, so the suspicious/experimental type gates must not reject its column types.

DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS v_written;
DROP VIEW IF EXISTS v_fs;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t (x LowCardinality(Int16)) ENGINE = Memory;
INSERT INTO t VALUES (7);
SET allow_suspicious_low_cardinality_types = 0;

-- The type inferred from the SELECT is accepted, and it is the type the view really has.
CREATE VIEW v AS SELECT x FROM t;
SELECT x FROM v;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'v' AND name = 'x';

-- The same statement with the column list spelled out: this is what SHOW CREATE prints and what is
-- dispatched to the other hosts of a cluster, so it has to be accepted too.
CREATE VIEW v_written (x LowCardinality(Int16)) AS SELECT x FROM t;

-- A second, independently gated setting: the exemption is the whole check, not one type.
CREATE VIEW v_fs AS SELECT toFixedString(materialize('a'), 300) AS x;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'v_fs' AND name = 'x';

-- A table stores its columns, so it stays refused, inferred type or not.
CREATE TABLE t_as ENGINE = Memory AS SELECT x FROM t; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

-- The inner table of a materialized view is a table, so it stays refused as well.
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT x FROM t; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

DROP VIEW v_fs;
DROP VIEW v_written;
DROP VIEW v;
DROP TABLE t;
