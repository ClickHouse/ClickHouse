-- A view stores no data, so the suspicious/experimental type gates must not reject its column types.

DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS v_written;
DROP VIEW IF EXISTS v_dict;
DROP VIEW IF EXISTS v_fs;
DROP DICTIONARY IF EXISTS dict;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t (x LowCardinality(Int16), id LowCardinality(String)) ENGINE = Memory;
INSERT INTO t VALUES (7, 'a');
SET allow_suspicious_low_cardinality_types = 0;

-- The type inferred from the SELECT is accepted, and it is the type the view really has.
CREATE VIEW v AS SELECT x FROM t;
SELECT x FROM v;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'v' AND name = 'x';

-- The same statement with the column list spelled out: this is what SHOW CREATE prints and what is
-- dispatched to the other hosts of a cluster, so it has to be accepted too.
CREATE VIEW v_written (x LowCardinality(Int16)) AS SELECT x FROM t;

-- The case reported in #57561: dictGet keeps the LowCardinality of its key argument, so the type
-- inferred for the view's column is LowCardinality(DateTime).
CREATE DICTIONARY dict (id String, timestamp DateTime)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT \'a\' AS id, now() AS timestamp'))
LAYOUT(DIRECT());
CREATE VIEW v_dict AS SELECT dictGet(dict, 'timestamp', id) AS ts FROM t;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'v_dict' AND name = 'ts';

-- A second, independently gated setting: the exemption is the whole check, not one type.
CREATE VIEW v_fs AS SELECT toFixedString(materialize('a'), 300) AS x;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'v_fs' AND name = 'x';

-- A table stores its columns, so it stays refused, inferred type or not.
CREATE TABLE t_as ENGINE = Memory AS SELECT x FROM t; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

-- The inner table of a materialized view is a table, so it stays refused as well.
CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT x FROM t; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

DROP VIEW v_fs;
DROP VIEW v_dict;
DROP DICTIONARY dict;
DROP VIEW v_written;
DROP VIEW v;
DROP TABLE t;
