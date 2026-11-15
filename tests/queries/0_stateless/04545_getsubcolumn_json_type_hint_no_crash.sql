-- Tags: no-fasttest
-- no-fasttest: requires the JSON data type.

SET enable_json_type = 1;

-- A ":`type hint`" continuation marker is not a subcolumn name a user can ask for directly.
SELECT getSubcolumn('{}'::JSON, ':`Int64`'); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn(materialize('{}'::JSON), ':`Int64`'); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn('{}'::JSON, ':`Array(JSON)`'); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn([('{}'::JSON)], ':`Int64`'); -- { serverError ILLEGAL_COLUMN }

-- A valid dynamic subcolumn of JSON still resolves normally.
SELECT getSubcolumn('{"a":5}'::JSON, 'a');

-- The marker is still resolvable as a continuation after a typed path prefix match.
DROP TABLE IF EXISTS t_04545;
CREATE TABLE t_04545 (json JSON(a Array(JSON), c Int64)) ENGINE = Memory;
INSERT INTO t_04545 VALUES ('{"a":[{"x":1}], "c":42}');
SELECT json.a.:`Array(JSON)`.x, json.c.:`Int64` FROM t_04545;
DROP TABLE t_04545;
