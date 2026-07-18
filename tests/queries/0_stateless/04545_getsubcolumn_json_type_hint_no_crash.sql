-- Tags: no-fasttest
-- no-fasttest: requires the JSON data type.

SET enable_json_type = 1;

-- getSubcolumn with an internal ":`type hint`" continuation marker as the subcolumn name used
-- to crash during query analysis (null deref in IDataType::getSubcolumnType) because
-- DataTypeObject::getDynamicSubcolumnData returned null for such names even in throw_if_null mode.
-- It must now raise ILLEGAL_COLUMN instead.
SELECT getSubcolumn('{}'::JSON, ':`Int64`'); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn(materialize('{}'::JSON), ':`Int64`'); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn('{}'::JSON, ':`Array(JSON)`'); -- { serverError ILLEGAL_COLUMN }

-- The same marker reached recursively through a nested JSON (Array element type) must also throw.
SELECT getSubcolumn([('{}'::JSON)], ':`Int64`'); -- { serverError ILLEGAL_COLUMN }

-- A valid dynamic subcolumn of JSON still resolves normally.
SELECT getSubcolumn('{"a":5}'::JSON, 'a');
