DROP TABLE IF EXISTS row_nested_gate;

-- The Row gate applies to nested types even when nested-type validation is disabled.
SET allow_experimental_row_type = 0;
SET validate_experimental_and_suspicious_types_inside_nested_types = 0;

CREATE TABLE row_nested_gate (a Array(Row(x UInt64))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE row_nested_gate (t Tuple(Row(x UInt64))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE row_nested_gate (m Map(String, Row(x UInt64))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
SELECT * FROM format(TSV, 'a Array(Row(x UInt64))', '[(1)]'); -- { serverError ILLEGAL_COLUMN }

SET validate_experimental_and_suspicious_types_inside_nested_types = 1;
CREATE TABLE row_nested_gate (a Array(Row(x UInt64))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_row_type = 1;
SET validate_experimental_and_suspicious_types_inside_nested_types = 0;
CREATE TABLE row_nested_gate (a Array(Row(x UInt64))) ENGINE = Memory;
INSERT INTO row_nested_gate VALUES ([(1), (2)]);
SELECT a, toTypeName(a) FROM row_nested_gate;

DROP TABLE row_nested_gate;
