-- Verify that ALTER TABLE DROP PART with a query parameter of a non-String type
-- throws a proper error instead of a logical error (bad cast).
-- The server replaces typed query parameters with _CAST(...) which is an ASTFunction, not ASTLiteral.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
SET param_partition = '2024-01-01';
ALTER TABLE t DROP PART {partition:Date}; -- { serverError BAD_ARGUMENTS }
DROP TABLE t;
