-- A table function argument that is not a column expression (here an empty subscript
-- producing an empty expression list, kept unresolved because of an unknown identifier)
-- used to reach `getColumnName` inside `evaluateConstantExpression` and raise a logical
-- error instead of a regular exception. Check that it now fails gracefully.
-- The analyzer reports `UNSUPPORTED_METHOD`, while the old analyzer (which never reached
-- the offending `getColumnName` call) reports `UNKNOWN_IDENTIFIER`; both are regular exceptions.

SELECT 1 FROM VALUES (CASE WHEN * THEN a0.x[][] END); -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT 1 FROM VALUES (CASE WHEN * THEN a0.x[][] WHEN a0 THEN 1 END); -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }

-- Regular usage of the `values` table function keeps working.
SELECT * FROM values('x UInt8', 1, 2, 3) ORDER BY x;
SELECT * FROM VALUES((1, 'a'), (2, 'b')) ORDER BY 1;
