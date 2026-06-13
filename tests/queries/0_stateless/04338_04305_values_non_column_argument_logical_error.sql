-- A table function argument that is not a column expression (here an empty subscript
-- producing an empty expression list, kept unresolved because of an unknown identifier)
-- used to reach `getColumnName` inside `evaluateConstantExpression` and raise a logical
-- error instead of a regular exception. Check that it now fails gracefully.

SELECT 1 FROM VALUES (CASE WHEN * THEN a0.x[][] END); -- { serverError UNSUPPORTED_METHOD }
SELECT 1 FROM VALUES (CASE WHEN * THEN a0.x[][] WHEN a0 THEN 1 END); -- { serverError UNSUPPORTED_METHOD }

-- Regular usage of the `values` table function keeps working.
SELECT * FROM values('x UInt8', 1, 2, 3) ORDER BY x;
SELECT * FROM VALUES((1, 'a'), (2, 'b')) ORDER BY 1;
