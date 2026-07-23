-- { echo }

-- Constant `Bool` literals evaluated through the `Field`-returning `evaluateConstantExpression`
-- (used by the `values` table function) must keep the `Bool` type when converted to a textual type,
-- not collapse to `UInt64`. Regression: `values('x String', true)` returned '1' instead of 'true'.
-- See .cursor/projects/valueref-pilot/BRIDGES.md (B1).
SELECT * FROM values('x String', true, false);
SELECT * FROM values('x Nullable(String)', true, NULL, false);
-- numeric target is value-preserving (sanity)
SELECT * FROM values('x UInt8', true, false);
