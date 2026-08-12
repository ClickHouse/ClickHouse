-- Tags: no-parallel
-- ^^ creates a SQL UDF (a global object); no-parallel avoids a name clash when the flaky check runs
-- many copies of this test concurrently.

-- { echo }

-- Constant `Bool` literals evaluated through the `Field`-returning `evaluateConstantExpression`
-- (used by the `values` table function) must keep the `Bool` type when converted to a textual type,
-- not collapse to `UInt64`. Regression: `values('x String', true)` returned '1' instead of 'true'.
SELECT * FROM values('x String', true, false);
SELECT * FROM values('x Nullable(String)', true, NULL, false);
-- numeric target is value-preserving (sanity)
SELECT * FROM values('x UInt8', true, false);

-- A non-literal that folds into a Bool literal must keep the tag too: the fix covers both the
-- analyzer path (constant-folded to a literal) and the non-analyzer path (`TreeRewriter` folds a SQL
-- UDF call into a literal), not only nodes that are literals to begin with.
DROP FUNCTION IF EXISTS udf_04611_bool;
CREATE FUNCTION udf_04611_bool AS () -> true;
SELECT * FROM values('x String', udf_04611_bool()) SETTINGS enable_analyzer = 1;
SELECT * FROM values('x String', udf_04611_bool()) SETTINGS enable_analyzer = 0;
DROP FUNCTION udf_04611_bool;
