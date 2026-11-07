-- `ASTNameTypePair` can carry a `DEFAULT` expression for a `Tuple` element
-- (`Tuple(a UInt8 DEFAULT 1)`), which is normalized to a column-level default only after parsing.
-- Its JSON representation must therefore serialize `default_expression`, otherwise the round-trip
-- through `parseQueryToJSON` / `formatQueryFromJSON` would silently drop the default.

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (c Tuple(a UInt8 DEFAULT 1, s String DEFAULT \'Hello\')) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN c Tuple(a UInt8, s String DEFAULT \'Hello\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN c Tuple(a UInt8, n Tuple(b UInt8 DEFAULT 5))'));

-- A non-literal default round-trips as well.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x String, c Tuple(a UInt8, s String DEFAULT concat(x, \'!\'))) ENGINE = Memory'));
