-- The APPLY transformer must emit its function name and its column-name prefix so that the formatted
-- query parses back to the same AST.

-- a single quote in the prefix must survive the round trip
SELECT formatQuerySingleLine('SELECT * APPLY (toString, ''x''''y'') FROM t');
-- a backslash in the prefix must survive the round trip
SELECT formatQuerySingleLine('SELECT * APPLY (toString, ''a\\\\b'') FROM t');
-- a function name that needs back-quoting must survive the round trip
SELECT formatQuerySingleLine('SELECT * APPLY `to String` FROM t');
-- control: an ordinary prefix and function name are formatted exactly as before
SELECT formatQuerySingleLine('SELECT * APPLY (toString, ''p_'') FROM t');

-- the same shapes executed: an assertions-enabled build checks the AST round trip per query
SELECT * APPLY (toString, 'x''y') FROM (SELECT 1 AS a);
SELECT * APPLY `to String` FROM (SELECT 1 AS a); -- { serverError UNKNOWN_FUNCTION }
