-- Regression tests for the review findings of the Trino dialect.

SET allow_experimental_trino_dialect = 1;

DROP TABLE IF EXISTS trino_review_regressions;
CREATE TABLE trino_review_regressions (s Nullable(String), arr Array(Nullable(Int64))) ENGINE = Memory;

SET dialect = 'trino';

-- An INSERT ... SELECT whose select list contains the `format` function and an ARRAY literal
-- must be translated as a query: the tokens `format`/`VALUES` after a top-level SELECT are not
-- an inline-data tail.
INSERT INTO trino_review_regressions SELECT format('%s-%s', 'a', 'b'), ARRAY[1, 2];
SELECT s, arr FROM trino_review_regressions;

-- The VARBINARY overload of length counts bytes, the VARCHAR overload counts code points.
SELECT length(to_utf8('𐐭')), length('𐐭');
SELECT length(CAST('𐐭' AS VARBINARY));
SELECT length(substr(to_utf8('𐐭'), 1, 2));
SELECT substr(to_utf8('ab'), 2, 1), substr('𐐭x', 1, 1);
SELECT length(lpad(to_utf8('a'), 3, to_utf8('𐐭'))), length(lpad('a', 3, '𐐭'));
SELECT length(rpad(to_utf8('a'), 3, to_utf8('𐐭'))), length(rpad('a', 3, '𐐭'));

-- json_extract with a bracket-quoted path returns the bare JSON value
-- (previously the JSON_QUERY fallback wrapped it into an array).
SELECT json_extract('{"hello": 2}', '$["hello"]');
SELECT json_extract('{"a": {"b c": [7, 8]}}', '$.a["b c"][1]');
SELECT json_extract('{"a": 1}', '$.a[*]'); -- { clientError NOT_IMPLEMENTED }
-- ^ unsupported paths are rejected rather than silently changing semantics
-- (the comment is below the statement because a parser-thrown error hint must be on the first unparsed line).

-- A qualified reference to a joined UNNEST alias is unqualified only within the query scope
-- that introduced it: the inner `t.x` must keep its qualifier (removing it would be ambiguous
-- with `u.x`).
SELECT t.x, (SELECT t.x FROM (VALUES 30) AS t (x) CROSS JOIN (VALUES 40) AS u (x))
FROM (VALUES 0) AS src (v)
CROSS JOIN UNNEST(ARRAY[1, 2]) AS t (x)
ORDER BY t.x;

DROP TABLE trino_review_regressions;
