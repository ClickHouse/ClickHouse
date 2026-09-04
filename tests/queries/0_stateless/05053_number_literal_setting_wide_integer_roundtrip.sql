-- A setting value must survive a format/parse round trip with its type intact. A literal too large
-- for UInt64 resolves to a wide integer, and `FieldVisitorToString` quotes those, so the formatted
-- query read the value back as a String. In a debug build the AST consistency check compares tree
-- hashes and aborts the query on that.

SELECT
    JSONExtractString(parseQueryToJSON(q), 'changes', 1, 'value', 'field_type') AS before,
    JSONExtractString(parseQueryToJSON(formatQuery(q)), 'changes', 1, 'value', 'field_type') AS after
FROM values('q String',
    'SET totals_auto_threshold = 18446744073709551616',
    'SET totals_auto_threshold = -18446744073709551616',
    'SET totals_auto_threshold = 340282366920938463463374607431768211456',
    'SET totals_auto_threshold = -170141183460469231731687303715884105729',
    'SET totals_auto_threshold = 1.5',
    'SET max_threads = 8')
ORDER BY q;

-- Same for the SETTINGS clause, which is the same AST node.
WITH 'SELECT 1 SETTINGS totals_auto_threshold = 18446744073709551616' AS q
SELECT
    JSONExtractString(parseQueryToJSON(q), 'list_of_selects', 'children', 1, 'settings', 'changes', 1, 'value', 'field_type') AS before,
    JSONExtractString(parseQueryToJSON(formatQuery(q)), 'list_of_selects', 'children', 1, 'settings', 'changes', 1, 'value', 'field_type') AS after;
