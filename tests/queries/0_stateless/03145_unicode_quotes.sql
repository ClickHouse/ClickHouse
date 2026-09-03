-- They work:
SELECT ‘This is an example of using English-style Unicode single quotes.’ AS “curly”;

-- It is unspecified which escaping rules apply inside the literal in Unicode quotes, and currently none apply (similarly to heredocs)
-- This could be changed.

SELECT ‘This is \an \\example ‘of using English-style Unicode single quotes.\’ AS “\c\\u\\\r\\\\l\\\\\y\\\\\\” FORMAT Vertical;

SELECT ‘’ = '' AS “1” FORMAT JSONLines;
