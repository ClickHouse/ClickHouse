-- Aggregating an empty input with an -Array combinator aggregate and a constant GROUP BY key
-- reported a pointer overflow under UBSan, so this only bites in a sanitizer build. The setting is
-- pinned because the runner randomizes it off, and the query then stops reaching the affected path.
-- A zero-row block no longer gets a group, so the result is empty and the cast is now unreachable.
SELECT sumArray([n]) FROM (SELECT 1 AS n, emptyArrayUInt8() AS r) ARRAY JOIN r GROUP BY n
SETTINGS optimize_group_by_constant_keys = 1;
