-- A NULL value must never match an array element, so indexOf/countEqual return 0 for it.
SELECT a, indexOf([0, 1], a), countEqual([0, 1, 0, 1, 1], a)
FROM (SELECT arrayJoin([0, 1, NULL]::Array(Nullable(UInt64))) AS a)
ORDER BY a NULLS LAST;
