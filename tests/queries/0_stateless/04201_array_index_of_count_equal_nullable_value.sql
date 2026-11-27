-- The array holds no NULLs, so a NULL needle matches nothing: indexOf/countEqual return 0.
SELECT a, indexOf([0, 1], a), countEqual([0, 1, 0, 1, 1], a)
FROM (SELECT arrayJoin([0, 1, NULL]::Array(Nullable(UInt64))) AS a)
ORDER BY a NULLS LAST;
