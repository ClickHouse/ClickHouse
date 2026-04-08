-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102013
-- system.completions should not contain duplicate rows for MergeTree settings.
-- Previously, both getMergeTreeSettings() and getReplicatedMergeTreeSettings()
-- were dumped, producing identical rows since they share the same setting names.

SELECT word, count() AS cnt
FROM system.completions
WHERE context = 'merge tree setting'
GROUP BY word
HAVING cnt > 1
ORDER BY word;
