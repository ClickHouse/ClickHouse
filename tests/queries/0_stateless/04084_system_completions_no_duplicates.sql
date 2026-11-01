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

-- Sanity check: at least one ReplicatedMergeTree-specific setting must be present.
-- These settings (e.g. `replicated_can_become_leader`, `replicated_deduplication_window`)
-- live inside the `MergeTreeSettings` struct, so dropping the redundant
-- `getReplicatedMergeTreeSettings()` dump must NOT have removed them. This guards
-- against accidentally losing replicated-specific names if the dump logic changes.
SELECT count() > 0
FROM system.completions
WHERE context = 'merge tree setting'
  AND startsWith(word, 'replicated_');
