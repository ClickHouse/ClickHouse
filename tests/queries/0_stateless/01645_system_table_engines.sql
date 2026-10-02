SELECT * FROM system.table_engines WHERE name in ('MergeTree', 'ReplicatedCollapsingMergeTree') ORDER BY name FORMAT PrettyCompactNoEscapes;
