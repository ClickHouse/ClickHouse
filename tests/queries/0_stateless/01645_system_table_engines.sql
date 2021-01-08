SELECT * FROM system.table_engines WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree') FORMAT PrettyCompactNoEscapes;
