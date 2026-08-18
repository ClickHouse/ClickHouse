DROP TABLE IF EXISTS data_01917;
DROP TABLE IF EXISTS data_01917_2;

CREATE TABLE data_01917
(
    key Int,
    d1 Int,
    d1_null Nullable(Int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

CREATE TABLE data_01917_2
(
    name String,
    frequency UInt64,
    INDEX memory (frequency * length(name)) TYPE set(1000) GRANULARITY 5,
    INDEX sample_index1 (length(name), name) TYPE minmax GRANULARITY 4,
    INDEX sample_index2 (lower(name), name) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
)
Engine=MergeTree()
ORDER BY name;

SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

SELECT count(*) FROM system.data_skipping_indices WHERE table = 'data_01917' AND database = currentDatabase();
SELECT count(*) FROM system.data_skipping_indices WHERE table = 'data_01917_2' AND database = currentDatabase();

SELECT name FROM system.data_skipping_indices WHERE type = 'minmax' AND database = currentDatabase();

DROP TABLE data_01917;
DROP TABLE data_01917_2;

