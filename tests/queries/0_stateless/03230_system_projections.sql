DROP TABLE IF EXISTS projections;
DROP TABLE IF EXISTS projections_2;

CREATE TABLE projections
(
    key String,
    d1 Int,
    PROJECTION improved_sorting_key (
        SELECT *
        ORDER BY d1, key
    )
)
Engine=MergeTree()
ORDER BY key;

CREATE TABLE projections_2
(
    name String,
    frequency UInt64,
    PROJECTION agg (
        SELECT name, max(frequency) max_frequency
        GROUP BY name
    ),
    PROJECTION agg_no_key (
        SELECT max(frequency) max_frequency
    )
)
Engine=MergeTree()
ORDER BY name;

INSERT INTO projections SELECT 'name_' || number AS key, number AS d1 FROM numbers(1000);
INSERT INTO projections_2 SELECT 'name_' || number % 2 AS name, number AS frequency FROM numbers(1000);

SELECT
    database,
    table,
    name,
    type,
    total_rows,
    data_compressed_bytes > 0,
    data_uncompressed_bytes > 0,
    sorting_key,
    query
FROM system.projections WHERE database = currentDatabase();

SELECT count(*) FROM system.projections WHERE table = 'projections' AND database = currentDatabase();
SELECT count(*) FROM system.projections WHERE table = 'projections_2' AND database = currentDatabase();

SELECT name FROM system.projections WHERE type = 'Normal' AND database = currentDatabase();

DROP TABLE projections;
DROP TABLE projections_2;
