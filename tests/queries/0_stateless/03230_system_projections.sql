-- One part per insert, so aggregate projections have a stable row count.
SET max_insert_block_size = 1000000;

DROP TABLE IF EXISTS projections;
DROP TABLE IF EXISTS projections_2;
DROP TABLE IF EXISTS projections_unmaterialized;

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

INSERT INTO projections SELECT 'name_' || toString(number), number FROM numbers(1000);
INSERT INTO projections_2 SELECT 'name_' || toString(number % 2), number FROM numbers(1000);

SELECT
    database,
    table,
    name,
    type,
    total_rows,
    data_compressed_bytes > 0,
    data_uncompressed_bytes > 0,
    sorting_key,
    query,
    settings
FROM system.projections
WHERE database = currentDatabase()
ORDER BY table, name;

-- Sizes must match the per-part snapshot system.projection_parts already publishes.
SELECT
    p.table,
    p.name,
    p.total_rows = pp.rows,
    p.data_compressed_bytes = pp.compressed,
    p.data_uncompressed_bytes = pp.uncompressed
FROM
(
    SELECT table, name, total_rows, data_compressed_bytes, data_uncompressed_bytes
    FROM system.projections
    WHERE database = currentDatabase() AND table IN ('projections', 'projections_2')
) AS p
INNER JOIN
(
    SELECT
        table,
        name,
        sum(rows) AS rows,
        sum(data_compressed_bytes) AS compressed,
        sum(data_uncompressed_bytes) AS uncompressed
    FROM system.projection_parts
    WHERE database = currentDatabase() AND table IN ('projections', 'projections_2') AND active
    GROUP BY table, name
) AS pp ON p.table = pp.table AND p.name = pp.name
ORDER BY p.table, p.name;

SELECT count(*) FROM system.projections WHERE table = 'projections' AND database = currentDatabase();
SELECT count(*) FROM system.projections WHERE table = 'projections_2' AND database = currentDatabase();

SELECT name FROM system.projections WHERE type = 'Normal' AND database = currentDatabase();

-- Metadata lists the projection immediately; existing parts do not have it until MATERIALIZE.
-- The reverted code dereferenced a null projection part in this state.
CREATE TABLE projections_unmaterialized
(
    key UInt64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO projections_unmaterialized SELECT number FROM numbers(100);

ALTER TABLE projections_unmaterialized ADD PROJECTION p (SELECT * ORDER BY key);

SELECT
    name,
    total_rows,
    data_compressed_bytes,
    data_uncompressed_bytes
FROM system.projections
WHERE database = currentDatabase() AND table = 'projections_unmaterialized';

ALTER TABLE projections_unmaterialized MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT
    name,
    total_rows,
    data_compressed_bytes > 0,
    data_uncompressed_bytes > 0
FROM system.projections
WHERE database = currentDatabase() AND table = 'projections_unmaterialized';

DROP TABLE projections;
DROP TABLE projections_2;
DROP TABLE projections_unmaterialized;
