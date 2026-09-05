-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_analyzer = 1; -- `Parquet` `VARIANT` subcolumn queries use the analyzer path.
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04111_parquet_variant_prewhere_subcolumns.parquet', Parquet)
SELECT CAST('{"kind":"commit","did":"did:plc:a","time_us":1,"commit":{"collection":"app.bsky.feed.post","operation":"create"}}' AS JSON) AS data
UNION ALL
SELECT CAST('{"kind":"commit","did":"did:plc:b","time_us":2,"commit":{"collection":"app.bsky.feed.like","operation":"delete"}}' AS JSON)
UNION ALL
SELECT CAST('{"kind":"identity","did":"did:plc:c","time_us":3}' AS JSON);

SELECT data.did
FROM file(
    currentDatabase() || '04111_parquet_variant_prewhere_subcolumns.parquet',
    Parquet,
    'data JSON(kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
PREWHERE data.kind = 'commit'
ORDER BY data.did
FORMAT TSVRaw;

SELECT data.`commit.collection`
FROM file(
    currentDatabase() || '04111_parquet_variant_prewhere_subcolumns.parquet',
    Parquet,
    'data JSON(kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
PREWHERE data.kind = 'commit'
WHERE data.`commit.operation` = 'create'
ORDER BY data.did
FORMAT TSVRaw;

SELECT trimLeft(explain)
FROM
(
    EXPLAIN PLAN actions=1, header=1
    SELECT data.`commit.collection`
    FROM file(
        currentDatabase() || '04111_parquet_variant_prewhere_subcolumns.parquet',
        Parquet,
        'data JSON(kind LowCardinality(String), `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String), did String, time_us UInt64)')
    PREWHERE data.kind = 'commit'
)
WHERE explain LIKE '%Prewhere info%'
   OR explain LIKE '%Prewhere filter column:%'
FORMAT TSVRaw;
