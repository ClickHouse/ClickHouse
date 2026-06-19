-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04110_parquet_variant_fastpath.parquet', Parquet)
SELECT CAST('{"kind":"commit","did":"a","commit":{"operation":"create","collection":"x"}}' AS JSON) AS json
UNION ALL
SELECT CAST('{"kind":"commit","commit":{"operation":"create","collection":"x"}}' AS JSON)
UNION ALL
SELECT CAST('{"kind":"other","commit":{"operation":"delete","collection":"y"}}' AS JSON)
UNION ALL
SELECT CAST('{}' AS JSON);

SELECT
    json.kind,
    json.did,
    json.`commit.operation`,
    json.`commit.collection`
FROM file(
    currentDatabase() || '04110_parquet_variant_fastpath.parquet',
    Parquet,
    'json JSON(max_dynamic_paths=0, kind LowCardinality(String), did String, `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String))')
ORDER BY
    json.kind = '',
    json.kind,
    json.did = '',
    json.did,
    json.`commit.operation`,
    json.`commit.collection`
FORMAT TSVRaw;

SELECT count()
FROM file(
    currentDatabase() || '04110_parquet_variant_fastpath.parquet',
    Parquet,
    'json JSON(max_dynamic_paths=0, kind LowCardinality(String), did String, `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String))')
WHERE json.kind = 'commit'
  AND json.`commit.operation` = 'create'
  AND json.`commit.collection` = 'x';

SET query_plan_optimize_prewhere = 1;

SELECT count()
FROM file(
    currentDatabase() || '04110_parquet_variant_fastpath.parquet',
    Parquet,
    toString('json JSON(max_dynamic_paths=0, kind LowCardinality(String), did String, `commit.operation` LowCardinality(String), `commit.collection` LowCardinality(String))'))
WHERE toFixedString('x', 1) = json.`commit.collection`
  AND json.`commit.operation` = toFixedString('create', toNullable(1025))
  AND toNullable('3') = json.kind;
