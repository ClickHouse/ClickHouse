-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_analyzer = 1; -- `Parquet` `VARIANT` subcolumn queries use the analyzer path.
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04138_parquet_variant_prewhere_row_number.parquet', Parquet)
SELECT CAST(raw AS JSON) AS data
FROM values(
    'n UInt8, raw String',
    (0, '{"kind":"commit","did":"did:plc:a"}'),
    (1, '{"kind":"identity","did":"did:plc:b"}'),
    (2, '{"kind":"commit","did":"did:plc:c"}'),
    (3, '{"kind":"identity","did":"did:plc:d"}'),
    (4, '{"kind":"commit","did":"did:plc:e"}'))
ORDER BY n;

SELECT _row_number, data.did
FROM file(
    currentDatabase() || '04138_parquet_variant_prewhere_row_number.parquet',
    Parquet,
    'data JSON(kind LowCardinality(String), did String)')
PREWHERE data.kind = 'commit'
ORDER BY _row_number
FORMAT TSVRaw;
