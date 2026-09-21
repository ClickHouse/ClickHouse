-- Tags: no-parallel, no-fasttest

insert into function file(03276_parquet_custom_encoder_compression_level_1.parquet) SETTINGS output_format_parquet_compression_method = 'zstd', output_format_compression_level=1, output_format_parquet_use_custom_encoder=1, engine_file_truncate_on_insert=1 SELECT number AS id, toString(number) AS name, now() + number AS timestamp FROM numbers(100000);
insert into function file(03276_parquet_custom_encoder_compression_level_22.parquet) SETTINGS output_format_parquet_compression_method = 'zstd', output_format_compression_level=22, output_format_parquet_use_custom_encoder=1, engine_file_truncate_on_insert=1 SELECT number AS id, toString(number) AS name, now() + number AS timestamp FROM numbers(100000);

WITH
    (SELECT total_compressed_size FROM file(03276_parquet_custom_encoder_compression_level_1.parquet, ParquetMetadata)) AS size_level_1,
    (SELECT total_compressed_size FROM file(03276_parquet_custom_encoder_compression_level_22.parquet, ParquetMetadata)) AS size_level_22
SELECT
    size_level_22 < size_level_1 AS compression_higher_level_produces_smaller_file;

insert into function file(03276_parquet_arrow_encoder_compression_level_1.parquet) SETTINGS output_format_parquet_compression_method = 'zstd', output_format_compression_level=1, output_format_parquet_use_custom_encoder=0, engine_file_truncate_on_insert=1 SELECT number AS id, toString(number) AS name, now() + number AS timestamp FROM numbers(100000);
insert into function file(03276_parquet_arrow_encoder_compression_level_22.parquet) SETTINGS output_format_parquet_compression_method = 'zstd', output_format_compression_level=22, output_format_parquet_use_custom_encoder=0, engine_file_truncate_on_insert=1 SELECT number AS id, toString(number) AS name, now() + number AS timestamp FROM numbers(100000);

WITH
    (SELECT total_compressed_size FROM file(03276_parquet_arrow_encoder_compression_level_1.parquet, ParquetMetadata)) AS size_level_1,
    (SELECT total_compressed_size FROM file(03276_parquet_arrow_encoder_compression_level_22.parquet, ParquetMetadata)) AS size_level_22
SELECT
    size_level_22 < size_level_1 AS compression_higher_level_produces_smaller_file;
