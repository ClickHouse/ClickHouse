| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | For Avro/AvroConfluent format: when field is not found in schema use default value instead of error | `0`     |
| `input_format_avro_null_as_default`         | For Avro/AvroConfluent format: insert default in case of null and non Nullable column	             |   `0`   |
| `format_avro_schema_registry_url`           | For AvroConfluent format: Confluent Schema Registry URL.                                            |         |
| `output_format_avro_codec`                  | Compression codec used for output. Possible values: 'null', 'deflate', 'snappy', 'zstd'.            |         |
| `output_format_avro_sync_interval`          | Sync interval in bytes.	                                                                         | `16384` |
| `output_format_avro_string_column_pattern`  | For Avro format: regexp of String columns to select as AVRO string.                                 |         |
| `output_format_avro_rows_in_file`           | Max rows in a file (if permitted by storage)	                                                     | `1`     |