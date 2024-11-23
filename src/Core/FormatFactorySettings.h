#pragma once

/// This header exists so we can share it between multiple setting objects that include format settings

#include <Core/SettingsObsoleteMacros.h>

// clang-format off
#if defined(__CLION_IDE__)
/// CLion freezes for a minute every time it processes this
#define FORMAT_FACTORY_SETTINGS(M, ALIAS)
#define OBSOLETE_FORMAT_SETTINGS(M, ALIAS)
#else

#define FORMAT_FACTORY_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Char, format_csv_delimiter, ',', R"(
The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.
)", 0) \
    DECLARE(Bool, format_csv_allow_single_quotes, false, R"(
If it is set to true, allow strings in single quotes.
)", 0) \
    DECLARE(Bool, format_csv_allow_double_quotes, true, R"(
If it is set to true, allow strings in double quotes.
)", 0) \
    DECLARE(Bool, output_format_csv_serialize_tuple_into_separate_columns, true, R"(
If it set to true, then Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost)
)", 0) \
    DECLARE(Bool, input_format_csv_deserialize_separate_columns_into_tuple, true, R"(
If it set to true, then separate columns written in CSV format can be deserialized to Tuple column.
)", 0) \
    DECLARE(Bool, output_format_csv_crlf_end_of_line, false, R"(
If it is set true, end of line in CSV format will be \\r\\n instead of \\n.
)", 0) \
    DECLARE(Bool, input_format_csv_allow_cr_end_of_line, false, R"(
If it is set true, \\r will be allowed at end of line not followed by \\n
)", 0) \
    DECLARE(Bool, input_format_csv_enum_as_number, false, R"(
Treat inserted enum values in CSV formats as enum indices
)", 0) \
    DECLARE(Bool, input_format_csv_arrays_as_nested_csv, false, R"(
When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: \"[\"\"Hello\"\", \"\"world\"\", \"\"42\"\"\"\" TV\"\"]\". Braces around array can be omitted.
)", 0) \
    DECLARE(Bool, input_format_skip_unknown_fields, true, R"(
Enables or disables skipping insertion of extra data.

When writing data, ClickHouse throws an exception if input data contain columns that do not exist in the target table. If skipping is enabled, ClickHouse does not insert extra data and does not throw an exception.

Supported formats:

- [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) (and other JSON formats)
- [BSONEachRow](../../interfaces/formats.md/#bsoneachrow) (and other JSON formats)
- [TSKV](../../interfaces/formats.md/#tskv)
- All formats with suffixes WithNames/WithNamesAndTypes
- [MySQLDump](../../interfaces/formats.md/#mysqldump)
- [Native](../../interfaces/formats.md/#native)

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, input_format_with_names_use_header, true, R"(
Enables or disables checking the column order when inserting data.

To improve insert performance, we recommend disabling this check if you are sure that the column order of the input data is the same as in the target table.

Supported formats:

- [CSVWithNames](../../interfaces/formats.md/#csvwithnames)
- [CSVWithNamesAndTypes](../../interfaces/formats.md/#csvwithnamesandtypes)
- [TabSeparatedWithNames](../../interfaces/formats.md/#tabseparatedwithnames)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md/#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNames](../../interfaces/formats.md/#jsoncompacteachrowwithnames)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNames](../../interfaces/formats.md/#jsoncompactstringseachrowwithnames)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNames](../../interfaces/formats.md/#rowbinarywithnames)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md/#rowbinarywithnamesandtypes)
- [CustomSeparatedWithNames](../../interfaces/formats.md/#customseparatedwithnames)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md/#customseparatedwithnamesandtypes)

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, input_format_with_types_use_header, true, R"(
Controls whether format parser should check if data types from the input data match data types from the target table.

Supported formats:

- [CSVWithNamesAndTypes](../../interfaces/formats.md/#csvwithnamesandtypes)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md/#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md/#rowbinarywithnamesandtypes-rowbinarywithnamesandtypes)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md/#customseparatedwithnamesandtypes)

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, input_format_import_nested_json, false, R"(
Enables or disables the insertion of JSON data with nested objects.

Supported formats:

- [JSONEachRow](../../interfaces/formats.md/#jsoneachrow)

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [Usage of Nested Structures](../../interfaces/formats.md/#jsoneachrow-nested) with the `JSONEachRow` format.
)", 0) \
    DECLARE(Bool, input_format_defaults_for_omitted_fields, true, R"(
When performing `INSERT` queries, replace omitted input column values with default values of the respective columns. This option applies to [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) (and other JSON formats), [CSV](../../interfaces/formats.md/#csv), [TabSeparated](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [Parquet](../../interfaces/formats.md/#parquet), [Arrow](../../interfaces/formats.md/#arrow), [Avro](../../interfaces/formats.md/#avro), [ORC](../../interfaces/formats.md/#orc), [Native](../../interfaces/formats.md/#native) formats and formats with `WithNames`/`WithNamesAndTypes` suffixes.

:::note
When this option is enabled, extended table metadata are sent from server to client. It consumes additional computing resources on the server and can reduce performance.
:::

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", IMPORTANT) \
    DECLARE(Bool, input_format_csv_empty_as_default, true, R"(
Treat empty fields in CSV input as default values.
)", 0) \
    DECLARE(Bool, input_format_tsv_empty_as_default, false, R"(
Treat empty fields in TSV input as default values.
)", 0) \
    DECLARE(Bool, input_format_tsv_enum_as_number, false, R"(
Treat inserted enum values in TSV formats as enum indices.
)", 0) \
    DECLARE(Bool, input_format_null_as_default, true, R"(
Enables or disables the initialization of [NULL](../../sql-reference/syntax.md/#null-literal) fields with [default values](../../sql-reference/statements/create/table.md/#create-default-values), if data type of these fields is not [nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable).
If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting.

This setting is applicable for most input formats.

For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Possible values:

- 0 — Inserting `NULL` into a not nullable column causes an exception.
- 1 — `NULL` fields are initialized with default column values.
)", 0) \
    DECLARE(Bool, input_format_force_null_for_omitted_fields, false, R"(
Force initialize omitted fields with null values
)", 0) \
    DECLARE(Bool, input_format_arrow_case_insensitive_column_matching, false, R"(
Ignore case when matching Arrow columns with CH columns.
)", 0) \
    DECLARE(Int64, input_format_orc_row_batch_size, 100'000, R"(
Batch size when reading ORC stripes.
)", 0) \
    DECLARE(Bool, input_format_orc_case_insensitive_column_matching, false, R"(
Ignore case when matching ORC columns with CH columns.
)", 0) \
    DECLARE(Bool, input_format_parquet_case_insensitive_column_matching, false, R"(
Ignore case when matching Parquet columns with CH columns.
)", 0) \
    DECLARE(Bool, input_format_parquet_preserve_order, false, R"(
Avoid reordering rows when reading from Parquet files. Usually makes it much slower.
)", 0) \
    DECLARE(Bool, input_format_parquet_filter_push_down, true, R"(
When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.
)", 0) \
    DECLARE(Bool, input_format_parquet_bloom_filter_push_down, false, R"(
When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.
)", 0) \
    DECLARE(Bool, input_format_parquet_use_native_reader, false, R"(
When reading Parquet files, to use native reader instead of arrow reader.
)", 0) \
    DECLARE(Bool, input_format_allow_seeks, true, R"(
Allow seeks while reading in ORC/Parquet/Arrow input formats.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_orc_allow_missing_columns, true, R"(
Allow missing columns while reading ORC input formats
)", 0) \
    DECLARE(Bool, input_format_orc_use_fast_decoder, true, R"(
Use a faster ORC decoder implementation.
)", 0) \
    DECLARE(Bool, input_format_orc_filter_push_down, true, R"(
When reading ORC files, skip whole stripes or row groups based on the WHERE/PREWHERE expressions, min/max statistics or bloom filter in the ORC metadata.
)", 0) \
    DECLARE(String, input_format_orc_reader_time_zone_name, "GMT", R"(
The time zone name for ORC row reader, the default ORC row reader's time zone is GMT.
)", 0) \
    DECLARE(Bool, input_format_orc_dictionary_as_low_cardinality, true, R"(
Treat ORC dictionary encoded columns as LowCardinality columns while reading ORC files.
)", 0) \
    DECLARE(Bool, input_format_parquet_allow_missing_columns, true, R"(
Allow missing columns while reading Parquet input formats
)", 0) \
    DECLARE(UInt64, input_format_parquet_local_file_min_bytes_for_seek, 8192, R"(
Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format
)", 0) \
    DECLARE(Bool, input_format_parquet_enable_row_group_prefetch, true, R"(
Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.
)", 0) \
    DECLARE(Bool, input_format_arrow_allow_missing_columns, true, R"(
Allow missing columns while reading Arrow input formats
)", 0) \
    DECLARE(Char, input_format_hive_text_fields_delimiter, '\x01', R"(
Delimiter between fields in Hive Text File
)", 0) \
    DECLARE(Char, input_format_hive_text_collection_items_delimiter, '\x02', R"(
Delimiter between collection(array or map) items in Hive Text File
)", 0) \
    DECLARE(Char, input_format_hive_text_map_keys_delimiter, '\x03', R"(
Delimiter between a pair of map key/values in Hive Text File
)", 0) \
    DECLARE(Bool, input_format_hive_text_allow_variable_number_of_columns, true, R"(
Ignore extra columns in Hive Text input (if file has more columns than expected) and treat missing fields in Hive Text input as default values
)", 0) \
    DECLARE(UInt64, input_format_msgpack_number_of_columns, 0, R"(
The number of columns in inserted MsgPack data. Used for automatic schema inference from data.
)", 0) \
    DECLARE(MsgPackUUIDRepresentation, output_format_msgpack_uuid_representation, FormatSettings::MsgPackUUIDRepresentation::EXT, R"(
The way how to output UUID in MsgPack format.
)", 0) \
    DECLARE(UInt64, input_format_max_rows_to_read_for_schema_inference, 25000, R"(
The maximum rows of data to read for automatic schema inference.
)", 0) \
    DECLARE(UInt64, input_format_max_bytes_to_read_for_schema_inference, 32 * 1024 * 1024, R"(
The maximum amount of data in bytes to read for automatic schema inference.
)", 0) \
    DECLARE(Bool, input_format_csv_use_best_effort_in_schema_inference, true, R"(
Use some tweaks and heuristics to infer schema in CSV format
)", 0) \
    DECLARE(Bool, input_format_csv_try_infer_numbers_from_strings, false, R"(
If enabled, during schema inference ClickHouse will try to infer numbers from string fields.
It can be useful if CSV data contains quoted UInt64 numbers.

Disabled by default.
)", 0) \
    DECLARE(Bool, input_format_csv_try_infer_strings_from_quoted_tuples, true, R"(
Interpret quoted tuples in the input data as a value of type String.
)", 0) \
    DECLARE(Bool, input_format_tsv_use_best_effort_in_schema_inference, true, R"(
Use some tweaks and heuristics to infer schema in TSV format
)", 0) \
    DECLARE(Bool, input_format_csv_detect_header, true, R"(
Automatically detect header with names and types in CSV format
)", 0) \
    DECLARE(Bool, input_format_csv_allow_whitespace_or_tab_as_delimiter, false, R"(
Allow to use spaces and tabs(\\t) as field delimiter in the CSV strings
)", 0) \
    DECLARE(Bool, input_format_csv_trim_whitespaces, true, R"(
Trims spaces and tabs (\\t) characters at the beginning and end in CSV strings
)", 0) \
    DECLARE(Bool, input_format_csv_use_default_on_bad_values, false, R"(
Allow to set default value to column when CSV field deserialization failed on bad value
)", 0) \
    DECLARE(Bool, input_format_csv_allow_variable_number_of_columns, false, R"(
Ignore extra columns in CSV input (if file has more columns than expected) and treat missing fields in CSV input as default values
)", 0) \
    DECLARE(Bool, input_format_tsv_allow_variable_number_of_columns, false, R"(
Ignore extra columns in TSV input (if file has more columns than expected) and treat missing fields in TSV input as default values
)", 0) \
    DECLARE(Bool, input_format_custom_allow_variable_number_of_columns, false, R"(
Ignore extra columns in CustomSeparated input (if file has more columns than expected) and treat missing fields in CustomSeparated input as default values
)", 0) \
    DECLARE(Bool, input_format_json_compact_allow_variable_number_of_columns, false, R"(
Ignore extra columns in JSONCompact(EachRow) input (if file has more columns than expected) and treat missing fields in JSONCompact(EachRow) input as default values
)", 0) \
    DECLARE(Bool, input_format_tsv_detect_header, true, R"(
Automatically detect header with names and types in TSV format
)", 0) \
    DECLARE(Bool, input_format_custom_detect_header, true, R"(
Automatically detect header with names and types in CustomSeparated format
)", 0) \
    DECLARE(Bool, input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference, false, R"(
Skip columns with unsupported types while schema inference for format Parquet
)", 0) \
    DECLARE(UInt64, input_format_parquet_max_block_size, DEFAULT_BLOCK_SIZE, R"(
Max block size for parquet reader.
)", 0) \
    DECLARE(UInt64, input_format_parquet_prefer_block_bytes, DEFAULT_BLOCK_SIZE * 256, R"(
Average block bytes output by parquet reader
)", 0) \
    DECLARE(Bool, input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference, false, R"(
Skip fields with unsupported types while schema inference for format Protobuf
)", 0) \
    DECLARE(Bool, input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference, false, R"(
Skip columns with unsupported types while schema inference for format CapnProto
)", 0) \
    DECLARE(Bool, input_format_orc_skip_columns_with_unsupported_types_in_schema_inference, false, R"(
Skip columns with unsupported types while schema inference for format ORC
)", 0) \
    DECLARE(Bool, input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference, false, R"(
Skip columns with unsupported types while schema inference for format Arrow
)", 0) \
    DECLARE(String, column_names_for_schema_inference, "", R"(
The list of column names to use in schema inference for formats without column names. The format: 'column1,column2,column3,...'
)", 0) \
    DECLARE(String, schema_inference_hints, "", R"(
The list of column names and types to use as hints in schema inference for formats without schema.

Example:

Query:
```sql
desc format(JSONEachRow, '{"x" : 1, "y" : "String", "z" : "0.0.0.0" }') settings schema_inference_hints='x UInt8, z IPv4';
```

Result:
```sql
x   UInt8
y   Nullable(String)
z   IPv4
```

:::note
If the `schema_inference_hints` is not formatted properly, or if there is a typo or a wrong datatype, etc... the whole schema_inference_hints will be ignored.
:::
)", 0) \
    DECLARE(SchemaInferenceMode, schema_inference_mode, "default", R"(
Mode of schema inference. 'default' - assume that all files have the same schema and schema can be inferred from any file, 'union' - files can have different schemas and the resulting schema should be the a union of schemas of all files
)", 0) \
    DECLARE(UInt64Auto, schema_inference_make_columns_nullable, 1, R"(
Controls making inferred types `Nullable` in schema inference.
If the setting is enabled, all inferred type will be `Nullable`, if disabled, the inferred type will never be `Nullable`, if set to `auto`, the inferred type will be `Nullable` only if the column contains `NULL` in a sample that is parsed during schema inference or file metadata contains information about column nullability.
)", 0) \
    DECLARE(Bool, input_format_json_read_bools_as_numbers, true, R"(
Allow parsing bools as numbers in JSON input formats.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_read_bools_as_strings, true, R"(
Allow parsing bools as strings in JSON input formats.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_try_infer_numbers_from_strings, false, R"(
If enabled, during schema inference ClickHouse will try to infer numbers from string fields.
It can be useful if JSON data contains quoted UInt64 numbers.

Disabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_validate_types_from_metadata, true, R"(
For JSON/JSONCompact/JSONColumnsWithMetadata input formats, if this setting is set to 1,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_read_numbers_as_strings, true, R"(
Allow parsing numbers as strings in JSON input formats.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_read_objects_as_strings, true, R"(
Allow parsing JSON objects as strings in JSON input formats.

Example:

```sql
SET input_format_json_read_objects_as_strings = 1;
CREATE TABLE test (id UInt64, obj String, date Date) ENGINE=Memory();
INSERT INTO test FORMAT JSONEachRow {"id" : 1, "obj" : {"a" : 1, "b" : "Hello"}, "date" : "2020-01-01"};
SELECT * FROM test;
```

Result:

```
┌─id─┬─obj──────────────────────┬───────date─┐
│  1 │ {"a" : 1, "b" : "Hello"} │ 2020-01-01 │
└────┴──────────────────────────┴────────────┘
```

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_read_arrays_as_strings, true, R"(
Allow parsing JSON arrays as strings in JSON input formats.

Example:

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```

Result:
```
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_try_infer_named_tuples_from_objects, true, R"(
If enabled, during schema inference ClickHouse will try to infer named Tuple from JSON objects.
The resulting named Tuple will contain all elements from all corresponding JSON objects from sample data.

Example:

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

Result:

```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects, false, R"(
Use String type instead of an exception in case of ambiguous paths in JSON objects during named tuples inference
)", 0) \
    DECLARE(Bool, input_format_json_infer_incomplete_types_as_strings, true, R"(
Allow to use String type for JSON keys that contain only `Null`/`{}`/`[]` in data sample during schema inference.
In JSON formats any value can be read as String, and we can avoid errors like `Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` during schema inference
by using String type for keys with unknown types.

Example:

```sql
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

Result:
```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_named_tuples_as_objects, true, R"(
Parse named tuple columns as JSON objects.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_ignore_unknown_keys_in_named_tuple, true, R"(
Ignore unknown keys in json object for named tuples.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_defaults_for_missing_elements_in_named_tuple, true, R"(
Insert default values for missing elements in JSON object while parsing named tuple.
This setting works only when setting `input_format_json_named_tuples_as_objects` is enabled.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_throw_on_bad_escape_sequence, true, R"(
Throw an exception if JSON string contains bad escape sequence in JSON input formats. If disabled, bad escape sequences will remain as is in the data.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_json_ignore_unnecessary_fields, true, R"(
Ignore unnecessary fields and not parse them. Enabling this may not throw exceptions on json strings of invalid format or with duplicated fields
)", 0) \
    DECLARE(Bool, input_format_try_infer_variants, false, R"(
If enabled, ClickHouse will try to infer type [`Variant`](../../sql-reference/data-types/variant.md) in schema inference for text formats when there is more than one possible type for column/array elements.

Possible values:

- 0 — Disabled.
- 1 — Enabled.
)", 0) \
    DECLARE(Bool, type_json_skip_duplicated_paths, false, R"(
When enabled, during parsing JSON object into JSON type duplicated paths will be ignored and only the first one will be inserted instead of an exception
)", 0) \
    DECLARE(UInt64, input_format_json_max_depth, 1000, R"(
Maximum depth of a field in JSON. This is not a strict limit, it does not have to be applied precisely.
)", 0) \
    DECLARE(Bool, input_format_json_empty_as_default, false, R"(
Treat empty fields in JSON input as default values.
)", 0) \
    DECLARE(Bool, input_format_try_infer_integers, true, R"(
If enabled, ClickHouse will try to infer integers instead of floats in schema inference for text formats. If all numbers in the column from input data are integers, the result type will be `Int64`, if at least one number is float, the result type will be `Float64`.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_try_infer_dates, true, R"(
If enabled, ClickHouse will try to infer type `Date` from string fields in schema inference for text formats. If all fields from a column in input data were successfully parsed as dates, the result type will be `Date`, if at least one field was not parsed as date, the result type will be `String`.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_try_infer_datetimes, true, R"(
If enabled, ClickHouse will try to infer type `DateTime64` from string fields in schema inference for text formats. If all fields from a column in input data were successfully parsed as datetimes, the result type will be `DateTime64`, if at least one field was not parsed as datetime, the result type will be `String`.

Enabled by default.
)", 0) \
    DECLARE(Bool, input_format_try_infer_datetimes_only_datetime64, false, R"(
When input_format_try_infer_datetimes is enabled, infer only DateTime64 but not DateTime types
)", 0) \
    DECLARE(Bool, input_format_try_infer_exponent_floats, false, R"(
Try to infer floats in exponential notation while schema inference in text formats (except JSON, where exponent numbers are always inferred)
)", 0) \
    DECLARE(Bool, output_format_markdown_escape_special_characters, false, R"(
Escape special characters in Markdown
)", 0) \
    DECLARE(Bool, input_format_protobuf_flatten_google_wrappers, false, R"(
Enable Google wrappers for regular non-nested columns, e.g. google.protobuf.StringValue 'str' for String column 'str'. For Nullable columns empty wrappers are recognized as defaults, and missing as nulls
)", 0) \
    DECLARE(Bool, output_format_protobuf_nullables_with_google_wrappers, false, R"(
When serializing Nullable columns with Google wrappers, serialize default values as empty wrappers. If turned off, default and null values are not serialized
)", 0) \
    DECLARE(UInt64, input_format_csv_skip_first_lines, 0, R"(
Skip specified number of lines at the beginning of data in CSV format
)", 0) \
    DECLARE(UInt64, input_format_tsv_skip_first_lines, 0, R"(
Skip specified number of lines at the beginning of data in TSV format
)", 0) \
    DECLARE(Bool, input_format_csv_skip_trailing_empty_lines, false, R"(
Skip trailing empty lines in CSV format
)", 0) \
    DECLARE(Bool, input_format_tsv_skip_trailing_empty_lines, false, R"(
Skip trailing empty lines in TSV format
)", 0) \
    DECLARE(Bool, input_format_custom_skip_trailing_empty_lines, false, R"(
Skip trailing empty lines in CustomSeparated format
)", 0) \
    DECLARE(Bool, input_format_tsv_crlf_end_of_line, false, R"(
If it is set true, file function will read TSV format with \\r\\n instead of \\n.
)", 0) \
    \
    DECLARE(Bool, input_format_native_allow_types_conversion, true, R"(
Allow data types conversion in Native input format
)", 0) \
    DECLARE(Bool, input_format_native_decode_types_in_binary_format, false, R"(
Read data types in binary format instead of type names in Native input format
)", 0) \
    DECLARE(Bool, output_format_native_encode_types_in_binary_format, false, R"(
Write data types in binary format instead of type names in Native output format
)", 0) \
    DECLARE(Bool, output_format_native_write_json_as_string, false, R"(
Write data of [JSON](../../sql-reference/data-types/newjson.md) column as [String](../../sql-reference/data-types/string.md) column containing JSON strings instead of default native JSON serialization.
)", 0) \
    \
    DECLARE(DateTimeInputFormat, date_time_input_format, FormatSettings::DateTimeInputFormat::Basic, R"(
Allows choosing a parser of the text representation of date and time.

The setting does not apply to [date and time functions](../../sql-reference/functions/date-time-functions.md).

Possible values:

- `'best_effort'` — Enables extended parsing.

    ClickHouse can parse the basic `YYYY-MM-DD HH:MM:SS` format and all [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date and time formats. For example, `'2018-06-08T01:02:03.000Z'`.

- `'basic'` — Use basic parser.

    ClickHouse can parse only the basic `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD` format. For example, `2019-08-20 10:18:56` or `2019-08-20`.

Cloud default value: `'best_effort'`.

See also:

- [DateTime data type.](../../sql-reference/data-types/datetime.md)
- [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)
)", 0) \
    DECLARE(DateTimeOutputFormat, date_time_output_format, FormatSettings::DateTimeOutputFormat::Simple, R"(
Allows choosing different output formats of the text representation of date and time.

Possible values:

- `simple` - Simple output format.

    ClickHouse output date and time `YYYY-MM-DD hh:mm:ss` format. For example, `2019-08-20 10:18:56`. The calculation is performed according to the data type's time zone (if present) or server time zone.

- `iso` - ISO output format.

    ClickHouse output date and time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) `YYYY-MM-DDThh:mm:ssZ` format. For example, `2019-08-20T10:18:56Z`. Note that output is in UTC (`Z` means UTC).

- `unix_timestamp` - Unix timestamp output format.

    ClickHouse output date and time in [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) format. For example `1566285536`.

See also:

- [DateTime data type.](../../sql-reference/data-types/datetime.md)
- [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)
)", 0) \
    DECLARE(IntervalOutputFormat, interval_output_format, FormatSettings::IntervalOutputFormat::Numeric, R"(
Allows choosing different output formats of the text representation of interval types.

Possible values:

-   `kusto` - KQL-style output format.

    ClickHouse outputs intervals in [KQL format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/standard-timespan-format-strings#the-constant-c-format-specifier). For example, `toIntervalDay(2)` would be formatted as `2.00:00:00`. Please note that for interval types of varying length (ie. `IntervalMonth` and `IntervalYear`) the average number of seconds per interval is taken into account.

-   `numeric` - Numeric output format.

    ClickHouse outputs intervals as their underlying numeric representation. For example, `toIntervalDay(2)` would be formatted as `2`.

See also:

-   [Interval](../../sql-reference/data-types/special-data-types/interval.md)
)", 0) \
    \
    DECLARE(Bool, date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands, false, R"(
Dynamically trim the trailing zeros of datetime64 values to adjust the output scale to [0, 3, 6],
corresponding to 'seconds', 'milliseconds', and 'microseconds')", 0) \
    DECLARE(Bool, input_format_ipv4_default_on_conversion_error, false, R"(
Deserialization of IPv4 will use default values instead of throwing exception on conversion error.

Disabled by default.
)", 0) \
    DECLARE(Bool, input_format_ipv6_default_on_conversion_error, false, R"(
Deserialization of IPV6 will use default values instead of throwing exception on conversion error.

Disabled by default.
)", 0) \
    DECLARE(String, bool_true_representation, "true", R"(
Text to represent true bool value in TSV/CSV/Vertical/Pretty formats.
)", 0) \
    DECLARE(String, bool_false_representation, "false", R"(
Text to represent false bool value in TSV/CSV/Vertical/Pretty formats.
)", 0) \
    \
    DECLARE(Bool, input_format_values_interpret_expressions, true, R"(
For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.
)", 0) \
    DECLARE(Bool, input_format_values_deduce_templates_of_expressions, true, R"(
For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.
)", 0) \
    DECLARE(Bool, input_format_values_accurate_types_of_literals, true, R"(
For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.
)", 0) \
    DECLARE(Bool, input_format_avro_allow_missing_fields, false, R"(
For Avro/AvroConfluent format: when field is not found in schema use default value instead of error
)", 0) \
    /** This setting is obsolete and do nothing, left for compatibility reasons. */ \
    DECLARE(Bool, input_format_avro_null_as_default, false, R"(
For Avro/AvroConfluent format: insert default in case of null and non Nullable column
)", 0) \
    DECLARE(UInt64, format_binary_max_string_size, 1_GiB, R"(
The maximum allowed size for String in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit
)", 0) \
    DECLARE(UInt64, format_binary_max_array_size, 1_GiB, R"(
The maximum allowed size for Array in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit
)", 0) \
    DECLARE(Bool, input_format_binary_decode_types_in_binary_format, false, R"(
Read data types in binary format instead of type names in RowBinaryWithNamesAndTypes input format
)", 0) \
    DECLARE(Bool, output_format_binary_encode_types_in_binary_format, false, R"(
Write data types in binary format instead of type names in RowBinaryWithNamesAndTypes output format
)", 0) \
    DECLARE(URI, format_avro_schema_registry_url, "", R"(
For AvroConfluent format: Confluent Schema Registry URL.
)", 0) \
    DECLARE(Bool, input_format_binary_read_json_as_string, false, R"(
Read values of [JSON](../../sql-reference/data-types/newjson.md) data type as JSON [String](../../sql-reference/data-types/string.md) values in RowBinary input format.
)", 0) \
    DECLARE(Bool, output_format_binary_write_json_as_string, false, R"(
Write values of [JSON](../../sql-reference/data-types/newjson.md) data type as JSON [String](../../sql-reference/data-types/string.md) values in RowBinary output format.
)", 0) \
    \
    DECLARE(Bool, output_format_json_quote_64bit_integers, true, R"(
Controls quoting of 64-bit or bigger [integers](../../sql-reference/data-types/int-uint.md) (like `UInt64` or `Int128`) when they are output in a [JSON](../../interfaces/formats.md/#json) format.
Such integers are enclosed in quotes by default. This behavior is compatible with most JavaScript implementations.

Possible values:

- 0 — Integers are output without quotes.
- 1 — Integers are enclosed in quotes.
)", 0) \
    DECLARE(Bool, output_format_json_quote_denormals, false, R"str(
Enables `+nan`, `-nan`, `+inf`, `-inf` outputs in [JSON](../../interfaces/formats.md/#json) output format.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

Consider the following table `account_orders`:

```text
┌─id─┬─name───┬─duration─┬─period─┬─area─┐
│  1 │ Andrew │       20 │      0 │  400 │
│  2 │ John   │       40 │      0 │    0 │
│  3 │ Bob    │       15 │      0 │ -100 │
└────┴────────┴──────────┴────────┴──────┘
```

When `output_format_json_quote_denormals = 0`, the query returns `null` values in output:

```sql
SELECT area/period FROM account_orders FORMAT JSON;
```

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.003648093,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

When `output_format_json_quote_denormals = 1`, the query returns:

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": "inf"
                },
                {
                        "divide(area, period)": "-nan"
                },
                {
                        "divide(area, period)": "-inf"
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.000070241,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```
)str", 0) \
    DECLARE(Bool, output_format_json_quote_decimals, false, R"(
Controls quoting of decimals in JSON output formats.

Disabled by default.
)", 0) \
    DECLARE(Bool, output_format_json_quote_64bit_floats, false, R"(
Controls quoting of 64-bit [floats](../../sql-reference/data-types/float.md) when they are output in JSON* formats.

Disabled by default.
)", 0) \
    \
    DECLARE(Bool, output_format_json_escape_forward_slashes, true, R"(
Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.

Enabled by default.
)", 0) \
    DECLARE(Bool, output_format_json_named_tuples_as_objects, true, R"(
Serialize named tuple columns as JSON objects.

Enabled by default.
)", 0) \
    DECLARE(Bool, output_format_json_skip_null_value_in_named_tuples, false, R"(
Skip key value pairs with null value when serialize named tuple columns as JSON objects. It is only valid when output_format_json_named_tuples_as_objects is true.
)", 0) \
    DECLARE(Bool, output_format_json_array_of_rows, false, R"(
Enables the ability to output all rows as a JSON array in the [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) format.

Possible values:

- 1 — ClickHouse outputs all rows as an array, each row in the `JSONEachRow` format.
- 0 — ClickHouse outputs each row separately in the `JSONEachRow` format.

**Example of a query with the enabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 1;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
[
{"number":"0"},
{"number":"1"},
{"number":"2"}
]
```

**Example of a query with the disabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 0;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
{"number":"0"}
{"number":"1"}
{"number":"2"}
```
)", 0) \
    DECLARE(Bool, output_format_json_validate_utf8, false, R"(
Controls validation of UTF-8 sequences in JSON output formats, doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate UTF-8.

Disabled by default.
)", 0) \
    \
    DECLARE(String, format_json_object_each_row_column_for_object_name, "", R"(
The name of column that will be used for storing/writing object names in [JSONObjectEachRow](../../interfaces/formats.md/#jsonobjecteachrow) format.
Column type should be String. If value is empty, default names `row_{i}`will be used for object names.

### input_format_json_compact_allow_variable_number_of_columns {#input_format_json_compact_allow_variable_number_of_columns}

Allow variable number of columns in rows in JSONCompact/JSONCompactEachRow input formats.
Ignore extra columns in rows with more columns than expected and treat missing columns as default values.

Disabled by default.

### output_format_markdown_escape_special_characters {#output_format_markdown_escape_special_characters}

When enabled, escape special characters in Markdown.

[Common Mark](https://spec.commonmark.org/0.30/#example-12) defines the following special characters that can be escaped by \:

```
! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ _ ` { | } ~
```

Possible values:

+ 0 — Disable.
+ 1 — Enable.

### input_format_json_empty_as_default {#input_format_json_empty_as_default}

When enabled, replace empty input fields in JSON with default values. For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Possible values:

+ 0 — Disable.
+ 1 — Enable.
)", 0) \
    \
    DECLARE(UInt64, output_format_pretty_max_rows, 10000, R"(
Rows limit for Pretty formats.
)", 0) \
    DECLARE(UInt64, output_format_pretty_max_column_pad_width, 250, R"(
Maximum width to pad all values in a column in Pretty formats.
)", 0) \
    DECLARE(UInt64, output_format_pretty_max_value_width, 10000, R"(
Maximum width of value to display in Pretty formats. If greater - it will be cut.
)", 0) \
    DECLARE(UInt64, output_format_pretty_max_value_width_apply_for_single_value, false, R"(
Only cut values (see the `output_format_pretty_max_value_width` setting) when it is not a single value in a block. Otherwise output it entirely, which is useful for the `SHOW CREATE TABLE` query.
)", 0) \
    DECLARE(UInt64Auto, output_format_pretty_color, "auto", R"(
Use ANSI escape sequences in Pretty formats. 0 - disabled, 1 - enabled, 'auto' - enabled if a terminal.
)", 0) \
    DECLARE(String, output_format_pretty_grid_charset, "UTF-8", R"(
Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).
)", 0) \
    DECLARE(UInt64, output_format_pretty_display_footer_column_names, true, R"(
Display column names in the footer if there are many table rows.

Possible values:

- 0 — No column names are displayed in the footer.
- 1 — Column names are displayed in the footer if row count is greater than or equal to the threshold value set by [output_format_pretty_display_footer_column_names_min_rows](#output_format_pretty_display_footer_column_names_min_rows) (50 by default).

**Example**

Query:

```sql
SELECT *, toTypeName(*) FROM (SELECT * FROM system.numbers LIMIT 1000);
```

Result:

```response
      ┌─number─┬─toTypeName(number)─┐
   1. │      0 │ UInt64             │
   2. │      1 │ UInt64             │
   3. │      2 │ UInt64             │
   ...
 999. │    998 │ UInt64             │
1000. │    999 │ UInt64             │
      └─number─┴─toTypeName(number)─┘
```
)", 0) \
    DECLARE(UInt64, output_format_pretty_display_footer_column_names_min_rows, 50, R"(
Sets the minimum number of rows for which a footer with column names will be displayed if setting [output_format_pretty_display_footer_column_names](#output_format_pretty_display_footer_column_names) is enabled.
)", 0) \
    DECLARE(UInt64, output_format_parquet_row_group_size, 1000000, R"(
Target row group size in rows.
)", 0) \
    DECLARE(UInt64, output_format_parquet_row_group_size_bytes, 512 * 1024 * 1024, R"(
Target row group size in bytes, before compression.
)", 0) \
    DECLARE(Bool, output_format_parquet_string_as_string, true, R"(
Use Parquet String type instead of Binary for String columns.
)", 0) \
    DECLARE(Bool, output_format_parquet_fixed_string_as_fixed_byte_array, true, R"(
Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.
)", 0) \
    DECLARE(ParquetVersion, output_format_parquet_version, "2.latest", R"(
Parquet format version for output format. Supported versions: 1.0, 2.4, 2.6 and 2.latest (default)
)", 0) \
    DECLARE(ParquetCompression, output_format_parquet_compression_method, "zstd", R"(
Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)
)", 0) \
    DECLARE(Bool, output_format_parquet_compliant_nested_types, true, R"(
In parquet file schema, use name 'element' instead of 'item' for list elements. This is a historical artifact of Arrow library implementation. Generally increases compatibility, except perhaps with some old versions of Arrow.
)", 0) \
    DECLARE(Bool, output_format_parquet_use_custom_encoder, true, R"(
Use a faster Parquet encoder implementation.
)", 0) \
    DECLARE(Bool, output_format_parquet_parallel_encoding, true, R"(
Do Parquet encoding in multiple threads. Requires output_format_parquet_use_custom_encoder.
)", 0) \
    DECLARE(UInt64, output_format_parquet_data_page_size, 1024 * 1024, R"(
Target page size in bytes, before compression.
)", 0) \
    DECLARE(UInt64, output_format_parquet_batch_size, 1024, R"(
Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.
)", 0) \
    DECLARE(Bool, output_format_parquet_write_page_index, true, R"(
Add a possibility to write page index into parquet files.
)", 0) \
    DECLARE(String, output_format_avro_codec, "", R"(
Compression codec used for output. Possible values: 'null', 'deflate', 'snappy', 'zstd'.
)", 0) \
    DECLARE(UInt64, output_format_avro_sync_interval, 16 * 1024, R"(
Sync interval in bytes.
)", 0) \
    DECLARE(String, output_format_avro_string_column_pattern, "", R"(
For Avro format: regexp of String columns to select as AVRO string.
)", 0) \
    DECLARE(UInt64, output_format_avro_rows_in_file, 1, R"(
Max rows in a file (if permitted by storage)
)", 0) \
    DECLARE(Bool, output_format_tsv_crlf_end_of_line, false, R"(
If it is set true, end of line in TSV format will be \\r\\n instead of \\n.
)", 0) \
    DECLARE(String, format_csv_null_representation, "\\N", R"(
Custom NULL representation in CSV format
)", 0) \
    DECLARE(String, format_tsv_null_representation, "\\N", R"(
Custom NULL representation in TSV format
)", 0) \
    DECLARE(Bool, output_format_decimal_trailing_zeros, false, R"(
Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.

Disabled by default.
)", 0) \
    \
    DECLARE(UInt64, input_format_allow_errors_num, 0, R"(
Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.
)", 0) \
    DECLARE(Float, input_format_allow_errors_ratio, 0, R"(
Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.
)", 0) \
    DECLARE(String, input_format_record_errors_file_path, "", R"(
Path of the file used to record errors while reading text formats (CSV, TSV).
)", 0) \
    DECLARE(String, errors_output_format, "CSV", R"(
Method to write Errors to text output.
)", 0) \
    \
    DECLARE(String, format_schema, "", R"(
This parameter is useful when you are using formats that require a schema definition, such as [Cap’n Proto](https://capnproto.org/) or [Protobuf](https://developers.google.com/protocol-buffers/). The value depends on the format.
)", 0) \
    DECLARE(String, format_template_resultset, "", R"(
Path to file which contains format string for result set (for Template format)
)", 0) \
    DECLARE(String, format_template_row, "", R"(
Path to file which contains format string for rows (for Template format)
)", 0) \
    DECLARE(String, format_template_row_format, "", R"(
Format string for rows (for Template format)
)", 0) \
    DECLARE(String, format_template_resultset_format, "", R"(
Format string for result set (for Template format)
)", 0) \
    DECLARE(String, format_template_rows_between_delimiter, "\n", R"(
Delimiter between rows (for Template format)
)", 0) \
    \
    DECLARE(EscapingRule, format_custom_escaping_rule, "Escaped", R"(
Field escaping rule (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_field_delimiter, "\t", R"(
Delimiter between fields (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_row_before_delimiter, "", R"(
Delimiter before field of the first column (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_row_after_delimiter, "\n", R"(
Delimiter after field of the last column (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_row_between_delimiter, "", R"(
Delimiter between rows (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_result_before_delimiter, "", R"(
Prefix before result set (for CustomSeparated format)
)", 0) \
    DECLARE(String, format_custom_result_after_delimiter, "", R"(
Suffix after result set (for CustomSeparated format)
)", 0) \
    \
    DECLARE(String, format_regexp, "", R"(
Regular expression (for Regexp format)
)", 0) \
    DECLARE(EscapingRule, format_regexp_escaping_rule, "Raw", R"(
Field escaping rule (for Regexp format)
)", 0) \
    DECLARE(Bool, format_regexp_skip_unmatched, false, R"(
Skip lines unmatched by regular expression (for Regexp format)
)", 0) \
    \
    DECLARE(Bool, output_format_enable_streaming, false, R"(
Enable streaming in output formats that support it.

Disabled by default.
)", 0) \
    DECLARE(Bool, output_format_write_statistics, true, R"(
Write statistics about read rows, bytes, time elapsed in suitable output formats.

Enabled by default
)", 0) \
    DECLARE(Bool, output_format_pretty_row_numbers, true, R"(
Add row numbers before each row for pretty output format
)", 0) \
    DECLARE(Bool, output_format_pretty_highlight_digit_groups, true, R"(
If enabled and if output is a terminal, highlight every digit corresponding to the number of thousands, millions, etc. with underline.
)", 0) \
    DECLARE(UInt64, output_format_pretty_single_large_number_tip_threshold, 1'000'000, R"(
Print a readable number tip on the right side of the table if the block consists of a single number which exceeds this value (except 0)
)", 0) \
    DECLARE(Bool, insert_distributed_one_random_shard, false, R"(
Enables or disables random shard insertion into a [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table when there is no distributed key.

By default, when inserting data into a `Distributed` table with more than one shard, the ClickHouse server will reject any insertion request if there is no distributed key. When `insert_distributed_one_random_shard = 1`, insertions are allowed and data is forwarded randomly among all shards.

Possible values:

- 0 — Insertion is rejected if there are multiple shards and no distributed key is given.
- 1 — Insertion is done randomly among all available shards when no distributed key is given.
)", 0) \
    \
    DECLARE(Bool, exact_rows_before_limit, false, R"(
When enabled, ClickHouse will provide exact value for rows_before_limit_at_least statistic, but with the cost that the data before limit will have to be read completely
)", 0) \
    DECLARE(Bool, rows_before_aggregation, false, R"(
When enabled, ClickHouse will provide exact value for rows_before_aggregation statistic, represents the number of rows read before aggregation
)", 0) \
    DECLARE(UInt64, cross_to_inner_join_rewrite, 1, R"(
Use inner join instead of comma/cross join if there are joining expressions in the WHERE section. Values: 0 - no rewrite, 1 - apply if possible for comma/cross, 2 - force rewrite all comma joins, cross - if possible
)", 0) \
    \
    DECLARE(Bool, output_format_arrow_low_cardinality_as_dictionary, false, R"(
Enable output LowCardinality type as Dictionary Arrow type
)", 0) \
    DECLARE(Bool, output_format_arrow_use_signed_indexes_for_dictionary, true, R"(
Use signed integers for dictionary indexes in Arrow format
)", 0) \
    DECLARE(Bool, output_format_arrow_use_64_bit_indexes_for_dictionary, false, R"(
Always use 64 bit integers for dictionary indexes in Arrow format
)", 0) \
    DECLARE(Bool, output_format_arrow_string_as_string, true, R"(
Use Arrow String type instead of Binary for String columns
)", 0) \
    DECLARE(Bool, output_format_arrow_fixed_string_as_fixed_byte_array, true, R"(
Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.
)", 0) \
    DECLARE(ArrowCompression, output_format_arrow_compression_method, "lz4_frame", R"(
Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)
)", 0) \
    \
    DECLARE(Bool, output_format_orc_string_as_string, true, R"(
Use ORC String type instead of Binary for String columns
)", 0) \
    DECLARE(ORCCompression, output_format_orc_compression_method, "zstd", R"(
Compression method for ORC output format. Supported codecs: lz4, snappy, zlib, zstd, none (uncompressed)
)", 0) \
    DECLARE(UInt64, output_format_orc_row_index_stride, 10'000, R"(
Target row index stride in ORC output format
)", 0) \
    DECLARE(Double, output_format_orc_dictionary_key_size_threshold, 0.0, R"(
For a string column in ORC output format, if the number of distinct values is greater than this fraction of the total number of non-null rows, turn off dictionary encoding. Otherwise dictionary encoding is enabled
)", 0) \
    \
    DECLARE(CapnProtoEnumComparingMode, format_capn_proto_enum_comparising_mode, FormatSettings::CapnProtoEnumComparingMode::BY_VALUES, R"(
How to map ClickHouse Enum and CapnProto Enum
)", 0) \
    \
    DECLARE(Bool, format_capn_proto_use_autogenerated_schema, true, R"(
Use autogenerated CapnProto schema when format_schema is not set
)", 0) \
    DECLARE(Bool, format_protobuf_use_autogenerated_schema, true, R"(
Use autogenerated Protobuf when format_schema is not set
)", 0) \
    DECLARE(String, output_format_schema, "", R"(
The path to the file where the automatically generated schema will be saved in [Cap’n Proto](../../interfaces/formats.md#capnproto-capnproto) or [Protobuf](../../interfaces/formats.md#protobuf-protobuf) formats.
)", 0) \
    \
    DECLARE(String, input_format_mysql_dump_table_name, "", R"(
Name of the table in MySQL dump from which to read data
)", 0) \
    DECLARE(Bool, input_format_mysql_dump_map_column_names, true, R"(
Match columns from table in MySQL dump and columns from ClickHouse table by names
)", 0) \
    \
    DECLARE(UInt64, output_format_sql_insert_max_batch_size, DEFAULT_BLOCK_SIZE, R"(
The maximum number  of rows in one INSERT statement.
)", 0) \
    DECLARE(String, output_format_sql_insert_table_name, "table", R"(
The name of table in the output INSERT query
)", 0) \
    DECLARE(Bool, output_format_sql_insert_include_column_names, true, R"(
Include column names in INSERT query
)", 0) \
    DECLARE(Bool, output_format_sql_insert_use_replace, false, R"(
Use REPLACE statement instead of INSERT
)", 0) \
    DECLARE(Bool, output_format_sql_insert_quote_names, true, R"(
Quote column names with '`' characters
)", 0) \
    \
    DECLARE(Bool, output_format_values_escape_quote_with_quote, false, R"(
If true escape ' with '', otherwise quoted with \\'
)", 0) \
    \
    DECLARE(Bool, output_format_bson_string_as_string, false, R"(
Use BSON String type instead of Binary for String columns.
)", 0) \
    DECLARE(Bool, input_format_bson_skip_fields_with_unsupported_types_in_schema_inference, false, R"(
Skip fields with unsupported types while schema inference for format BSON.
)", 0) \
    \
    DECLARE(Bool, format_display_secrets_in_show_and_select, false, R"(
Enables or disables showing secrets in `SHOW` and `SELECT` queries for tables, databases,
table functions, and dictionaries.

User wishing to see secrets must also have
[`display_secrets_in_show_and_select` server setting](../server-configuration-parameters/settings#display_secrets_in_show_and_select)
turned on and a
[`displaySecretsInShowAndSelect`](../../sql-reference/statements/grant#display-secrets) privilege.

Possible values:

-   0 — Disabled.
-   1 — Enabled.
)", IMPORTANT) \
    DECLARE(Bool, regexp_dict_allow_hyperscan, true, R"(
Allow regexp_tree dictionary using Hyperscan library.
)", 0) \
    DECLARE(Bool, regexp_dict_flag_case_insensitive, false, R"(
Use case-insensitive matching for a regexp_tree dictionary. Can be overridden in individual expressions with (?i) and (?-i).
)", 0) \
    DECLARE(Bool, regexp_dict_flag_dotall, false, R"(
Allow '.' to match newline characters for a regexp_tree dictionary.
)", 0) \
    \
    DECLARE(Bool, dictionary_use_async_executor, false, R"(
Execute a pipeline for reading dictionary source in several threads. It's supported only by dictionaries with local CLICKHOUSE source.
)", 0) \
    DECLARE(Bool, precise_float_parsing, false, R"(
Prefer more precise (but slower) float parsing algorithm
)", 0) \
    DECLARE(DateTimeOverflowBehavior, date_time_overflow_behavior, "ignore", R"(
Overflow mode for Date, Date32, DateTime, DateTime64 types. Possible values: 'ignore', 'throw', 'saturate'.
)", 0) \
    DECLARE(Bool, validate_experimental_and_suspicious_types_inside_nested_types, true, R"(
Validate usage of experimental and suspicious types inside nested types like Array/Map/Tuple
)", 0) \
    \
    DECLARE(IdentifierQuotingRule, show_create_query_identifier_quoting_rule, IdentifierQuotingRule::WhenNecessary, R"(
Set the quoting rule for identifiers in SHOW CREATE query
)", 0) \
    DECLARE(IdentifierQuotingStyle, show_create_query_identifier_quoting_style, IdentifierQuotingStyle::Backticks, R"(
Set the quoting style for identifiers in SHOW CREATE query
)", 0) \

// End of FORMAT_FACTORY_SETTINGS

#define OBSOLETE_FORMAT_SETTINGS(M, ALIAS) \
    /** Obsolete format settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    MAKE_OBSOLETE(M, Bool, input_format_arrow_import_nested, false) \
    MAKE_OBSOLETE(M, Bool, input_format_parquet_import_nested, false) \
    MAKE_OBSOLETE(M, Bool, input_format_orc_import_nested, false)                                                                          \

#endif // __CLION_IDE__

#define LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \
    FORMAT_FACTORY_SETTINGS(M, ALIAS) \
    OBSOLETE_FORMAT_SETTINGS(M, ALIAS)

