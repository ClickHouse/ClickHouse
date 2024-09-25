#pragma once

#include <Core/SettingsObsoleteMacros.h>

/// This header exists so we can share it between Settings.cpp, FormatFactorySettings.cpp and other storage settings

// clang-format off
#if defined(__CLION_IDE__)
/// CLion freezes for a minute every time is processes this
#define FORMAT_FACTORY_SETTINGS(M, ALIAS)
#define OBSOLETE_FORMAT_SETTINGS(M, ALIAS)
#else

#define FORMAT_FACTORY_SETTINGS(M, ALIAS) \
    M(Char, format_csv_delimiter, ',', "The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.", 0) \
    M(Bool, format_csv_allow_single_quotes, false, "If it is set to true, allow strings in single quotes.", 0) \
    M(Bool, format_csv_allow_double_quotes, true, "If it is set to true, allow strings in double quotes.", 0) \
    M(Bool, output_format_csv_serialize_tuple_into_separate_columns, true, "If it set to true, then Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost)", 0) \
    M(Bool, input_format_csv_deserialize_separate_columns_into_tuple, true, "If it set to true, then separate columns written in CSV format can be deserialized to Tuple column.", 0) \
    M(Bool, output_format_csv_crlf_end_of_line, false, "If it is set true, end of line in CSV format will be \\r\\n instead of \\n.", 0) \
    M(Bool, input_format_csv_allow_cr_end_of_line, false, "If it is set true, \\r will be allowed at end of line not followed by \\n", 0) \
    M(Bool, input_format_csv_enum_as_number, false, "Treat inserted enum values in CSV formats as enum indices", 0) \
    M(Bool, input_format_csv_arrays_as_nested_csv, false, R"(When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: "[""Hello"", ""world"", ""42"""" TV""]". Braces around array can be omitted.)", 0) \
    M(Bool, input_format_skip_unknown_fields, true, "Skip columns with unknown names from input data (it works for JSONEachRow, -WithNames, -WithNamesAndTypes and TSKV formats).", 0) \
    M(Bool, input_format_with_names_use_header, true, "For -WithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.", 0) \
    M(Bool, input_format_with_types_use_header, true, "For -WithNamesAndTypes input formats this controls whether format parser should check if data types from the input match data types from the header.", 0) \
    M(Bool, input_format_import_nested_json, false, "Map nested JSON data to nested tables (it works for JSONEachRow format).", 0) \
    M(Bool, input_format_defaults_for_omitted_fields, true, "For input data calculate default expressions for omitted fields (it works for JSONEachRow, -WithNames, -WithNamesAndTypes formats).", IMPORTANT) \
    M(Bool, input_format_csv_empty_as_default, true, "Treat empty fields in CSV input as default values.", 0) \
    M(Bool, input_format_tsv_empty_as_default, false, "Treat empty fields in TSV input as default values.", 0) \
    M(Bool, input_format_tsv_enum_as_number, false, "Treat inserted enum values in TSV formats as enum indices.", 0) \
    M(Bool, input_format_null_as_default, true, "Initialize null fields with default values if the data type of this field is not nullable and it is supported by the input format", 0) \
    M(Bool, input_format_force_null_for_omitted_fields, false, "Force initialize omitted fields with null values", 0) \
    M(Bool, input_format_arrow_case_insensitive_column_matching, false, "Ignore case when matching Arrow columns with CH columns.", 0) \
    M(Int64, input_format_orc_row_batch_size, 100'000, "Batch size when reading ORC stripes.", 0) \
    M(Bool, input_format_orc_case_insensitive_column_matching, false, "Ignore case when matching ORC columns with CH columns.", 0) \
    M(Bool, input_format_parquet_case_insensitive_column_matching, false, "Ignore case when matching Parquet columns with CH columns.", 0) \
    M(Bool, input_format_parquet_preserve_order, false, "Avoid reordering rows when reading from Parquet files. Usually makes it much slower.", 0) \
    M(Bool, input_format_parquet_filter_push_down, true, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.", 0) \
    M(Bool, input_format_parquet_use_native_reader, false, "When reading Parquet files, to use native reader instead of arrow reader.", 0) \
    M(Bool, input_format_allow_seeks, true, "Allow seeks while reading in ORC/Parquet/Arrow input formats", 0) \
    M(Bool, input_format_orc_allow_missing_columns, true, "Allow missing columns while reading ORC input formats", 0) \
    M(Bool, input_format_orc_use_fast_decoder, true, "Use a faster ORC decoder implementation.", 0) \
    M(Bool, input_format_orc_filter_push_down, true, "When reading ORC files, skip whole stripes or row groups based on the WHERE/PREWHERE expressions, min/max statistics or bloom filter in the ORC metadata.", 0) \
    M(String, input_format_orc_reader_time_zone_name, "GMT", "The time zone name for ORC row reader, the default ORC row reader's time zone is GMT.", 0) \
    M(Bool, input_format_parquet_allow_missing_columns, true, "Allow missing columns while reading Parquet input formats", 0) \
    M(UInt64, input_format_parquet_local_file_min_bytes_for_seek, 8192, "Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format", 0) \
    M(Bool, input_format_arrow_allow_missing_columns, true, "Allow missing columns while reading Arrow input formats", 0) \
    M(Char, input_format_hive_text_fields_delimiter, '\x01', "Delimiter between fields in Hive Text File", 0) \
    M(Char, input_format_hive_text_collection_items_delimiter, '\x02', "Delimiter between collection(array or map) items in Hive Text File", 0) \
    M(Char, input_format_hive_text_map_keys_delimiter, '\x03', "Delimiter between a pair of map key/values in Hive Text File", 0) \
    M(Bool, input_format_hive_text_allow_variable_number_of_columns, true, "Ignore extra columns in Hive Text input (if file has more columns than expected) and treat missing fields in Hive Text input as default values", 0) \
    M(UInt64, input_format_msgpack_number_of_columns, 0, "The number of columns in inserted MsgPack data. Used for automatic schema inference from data.", 0) \
    M(MsgPackUUIDRepresentation, output_format_msgpack_uuid_representation, FormatSettings::MsgPackUUIDRepresentation::EXT, "The way how to output UUID in MsgPack format.", 0) \
    M(UInt64, input_format_max_rows_to_read_for_schema_inference, 25000, "The maximum rows of data to read for automatic schema inference", 0) \
    M(UInt64, input_format_max_bytes_to_read_for_schema_inference, 32 * 1024 * 1024, "The maximum bytes of data to read for automatic schema inference", 0) \
    M(Bool, input_format_csv_use_best_effort_in_schema_inference, true, "Use some tweaks and heuristics to infer schema in CSV format", 0) \
    M(Bool, input_format_csv_try_infer_numbers_from_strings, false, "Try to infer numbers from string fields while schema inference in CSV format", 0) \
    M(Bool, input_format_csv_try_infer_strings_from_quoted_tuples, true, "Interpret quoted tuples in the input data as a value of type String.", 0) \
    M(Bool, input_format_tsv_use_best_effort_in_schema_inference, true, "Use some tweaks and heuristics to infer schema in TSV format", 0) \
    M(Bool, input_format_csv_detect_header, true, "Automatically detect header with names and types in CSV format", 0) \
    M(Bool, input_format_csv_allow_whitespace_or_tab_as_delimiter, false, "Allow to use spaces and tabs(\\t) as field delimiter in the CSV strings", 0) \
    M(Bool, input_format_csv_trim_whitespaces, true, "Trims spaces and tabs (\\t) characters at the beginning and end in CSV strings", 0) \
    M(Bool, input_format_csv_use_default_on_bad_values, false, "Allow to set default value to column when CSV field deserialization failed on bad value", 0) \
    M(Bool, input_format_csv_allow_variable_number_of_columns, false, "Ignore extra columns in CSV input (if file has more columns than expected) and treat missing fields in CSV input as default values", 0) \
    M(Bool, input_format_tsv_allow_variable_number_of_columns, false, "Ignore extra columns in TSV input (if file has more columns than expected) and treat missing fields in TSV input as default values", 0) \
    M(Bool, input_format_custom_allow_variable_number_of_columns, false, "Ignore extra columns in CustomSeparated input (if file has more columns than expected) and treat missing fields in CustomSeparated input as default values", 0) \
    M(Bool, input_format_json_compact_allow_variable_number_of_columns, false, "Ignore extra columns in JSONCompact(EachRow) input (if file has more columns than expected) and treat missing fields in JSONCompact(EachRow) input as default values", 0) \
    M(Bool, input_format_tsv_detect_header, true, "Automatically detect header with names and types in TSV format", 0) \
    M(Bool, input_format_custom_detect_header, true, "Automatically detect header with names and types in CustomSeparated format", 0) \
    M(Bool, input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format Parquet", 0) \
    M(UInt64, input_format_parquet_max_block_size, DEFAULT_BLOCK_SIZE, "Max block size for parquet reader.", 0) \
    M(UInt64, input_format_parquet_prefer_block_bytes, DEFAULT_BLOCK_SIZE * 256, "Average block bytes output by parquet reader", 0) \
    M(Bool, input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip fields with unsupported types while schema inference for format Protobuf", 0) \
    M(Bool, input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format CapnProto", 0) \
    M(Bool, input_format_orc_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format ORC", 0) \
    M(Bool, input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference, false, "Skip columns with unsupported types while schema inference for format Arrow", 0) \
    M(String, column_names_for_schema_inference, "", "The list of column names to use in schema inference for formats without column names. The format: 'column1,column2,column3,...'", 0) \
    M(String, schema_inference_hints, "", "The list of column names and types to use in schema inference for formats without column names. The format: 'column_name1 column_type1, column_name2 column_type2, ...'", 0) \
    M(SchemaInferenceMode, schema_inference_mode, "default", "Mode of schema inference. 'default' - assume that all files have the same schema and schema can be inferred from any file, 'union' - files can have different schemas and the resulting schema should be the a union of schemas of all files", 0) \
    M(UInt64Auto, schema_inference_make_columns_nullable, 1, "If set to true, all inferred types will be Nullable in schema inference. When set to false, no columns will be converted to Nullable. When set to 'auto', ClickHouse will use information about nullability from the data.", 0) \
    M(Bool, input_format_json_read_bools_as_numbers, true, "Allow to parse bools as numbers in JSON input formats", 0) \
    M(Bool, input_format_json_read_bools_as_strings, true, "Allow to parse bools as strings in JSON input formats", 0) \
    M(Bool, input_format_json_try_infer_numbers_from_strings, false, "Try to infer numbers from string fields while schema inference", 0) \
    M(Bool, input_format_json_validate_types_from_metadata, true, "For JSON/JSONCompact/JSONColumnsWithMetadata input formats this controls whether format parser should check if data types from input metadata match data types of the corresponding columns from the table", 0) \
    M(Bool, input_format_json_read_numbers_as_strings, true, "Allow to parse numbers as strings in JSON input formats", 0) \
    M(Bool, input_format_json_read_objects_as_strings, true, "Allow to parse JSON objects as strings in JSON input formats", 0) \
    M(Bool, input_format_json_read_arrays_as_strings, true, "Allow to parse JSON arrays as strings in JSON input formats", 0) \
    M(Bool, input_format_json_try_infer_named_tuples_from_objects, true, "Try to infer named tuples from JSON objects in JSON input formats", 0) \
    M(Bool, input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects, false, "Use String type instead of an exception in case of ambiguous paths in JSON objects during named tuples inference", 0) \
    M(Bool, input_format_json_infer_incomplete_types_as_strings, true, "Use type String for keys that contains only Nulls or empty objects/arrays during schema inference in JSON input formats", 0) \
    M(Bool, input_format_json_named_tuples_as_objects, true, "Deserialize named tuple columns as JSON objects", 0) \
    M(Bool, input_format_json_ignore_unknown_keys_in_named_tuple, true, "Ignore unknown keys in json object for named tuples", 0) \
    M(Bool, input_format_json_defaults_for_missing_elements_in_named_tuple, true, "Insert default value in named tuple element if it's missing in json object", 0) \
    M(Bool, input_format_json_throw_on_bad_escape_sequence, true, "Throw an exception if JSON string contains bad escape sequence in JSON input formats. If disabled, bad escape sequences will remain as is in the data", 0) \
    M(Bool, input_format_json_ignore_unnecessary_fields, true, "Ignore unnecessary fields and not parse them. Enabling this may not throw exceptions on json strings of invalid format or with duplicated fields", 0) \
    M(Bool, input_format_try_infer_variants, false, "Try to infer the Variant type in text formats when there is more than one possible type for column/array elements", 0) \
    M(Bool, type_json_skip_duplicated_paths, false, "When enabled, during parsing JSON object into JSON type duplicated paths will be ignored and only the first one will be inserted instead of an exception", 0) \
    M(UInt64, input_format_json_max_depth, 1000, "Maximum depth of a field in JSON. This is not a strict limit, it does not have to be applied precisely.", 0) \
    M(Bool, input_format_json_empty_as_default, false, "Treat empty fields in JSON input as default values.", 0) \
    M(Bool, input_format_try_infer_integers, true, "Try to infer integers instead of floats while schema inference in text formats", 0) \
    M(Bool, input_format_try_infer_dates, true, "Try to infer dates from string fields while schema inference in text formats", 0) \
    M(Bool, input_format_try_infer_datetimes, true, "Try to infer datetimes from string fields while schema inference in text formats", 0) \
    M(Bool, input_format_try_infer_datetimes_only_datetime64, false, "When input_format_try_infer_datetimes is enabled, infer only DateTime64 but not DateTime types", 0) \
    M(Bool, input_format_try_infer_exponent_floats, false, "Try to infer floats in exponential notation while schema inference in text formats (except JSON, where exponent numbers are always inferred)", 0) \
    M(Bool, output_format_markdown_escape_special_characters, false, "Escape special characters in Markdown", 0) \
    M(Bool, input_format_protobuf_flatten_google_wrappers, false, "Enable Google wrappers for regular non-nested columns, e.g. google.protobuf.StringValue 'str' for String column 'str'. For Nullable columns empty wrappers are recognized as defaults, and missing as nulls", 0) \
    M(Bool, output_format_protobuf_nullables_with_google_wrappers, false, "When serializing Nullable columns with Google wrappers, serialize default values as empty wrappers. If turned off, default and null values are not serialized", 0) \
    M(UInt64, input_format_csv_skip_first_lines, 0, "Skip specified number of lines at the beginning of data in CSV format", 0) \
    M(UInt64, input_format_tsv_skip_first_lines, 0, "Skip specified number of lines at the beginning of data in TSV format", 0) \
    M(Bool, input_format_csv_skip_trailing_empty_lines, false, "Skip trailing empty lines in CSV format", 0) \
    M(Bool, input_format_tsv_skip_trailing_empty_lines, false, "Skip trailing empty lines in TSV format", 0) \
    M(Bool, input_format_custom_skip_trailing_empty_lines, false, "Skip trailing empty lines in CustomSeparated format", 0) \
    M(Bool, input_format_tsv_crlf_end_of_line, false, "If it is set true, file function will read TSV format with \\r\\n instead of \\n.", 0) \
    \
    M(Bool, input_format_native_allow_types_conversion, true, "Allow data types conversion in Native input format", 0) \
    M(Bool, input_format_native_decode_types_in_binary_format, false, "Read data types in binary format instead of type names in Native input format", 0) \
    M(Bool, output_format_native_encode_types_in_binary_format, false, "Write data types in binary format instead of type names in Native output format", 0) \
    \
    M(DateTimeInputFormat, date_time_input_format, FormatSettings::DateTimeInputFormat::Basic, "Method to read DateTime from text input formats. Possible values: 'basic', 'best_effort' and 'best_effort_us'.", 0) \
    M(DateTimeOutputFormat, date_time_output_format, FormatSettings::DateTimeOutputFormat::Simple, "Method to write DateTime to text output. Possible values: 'simple', 'iso', 'unix_timestamp'.", 0) \
    M(IntervalOutputFormat, interval_output_format, FormatSettings::IntervalOutputFormat::Numeric, "Textual representation of Interval. Possible values: 'kusto', 'numeric'.", 0) \
    \
    M(Bool, input_format_ipv4_default_on_conversion_error, false, "Deserialization of IPv4 will use default values instead of throwing exception on conversion error.", 0) \
    M(Bool, input_format_ipv6_default_on_conversion_error, false, "Deserialization of IPV6 will use default values instead of throwing exception on conversion error.", 0) \
    M(String, bool_true_representation, "true", "Text to represent bool value in TSV/CSV formats.", 0) \
    M(String, bool_false_representation, "false", "Text to represent bool value in TSV/CSV formats.", 0) \
    \
    M(Bool, input_format_values_interpret_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.", 0) \
    M(Bool, input_format_values_deduce_templates_of_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.", 0) \
    M(Bool, input_format_values_accurate_types_of_literals, true, "For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.", 0) \
    M(Bool, input_format_avro_allow_missing_fields, false, "For Avro/AvroConfluent format: when field is not found in schema use default value instead of error", 0) \
    /** This setting is obsolete and do nothing, left for compatibility reasons. */ \
    M(Bool, input_format_avro_null_as_default, false, "For Avro/AvroConfluent format: insert default in case of null and non Nullable column", 0) \
    M(UInt64, format_binary_max_string_size, 1_GiB, "The maximum allowed size for String in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit", 0) \
    M(UInt64, format_binary_max_array_size, 1_GiB, "The maximum allowed size for Array in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit", 0) \
    M(Bool, input_format_binary_decode_types_in_binary_format, false, "Read data types in binary format instead of type names in RowBinaryWithNamesAndTypes input format", 0) \
    M(Bool, output_format_binary_encode_types_in_binary_format, false, "Write data types in binary format instead of type names in RowBinaryWithNamesAndTypes output format ", 0) \
    M(URI, format_avro_schema_registry_url, "", "For AvroConfluent format: Confluent Schema Registry URL.", 0) \
    \
    M(Bool, output_format_json_quote_64bit_integers, true, "Controls quoting of 64-bit integers in JSON output format.", 0) \
    M(Bool, output_format_json_quote_denormals, false, "Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.", 0) \
    M(Bool, output_format_json_quote_decimals, false, "Controls quoting of decimals in JSON output format.", 0) \
    M(Bool, output_format_json_quote_64bit_floats, false, "Controls quoting of 64-bit float numbers in JSON output format.", 0) \
    \
    M(Bool, output_format_json_escape_forward_slashes, true, "Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.", 0) \
    M(Bool, output_format_json_named_tuples_as_objects, true, "Serialize named tuple columns as JSON objects.", 0) \
    M(Bool, output_format_json_skip_null_value_in_named_tuples, false, "Skip key value pairs with null value when serialize named tuple columns as JSON objects. It is only valid when output_format_json_named_tuples_as_objects is true.", 0) \
    M(Bool, output_format_json_array_of_rows, false, "Output a JSON array of all rows in JSONEachRow(Compact) format.", 0) \
    M(Bool, output_format_json_validate_utf8, false, "Validate UTF-8 sequences in JSON output formats, doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8", 0) \
    \
    M(String, format_json_object_each_row_column_for_object_name, "", "The name of column that will be used as object names in JSONObjectEachRow format. Column type should be String", 0) \
    \
    M(UInt64, output_format_pretty_max_rows, 10000, "Rows limit for Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_column_pad_width, 250, "Maximum width to pad all values in a column in Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_value_width, 10000, "Maximum width of value to display in Pretty formats. If greater - it will be cut.", 0) \
    M(UInt64, output_format_pretty_max_value_width_apply_for_single_value, false, "Only cut values (see the `output_format_pretty_max_value_width` setting) when it is not a single value in a block. Otherwise output it entirely, which is useful for the `SHOW CREATE TABLE` query.", 0) \
    M(UInt64Auto, output_format_pretty_color, "auto", "Use ANSI escape sequences in Pretty formats. 0 - disabled, 1 - enabled, 'auto' - enabled if a terminal.", 0) \
    M(String, output_format_pretty_grid_charset, "UTF-8", "Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).", 0) \
    M(UInt64, output_format_pretty_display_footer_column_names, true, "Display column names in the footer if there are 999 or more rows.", 0) \
    M(UInt64, output_format_pretty_display_footer_column_names_min_rows, 50, "Sets the minimum threshold value of rows for which to enable displaying column names in the footer. 50 (default)", 0) \
    M(UInt64, output_format_parquet_row_group_size, 1000000, "Target row group size in rows.", 0) \
    M(UInt64, output_format_parquet_row_group_size_bytes, 512 * 1024 * 1024, "Target row group size in bytes, before compression.", 0) \
    M(Bool, output_format_parquet_string_as_string, true, "Use Parquet String type instead of Binary for String columns.", 0) \
    M(Bool, output_format_parquet_fixed_string_as_fixed_byte_array, true, "Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.", 0) \
    M(ParquetVersion, output_format_parquet_version, "2.latest", "Parquet format version for output format. Supported versions: 1.0, 2.4, 2.6 and 2.latest (default)", 0) \
    M(ParquetCompression, output_format_parquet_compression_method, "zstd", "Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)", 0) \
    M(Bool, output_format_parquet_compliant_nested_types, true, "In parquet file schema, use name 'element' instead of 'item' for list elements. This is a historical artifact of Arrow library implementation. Generally increases compatibility, except perhaps with some old versions of Arrow.", 0) \
    M(Bool, output_format_parquet_use_custom_encoder, true, "Use a faster Parquet encoder implementation.", 0) \
    M(Bool, output_format_parquet_parallel_encoding, true, "Do Parquet encoding in multiple threads. Requires output_format_parquet_use_custom_encoder.", 0) \
    M(UInt64, output_format_parquet_data_page_size, 1024 * 1024, "Target page size in bytes, before compression.", 0) \
    M(UInt64, output_format_parquet_batch_size, 1024, "Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.", 0) \
    M(Bool, output_format_parquet_write_page_index, true, "Add a possibility to write page index into parquet files.", 0) \
    M(String, output_format_avro_codec, "", "Compression codec used for output. Possible values: 'null', 'deflate', 'snappy', 'zstd'.", 0) \
    M(UInt64, output_format_avro_sync_interval, 16 * 1024, "Sync interval in bytes.", 0) \
    M(String, output_format_avro_string_column_pattern, "", "For Avro format: regexp of String columns to select as AVRO string.", 0) \
    M(UInt64, output_format_avro_rows_in_file, 1, "Max rows in a file (if permitted by storage)", 0) \
    M(Bool, output_format_tsv_crlf_end_of_line, false, "If it is set true, end of line in TSV format will be \\r\\n instead of \\n.", 0) \
    M(String, format_csv_null_representation, "\\N", "Custom NULL representation in CSV format", 0) \
    M(String, format_tsv_null_representation, "\\N", "Custom NULL representation in TSV format", 0) \
    M(Bool, output_format_decimal_trailing_zeros, false, "Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.", 0) \
    \
    M(UInt64, input_format_allow_errors_num, 0, "Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(Float, input_format_allow_errors_ratio, 0, "Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(String, input_format_record_errors_file_path, "", "Path of the file used to record errors while reading text formats (CSV, TSV).", 0) \
    M(String, errors_output_format, "CSV", "Method to write Errors to text output.", 0) \
    \
    M(String, format_schema, "", "Schema identifier (used by schema-based formats)", 0) \
    M(String, format_template_resultset, "", "Path to file which contains format string for result set (for Template format)", 0) \
    M(String, format_template_row, "", "Path to file which contains format string for rows (for Template format)", 0) \
    M(String, format_template_row_format, "", "Format string for rows (for Template format)", 0) \
    M(String, format_template_resultset_format, "", "Format string for result set (for Template format)", 0) \
    M(String, format_template_rows_between_delimiter, "\n", "Delimiter between rows (for Template format)", 0) \
    \
    M(EscapingRule, format_custom_escaping_rule, "Escaped", "Field escaping rule (for CustomSeparated format)", 0) \
    M(String, format_custom_field_delimiter, "\t", "Delimiter between fields (for CustomSeparated format)", 0) \
    M(String, format_custom_row_before_delimiter, "", "Delimiter before field of the first column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_after_delimiter, "\n", "Delimiter after field of the last column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_between_delimiter, "", "Delimiter between rows (for CustomSeparated format)", 0) \
    M(String, format_custom_result_before_delimiter, "", "Prefix before result set (for CustomSeparated format)", 0) \
    M(String, format_custom_result_after_delimiter, "", "Suffix after result set (for CustomSeparated format)", 0) \
    \
    M(String, format_regexp, "", "Regular expression (for Regexp format)", 0) \
    M(EscapingRule, format_regexp_escaping_rule, "Raw", "Field escaping rule (for Regexp format)", 0) \
    M(Bool, format_regexp_skip_unmatched, false, "Skip lines unmatched by regular expression (for Regexp format)", 0) \
    \
    M(Bool, output_format_enable_streaming, false, "Enable streaming in output formats that support it.", 0) \
    M(Bool, output_format_write_statistics, true, "Write statistics about read rows, bytes, time elapsed in suitable output formats.", 0) \
    M(Bool, output_format_pretty_row_numbers, true, "Add row numbers before each row for pretty output format", 0) \
    M(Bool, output_format_pretty_highlight_digit_groups, true, "If enabled and if output is a terminal, highlight every digit corresponding to the number of thousands, millions, etc. with underline.", 0) \
    M(UInt64, output_format_pretty_single_large_number_tip_threshold, 1'000'000, "Print a readable number tip on the right side of the table if the block consists of a single number which exceeds this value (except 0)", 0) \
    M(Bool, insert_distributed_one_random_shard, false, "If setting is enabled, inserting into distributed table will choose a random shard to write when there is no sharding key", 0) \
    \
    M(Bool, exact_rows_before_limit, false, "When enabled, ClickHouse will provide exact value for rows_before_limit_at_least statistic, but with the cost that the data before limit will have to be read completely", 0) \
    M(Bool, rows_before_aggregation, false, "When enabled, ClickHouse will provide exact value for rows_before_aggregation statistic, represents the number of rows read before aggregation", 0) \
    M(UInt64, cross_to_inner_join_rewrite, 1, "Use inner join instead of comma/cross join if there are joining expressions in the WHERE section. Values: 0 - no rewrite, 1 - apply if possible for comma/cross, 2 - force rewrite all comma joins, cross - if possible", 0) \
    \
    M(Bool, output_format_arrow_low_cardinality_as_dictionary, false, "Enable output LowCardinality type as Dictionary Arrow type", 0) \
    M(Bool, output_format_arrow_use_signed_indexes_for_dictionary, true, "Use signed integers for dictionary indexes in Arrow format", 0) \
    M(Bool, output_format_arrow_use_64_bit_indexes_for_dictionary, false, "Always use 64 bit integers for dictionary indexes in Arrow format", 0) \
    M(Bool, output_format_arrow_string_as_string, true, "Use Arrow String type instead of Binary for String columns", 0) \
    M(Bool, output_format_arrow_fixed_string_as_fixed_byte_array, true, "Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.", 0) \
    M(ArrowCompression, output_format_arrow_compression_method, "lz4_frame", "Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)", 0) \
    \
    M(Bool, output_format_orc_string_as_string, true, "Use ORC String type instead of Binary for String columns", 0) \
    M(ORCCompression, output_format_orc_compression_method, "zstd", "Compression method for ORC output format. Supported codecs: lz4, snappy, zlib, zstd, none (uncompressed)", 0) \
    M(UInt64, output_format_orc_row_index_stride, 10'000, "Target row index stride in ORC output format", 0) \
    M(Double, output_format_orc_dictionary_key_size_threshold, 0.0, "For a string column in ORC output format, if the number of distinct values is greater than this fraction of the total number of non-null rows, turn off dictionary encoding. Otherwise dictionary encoding is enabled", 0) \
    \
    M(CapnProtoEnumComparingMode, format_capn_proto_enum_comparising_mode, FormatSettings::CapnProtoEnumComparingMode::BY_VALUES, "How to map ClickHouse Enum and CapnProto Enum", 0) \
    \
    M(Bool, format_capn_proto_use_autogenerated_schema, true, "Use autogenerated CapnProto schema when format_schema is not set", 0) \
    M(Bool, format_protobuf_use_autogenerated_schema, true, "Use autogenerated Protobuf when format_schema is not set", 0) \
    M(String, output_format_schema, "", "The path to the file where the automatically generated schema will be saved", 0) \
    \
    M(String, input_format_mysql_dump_table_name, "", "Name of the table in MySQL dump from which to read data", 0) \
    M(Bool, input_format_mysql_dump_map_column_names, true, "Match columns from table in MySQL dump and columns from ClickHouse table by names", 0) \
    \
    M(UInt64, output_format_sql_insert_max_batch_size, DEFAULT_BLOCK_SIZE, "The maximum number  of rows in one INSERT statement.", 0) \
    M(String, output_format_sql_insert_table_name, "table", "The name of table in the output INSERT query", 0) \
    M(Bool, output_format_sql_insert_include_column_names, true, "Include column names in INSERT query", 0) \
    M(Bool, output_format_sql_insert_use_replace, false, "Use REPLACE statement instead of INSERT", 0) \
    M(Bool, output_format_sql_insert_quote_names, true, "Quote column names with '`' characters", 0) \
    \
    M(Bool, output_format_values_escape_quote_with_quote, false, "If true escape ' with '', otherwise quoted with \\'", 0) \
    \
    M(Bool, output_format_bson_string_as_string, false, "Use BSON String type instead of Binary for String columns.", 0) \
    M(Bool, input_format_bson_skip_fields_with_unsupported_types_in_schema_inference, false, "Skip fields with unsupported types while schema inference for format BSON.", 0) \
    \
    M(Bool, format_display_secrets_in_show_and_select, false, "Do not hide secrets in SHOW and SELECT queries.", IMPORTANT) \
    M(Bool, regexp_dict_allow_hyperscan, true, "Allow regexp_tree dictionary using Hyperscan library.", 0) \
    M(Bool, regexp_dict_flag_case_insensitive, false, "Use case-insensitive matching for a regexp_tree dictionary. Can be overridden in individual expressions with (?i) and (?-i).", 0) \
    M(Bool, regexp_dict_flag_dotall, false, "Allow '.' to match newline characters for a regexp_tree dictionary.", 0) \
    \
    M(Bool, dictionary_use_async_executor, false, "Execute a pipeline for reading dictionary source in several threads. It's supported only by dictionaries with local CLICKHOUSE source.", 0) \
    M(Bool, precise_float_parsing, false, "Prefer more precise (but slower) float parsing algorithm", 0) \
    M(DateTimeOverflowBehavior, date_time_overflow_behavior, "ignore", "Overflow mode for Date, Date32, DateTime, DateTime64 types. Possible values: 'ignore', 'throw', 'saturate'.", 0) \
    M(Bool, validate_experimental_and_suspicious_types_inside_nested_types, true, "Validate usage of experimental and suspicious types inside nested types like Array/Map/Tuple", 0) \
    \
    M(Bool, output_format_always_quote_identifiers, false, "Always quote identifiers", 0) \
    M(IdentifierQuotingStyle, output_format_identifier_quoting_style, IdentifierQuotingStyle::Backticks, "Set the quoting style for identifiers", 0) \

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
