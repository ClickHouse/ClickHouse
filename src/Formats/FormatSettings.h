#pragma once

#include <Core/Defines.h>
#include <Core/Names.h>
#include <base/types.h>
#include <base/unit.h>

namespace DB
{

/**
  * Various tweaks for input/output formats. Text serialization/deserialization
  * of data types also depend on some of these settings. It is different from
  * FormatFactorySettings in that it has all necessary user-provided settings
  * combined with information from context etc, that we can use directly during
  * serialization. In contrast, FormatFactorySettings' job is to reflect the
  * changes made to user-visible format settings, such as when tweaking the
  * the format for File engine.
  * NOTE Parameters for unrelated formats and unrelated data types are collected
  * in this struct - it prevents modularity, but they are difficult to separate.
  */
struct FormatSettings
{
    /// Format will be used for streaming. Not every formats support it
    /// Option means that each chunk of data need to be formatted independently. Also each chunk will be flushed at the end of processing.
    bool enable_streaming = false;

    bool skip_unknown_fields = false;
    bool with_names_use_header = false;
    bool with_types_use_header = false;
    bool write_statistics = true;
    bool import_nested_json = false;
    bool null_as_default = true;
    bool force_null_for_omitted_fields = false;
    bool decimal_trailing_zeros = false;
    bool defaults_for_omitted_fields = true;
    bool is_writing_to_terminal = false;
    bool try_infer_variant = false;

    bool seekable_read = true;
    UInt64 max_rows_to_read_for_schema_inference = 25000;
    UInt64 max_bytes_to_read_for_schema_inference = 32 * 1024 * 1024;

    String column_names_for_schema_inference{};
    String schema_inference_hints{};

    bool try_infer_integers = true;
    bool try_infer_dates = true;
    bool try_infer_datetimes = true;
    bool try_infer_datetimes_only_datetime64 = false;
    bool try_infer_exponent_floats = false;

    enum class DateTimeInputFormat : uint8_t
    {
        Basic,        /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort,   /// Use sophisticated rules to parse whatever possible.
        BestEffortUS  /// Use sophisticated rules to parse American style: mm/dd/yyyy
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;

    enum class DateTimeOutputFormat : uint8_t
    {
        Simple,
        ISO,
        UnixTimestamp
    };

    enum class EscapingRule : uint8_t
    {
        None,
        Escaped,
        Quoted,
        CSV,
        JSON,
        XML,
        Raw
    };

    UInt64 schema_inference_make_columns_nullable = 1;

    DateTimeOutputFormat date_time_output_format = DateTimeOutputFormat::Simple;

    enum class IntervalOutputFormat : uint8_t
    {
        Kusto,
        Numeric
    };

    struct
    {
        IntervalOutputFormat output_format = IntervalOutputFormat::Numeric;
    } interval{};

    enum class DateTimeOverflowBehavior : uint8_t
    {
        Ignore,
        Throw,
        Saturate
    };

    DateTimeOverflowBehavior date_time_overflow_behavior = DateTimeOverflowBehavior::Ignore;

    bool input_format_ipv4_default_on_conversion_error = false;
    bool input_format_ipv6_default_on_conversion_error = false;

    UInt64 input_allow_errors_num = 0;
    Float32 input_allow_errors_ratio = 0;

    UInt64 client_protocol_version = 0;

    UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;

    size_t max_threads = 1;

    enum class ArrowCompression : uint8_t
    {
        NONE,
        LZ4_FRAME,
        ZSTD
    };

    struct
    {
        UInt64 max_binary_string_size = 1_GiB;
        UInt64 max_binary_array_size = 1_GiB;
        bool encode_types_in_binary_format = false;
        bool decode_types_in_binary_format = false;
    } binary{};

    struct
    {
        UInt64 row_group_size = 1000000;
        bool low_cardinality_as_dictionary = false;
        bool use_signed_indexes_for_dictionary = false;
        bool use_64_bit_indexes_for_dictionary = false;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
        bool output_string_as_string = false;
        bool output_fixed_string_as_fixed_byte_array = true;
        ArrowCompression output_compression_method = ArrowCompression::NONE;
    } arrow{};

    struct
    {
        String schema_registry_url;
        String output_codec;
        UInt64 output_sync_interval = 16 * 1024;
        bool allow_missing_fields = false;
        String string_column_pattern;
        UInt64 output_rows_in_file = 1;
    } avro{};

    String bool_true_representation = "true";
    String bool_false_representation = "false";

    struct CSV
    {
        char delimiter = ',';
        bool allow_single_quotes = true;
        bool allow_double_quotes = true;
        bool serialize_tuple_into_separate_columns = true;
        bool deserialize_separate_columns_into_tuple = true;
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        bool allow_cr_end_of_line = false;
        bool enum_as_number = false;
        bool arrays_as_nested_csv = false;
        String null_representation = "\\N";
        char tuple_delimiter = ',';
        bool use_best_effort_in_schema_inference = true;
        UInt64 skip_first_lines = 0;
        String custom_delimiter;
        bool try_detect_header = true;
        bool skip_trailing_empty_lines = false;
        bool trim_whitespaces = true;
        bool allow_whitespace_or_tab_as_delimiter = false;
        bool allow_variable_number_of_columns = false;
        bool use_default_on_bad_values = false;
        bool try_infer_numbers_from_strings = true;
        bool try_infer_strings_from_quoted_tuples = true;
    } csv{};

    struct HiveText
    {
        char fields_delimiter = '\x01';
        char collection_items_delimiter = '\x02';
        char map_keys_delimiter = '\x03';
        bool allow_variable_number_of_columns = true;
        Names input_field_names;
    } hive_text{};

    struct Custom
    {
        std::string result_before_delimiter;
        std::string result_after_delimiter;
        std::string row_before_delimiter;
        std::string row_after_delimiter;
        std::string row_between_delimiter;
        std::string field_delimiter;
        EscapingRule escaping_rule = EscapingRule::Escaped;
        bool try_detect_header = true;
        bool skip_trailing_empty_lines = false;
        bool allow_variable_number_of_columns = false;
    } custom{};

    struct JSON
    {
        size_t max_depth = 1000;
        bool array_of_rows = false;
        bool quote_64bit_integers = true;
        bool quote_64bit_floats = false;
        bool quote_denormals = true;
        bool quote_decimals = false;
        bool escape_forward_slashes = true;
        bool read_named_tuples_as_objects = false;
        bool use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = false;
        bool write_named_tuples_as_objects = false;
        bool skip_null_value_in_named_tuples = false;
        bool defaults_for_missing_elements_in_named_tuple = false;
        bool ignore_unknown_keys_in_named_tuple = false;
        bool serialize_as_strings = false;
        bool read_bools_as_numbers = true;
        bool read_bools_as_strings = true;
        bool read_numbers_as_strings = true;
        bool read_objects_as_strings = true;
        bool read_arrays_as_strings = true;
        bool try_infer_numbers_from_strings = false;
        bool validate_types_from_metadata = true;
        bool validate_utf8 = false;
        bool allow_deprecated_object_type = false;
        bool allow_json_type = false;
        bool valid_output_on_exception = false;
        bool compact_allow_variable_number_of_columns = false;
        bool try_infer_objects_as_tuples = false;
        bool infer_incomplete_types_as_strings = true;
        bool throw_on_bad_escape_sequence = true;
        bool ignore_unnecessary_fields = true;
        bool empty_as_default = false;
        bool type_json_skip_duplicated_paths = false;
    } json{};

    struct
    {
        String column_for_object_name{};
    } json_object_each_row{};

    enum class ParquetVersion : uint8_t
    {
        V1_0,
        V2_4,
        V2_6,
        V2_LATEST,
    };

    enum class ParquetCompression : uint8_t
    {
        NONE,
        SNAPPY,
        ZSTD,
        LZ4,
        GZIP,
        BROTLI,
    };

    struct
    {
        UInt64 row_group_rows = 1000000;
        UInt64 row_group_bytes = 512 * 1024 * 1024;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
        bool filter_push_down = true;
        bool use_native_reader = false;
        std::unordered_set<int> skip_row_groups = {};
        bool output_string_as_string = false;
        bool output_fixed_string_as_fixed_byte_array = true;
        bool preserve_order = false;
        bool use_custom_encoder = true;
        bool parallel_encoding = true;
        UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
        size_t prefer_block_bytes = DEFAULT_BLOCK_SIZE * 256;
        ParquetVersion output_version;
        ParquetCompression output_compression_method = ParquetCompression::SNAPPY;
        bool output_compliant_nested_types = true;
        size_t data_page_size = 1024 * 1024;
        size_t write_batch_size = 1024;
        bool write_page_index = false;
        size_t local_read_min_bytes_for_seek = 8192;
    } parquet{};

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        UInt64 max_value_width = 10000;
        UInt64 max_value_width_apply_for_single_value = false;
        bool highlight_digit_groups = true;
        /// Set to 2 for auto
        UInt64 color = 2;

        bool output_format_pretty_row_numbers = false;
        UInt64 output_format_pretty_single_large_number_tip_threshold = 1'000'000;
        UInt64 output_format_pretty_display_footer_column_names = 1;
        UInt64 output_format_pretty_display_footer_column_names_min_rows = 50;

        enum class Charset : uint8_t
        {
            UTF8,
            ASCII,
        };

        Charset charset = Charset::UTF8;
    } pretty{};

    struct
    {
        bool input_flatten_google_wrappers = false;
        bool output_nullables_with_google_wrappers = false;
        /**
         * Some buffers (kafka / rabbit) split the rows internally using callback,
         * and always send one row per message, so we can push there formats
         * without framing / delimiters (like ProtobufSingle). In other cases,
         * we have to enforce exporting at most one row in the format output,
         * because Protobuf without delimiters is not generally useful.
         */
        bool allow_multiple_rows_without_delimiter = false;
        bool skip_fields_with_unsupported_types_in_schema_inference = false;
        bool use_autogenerated_schema = true;
        std::string google_protos_path;
    } protobuf{};

    struct
    {
        uint32_t client_capabilities = 0;
        size_t max_packet_size = 0;
        uint8_t * sequence_id = nullptr; /// Not null if it's MySQLWire output format used to handle MySQL protocol connections.
        /**
         * COM_QUERY uses Text ResultSet
         * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
         * COM_STMT_EXECUTE uses Binary Protocol ResultSet
         * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute_response.html
         * By default, use Text ResultSet.
         */
        bool binary_protocol = false;
    } mysql_wire{};

    struct
    {
        std::string regexp;
        EscapingRule escaping_rule = EscapingRule::Raw;
        bool skip_unmatched = false;
    } regexp{};

    struct
    {
        std::string format_schema;
        std::string format_schema_path;
        bool is_server = false;
        std::string output_format_schema;
    } schema{};

    struct
    {
        String resultset_format;
        String row_format;
        String row_between_delimiter;
        String row_format_template;
        String resultset_format_template;
    } template_settings{};

    struct
    {
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        String null_representation = "\\N";
        bool enum_as_number = false;
        bool use_best_effort_in_schema_inference = true;
        UInt64 skip_first_lines = 0;
        bool try_detect_header = true;
        bool skip_trailing_empty_lines = false;
        bool allow_variable_number_of_columns = false;
        bool crlf_end_of_line_input = false;
    } tsv{};

    struct
    {
        bool interpret_expressions = true;
        bool deduce_templates_of_expressions = true;
        bool accurate_types_of_literals = true;
        bool allow_data_after_semicolon = false;
        bool escape_quote_with_quote = false;
    } values{};

    enum class ORCCompression : uint8_t
    {
        NONE,
        LZ4,
        SNAPPY,
        ZSTD,
        ZLIB,
    };

    struct
    {
        bool allow_missing_columns = false;
        int64_t row_batch_size = 100'000;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
        std::unordered_set<int> skip_stripes = {};
        bool output_string_as_string = false;
        ORCCompression output_compression_method = ORCCompression::NONE;
        bool use_fast_decoder = true;
        bool filter_push_down = true;
        UInt64 output_row_index_stride = 10'000;
        String reader_time_zone_name = "GMT";
        double output_dictionary_key_size_threshold = 0.0;
    } orc{};

    /// For capnProto format we should determine how to
    /// compare ClickHouse Enum and Enum from schema.
    enum class CapnProtoEnumComparingMode : uint8_t
    {
        BY_NAMES, // Names in enums should be the same, values can be different.
        BY_NAMES_CASE_INSENSITIVE, // Case-insensitive name comparison.
        BY_VALUES, // Values should be the same, names can be different.
    };

    struct CapnProto
    {
        CapnProtoEnumComparingMode enum_comparing_mode = CapnProtoEnumComparingMode::BY_VALUES;
        bool skip_fields_with_unsupported_types_in_schema_inference = false;
        bool use_autogenerated_schema = true;
    } capn_proto{};

    enum class MsgPackUUIDRepresentation : uint8_t
    {
        STR, // Output UUID as a string of 36 characters.
        BIN, // Output UUID as 16-bytes binary.
        EXT, // Output UUID as ExtType = 2
    };

    struct
    {
        UInt64 number_of_columns = 0;
        MsgPackUUIDRepresentation output_uuid_representation = MsgPackUUIDRepresentation::EXT;
    } msgpack{};

    struct MySQLDump
    {
        String table_name;
        bool map_column_names = true;
    } mysql_dump{};

    struct
    {
        UInt64 max_batch_size = DEFAULT_BLOCK_SIZE;
        String table_name = "table";
        bool include_column_names = true;
        bool use_replace = false;
        bool quote_names = true;
    } sql_insert{};

    struct
    {
        bool output_string_as_string;
        bool skip_fields_with_unsupported_types_in_schema_inference;
    } bson{};

    struct
    {
        bool allow_types_conversion = true;
        bool encode_types_in_binary_format = false;
        bool decode_types_in_binary_format = false;
    } native{};

    struct
    {
        bool valid_output_on_exception = false;
    } xml{};

    struct
    {
        bool escape_special_characters = false;
    } markdown{};
};

}
