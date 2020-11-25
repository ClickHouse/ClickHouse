#pragma once

#include <common/types.h>


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
    bool write_statistics = true;
    bool import_nested_json = false;
    bool null_as_default = false;

    enum class DateTimeInputFormat
    {
        Basic,      /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort  /// Use sophisticated rules to parse whatever possible.
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;

    enum class DateTimeOutputFormat
    {
        Simple,
        ISO,
        UnixTimestamp
    };

    DateTimeOutputFormat date_time_output_format = DateTimeOutputFormat::Simple;

    UInt64 input_allow_errors_num = 0;
    Float32 input_allow_errors_ratio = 0;

    struct
    {
        UInt64 row_group_size = 1000000;
    } arrow;

    struct
    {
        String schema_registry_url;
        String output_codec;
        UInt64 output_sync_interval = 16 * 1024;
        bool allow_missing_fields = false;
    } avro;

    struct CSV
    {
        char delimiter = ',';
        bool allow_single_quotes = true;
        bool allow_double_quotes = true;
        bool unquoted_null_literal_as_null = false;
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        bool input_format_enum_as_number = false;
    } csv;

    struct Custom
    {
        std::string result_before_delimiter;
        std::string result_after_delimiter;
        std::string row_before_delimiter;
        std::string row_after_delimiter;
        std::string row_between_delimiter;
        std::string field_delimiter;
        std::string escaping_rule;
    } custom;

    struct
    {
        bool array_of_rows = false;
        bool quote_64bit_integers = true;
        bool quote_denormals = true;
        bool escape_forward_slashes = true;
        bool named_tuples_as_objects = false;
        bool serialize_as_strings = false;
    } json;

    struct
    {
        UInt64 row_group_size = 1000000;
    } parquet;

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        UInt64 max_value_width = 10000;
        bool color = true;

        bool output_format_pretty_row_numbers = false;

        enum class Charset
        {
            UTF8,
            ASCII,
        };

        Charset charset = Charset::UTF8;
    } pretty;

    struct
    {
        bool write_row_delimiters = true;
        /**
         * Some buffers (kafka / rabbit) split the rows internally using callback,
         * and always send one row per message, so we can push there formats
         * without framing / delimiters (like ProtobufSingle). In other cases,
         * we have to enforce exporting at most one row in the format output,
         * because Protobuf without delimiters is not generally useful.
         */
        bool allow_many_rows_no_delimiters = false;
    } protobuf;

    struct
    {
        std::string regexp;
        std::string escaping_rule;
        bool skip_unmatched = false;
    } regexp;

    struct
    {
        std::string format_schema;
        std::string format_schema_path;
        bool is_server = false;
    } schema;

    struct
    {
        String resultset_format;
        String row_format;
        String row_between_delimiter;
    } template_settings;

    struct
    {
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        String null_representation = "\\N";
        bool input_format_enum_as_number = false;
    } tsv;

    struct
    {
        bool interpret_expressions = true;
        bool deduce_templates_of_expressions = true;
        bool accurate_types_of_literals = true;
    } values;
};

}

