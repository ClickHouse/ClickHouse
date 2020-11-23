#pragma once

#include <Core/Types.h>


namespace DB
{

/** Various tweaks for input/output formats.
  * Text serialization/deserialization of data types also depend on some of these settings.
  * NOTE Parameters for unrelated formats and unrelated data types
  *  are collected in this struct - it prevents modularity, but they are difficult to separate.
  */
struct FormatSettings
{
    /// Format will be used for streaming. Not every formats support it
    /// Option means that each chunk of data need to be formatted independently. Also each chunk will be flushed at the end of processing.
    bool enable_streaming = false;

    struct JSON
    {
        bool quote_64bit_integers = true;
        bool quote_denormals = true;
        bool escape_forward_slashes = true;
    };

    JSON json;

    struct CSV
    {
        char delimiter = ',';
        bool allow_single_quotes = true;
        bool allow_double_quotes = true;
        bool unquoted_null_literal_as_null = false;
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
    };

    CSV csv;

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        UInt64 max_value_width = 10000;
        bool color = true;

        enum class Charset
        {
            UTF8,
            ASCII,
        };

        Charset charset = Charset::UTF8;
    };

    Pretty pretty;

    struct Values
    {
        bool interpret_expressions = true;
        bool deduce_templates_of_expressions = true;
        bool accurate_types_of_literals = true;
    };

    Values values;

    struct Template
    {
        String resultset_format;
        String row_format;
        String row_between_delimiter;
    };

    Template template_settings;

    struct TSV
    {
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
    };

    TSV tsv;

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

    UInt64 input_allow_errors_num = 0;
    Float32 input_allow_errors_ratio = 0;

    struct Arrow
    {
        UInt64 row_group_size = 1000000;
    } arrow;

    struct Parquet
    {
        UInt64 row_group_size = 1000000;
    } parquet;

    struct Schema
    {
        std::string format_schema;
        std::string format_schema_path;
        bool is_server = false;
    };

    Schema schema;

    struct Custom
    {
        std::string result_before_delimiter;
        std::string result_after_delimiter;
        std::string row_before_delimiter;
        std::string row_after_delimiter;
        std::string row_between_delimiter;
        std::string field_delimiter;
        std::string escaping_rule;
    };

    Custom custom;

    struct Avro
    {
        String schema_registry_url;
        String output_codec;
        UInt64 output_sync_interval = 16 * 1024;
        bool allow_missing_fields = false;
    };

    Avro avro;

    struct Regexp
    {
        std::string regexp;
        std::string escaping_rule;
        bool skip_unmatched = false;
    };

    Regexp regexp;

};

}

