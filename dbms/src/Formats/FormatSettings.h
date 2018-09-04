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
    };

    CSV csv;

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        bool color = true;
    };

    Pretty pretty;

    struct Values
    {
        bool interpret_expressions = true;
    };

    Values values;

    bool skip_unknown_fields = false;
    bool write_statistics = true;

    enum class DateTimeInputFormat
    {
        Basic,      /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort  /// Use sophisticated rules to parse whatever possible.
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;

    UInt64 input_allow_errors_num = 0;
    Float64 input_allow_errors_ratio = 0;
};

}

