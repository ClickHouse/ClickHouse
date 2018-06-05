#pragma once

namespace DB
{

struct FormatSettings
{
    struct JSON
    {
        bool quote_64bit_integers = true;
        bool quote_denormals = true;
    };

    JSON json;

    struct CSV
    {
        char delimiter = ',';
    };

    CSV csv;

    enum class DateTimeInputFormat
    {
        Basic,      /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort  /// Use sophisticated rules to parse whatever possible.
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;
};

}

