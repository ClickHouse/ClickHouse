#pragma once

namespace DB
{

struct FormatSettingsJSON
{
    bool force_quoting_64bit_integers = true;
    bool output_format_json_quote_denormals = true;

    FormatSettingsJSON() = default;

    FormatSettingsJSON(bool force_quoting_64bit_integers_, bool output_format_json_quote_denormals_)
        : force_quoting_64bit_integers(force_quoting_64bit_integers_), output_format_json_quote_denormals(output_format_json_quote_denormals_) {}
};

}
