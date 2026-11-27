#pragma once

#include <Formats/FormatSettings.h>

namespace DB
{

/// Settings for writing values as JSON text that is parsed back right away: numbers are written unquoted
/// so they are read back as numbers, and inf and nan, which JSON has no number for, are quoted instead of
/// being written as null.
inline FormatSettings getJSONRoundTripFormatSettings(const FormatSettings & format_settings)
{
    FormatSettings result = format_settings;
    result.json.quote_64bit_integers = false;
    result.json.quote_denormals = true;
    /// An internal round trip must be lossless and independent of the user's presentation settings, which
    /// would otherwise change the stored value: shortest float text and canonical Boolean tokens.
    result.float_precision = 0;
    result.bool_true_representation = "true";
    result.bool_false_representation = "false";
    /// The text holds stored values, not user input, so a date/time is read back in its type's zone.
    result.session_time_zone = nullptr;
    return result;
}

}
