#pragma once

#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>

namespace DB
{

/// Not in IO/WriteHelpers.h, because it needs the definition of FormatSettings.
template <typename T>
void writeJSONNumber(T x, WriteBuffer & ostr, const FormatSettings & settings)
{
    bool is_finite = isFinite(x);

    const bool need_quote = (is_integer<T> && (sizeof(T) >= 8) && settings.json.quote_64bit_integers)
        || (settings.json.quote_denormals && !is_finite) || (is_floating_point<T> && (sizeof(T) >= 8) && settings.json.quote_64bit_floats);

    if (need_quote)
        writeChar('"', ostr);

    if (is_finite)
    {
        if constexpr (is_floating_point<T>)
            writeFloatText(x, ostr, settings);
        else
            writeText(x, ostr);
    }
    else if (!settings.json.quote_denormals)
        writeCString("null", ostr);
    else
    {
        if constexpr (is_floating_point<T>)
        {
            if (signBit(x))
            {
                if (isNaN(x))
                    writeCString("-nan", ostr);
                else
                    writeCString("-inf", ostr);
            }
            else
            {
                if (isNaN(x))
                    writeCString("nan", ostr);
                else
                    writeCString("inf", ostr);
            }
        }
    }

    if (need_quote)
        writeChar('"', ostr);
}

}
