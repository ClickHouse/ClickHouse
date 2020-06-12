#pragma once

#include "FunctionsURL.h"
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

/// Extracts scheme from given url.
inline StringRef getURLScheme(const char * data, size_t size)
{
    // scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    const char * pos = data;
    const char * end = data + size;

    if (isAlphaASCII(*pos))
    {
        for (++pos; pos < end; ++pos)
        {
            if (!(isAlphaNumericASCII(*pos) || *pos == '+' || *pos == '-' || *pos == '.'))
            {
                break;
            }
        }

        return StringRef(data, pos - data);
    }

    return {};
}

struct ExtractProtocol
{
    static size_t getReserveLengthForElement()
    {
        return strlen("https") + 1;
    }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        StringRef scheme = getURLScheme(data, size);
        Pos pos = data + scheme.size;

        if (scheme.size == 0 || (data + size) - pos < 4)
            return;

        if (pos[0] == ':')
            res_size = pos - data;
    }
};

}

