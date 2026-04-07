#pragma once

#include <base/types.h>

namespace DB
{

/// Counts the number of literal characters in Joda format string until the next closing literal
/// sequence single quote. Returns -1 if no literal single quote was found.
/// In Joda format string(https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)
/// literal content must be quoted with single quote. and two single quote means literal with one single quote.
/// For example:
/// Format string: "'aaaa'", unescaped literal: "aaaa";
/// Format string: "'aa''aa'", unescaped literal: "aa'aa";
/// Format string: "'aaa''aa" is not valid because of missing of end single quote.
inline Int64 numLiteralChars(const char * cur, const char * end)
{
    bool found = false;
    Int64 count = 0;
    while (cur < end)
    {
        if (*cur == '\'')
        {
            if (cur + 1 < end && *(cur + 1) == '\'')
            {
                count += 2;
                cur += 2;
            }
            else
            {
                found = true;
                break;
            }
        }
        else
        {
            ++count;
            ++cur;
        }
    }
    return found ? count : -1;
}

}
