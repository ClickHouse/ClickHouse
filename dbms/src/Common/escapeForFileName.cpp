#include <Common/hex.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>

namespace DB
{

std::string escapeForFileName(const std::string & s)
{
    std::string res;
    const char * pos = s.data();
    const char * end = pos + s.size();

    while (pos != end)
    {
        char c = *pos;

        if (isWordCharASCII(c))
            res += c;
        else
        {
            res += '%';
            res += hexUppercase(c / 16);
            res += hexUppercase(c % 16);
        }

        ++pos;
    }

    return res;
}

std::string unescapeForFileName(const std::string & s)
{
    std::string res;
    const char * pos = s.data();
    const char * end = pos + s.size();

    while (pos != end)
    {
        if (*pos != '%')
            res += *pos;
        else
        {
            /// skip '%'
            if (++pos == end) break;

            char val = unhex(*pos) * 16;

            if (++pos == end) break;

            val += unhex(*pos);

            res += val;
        }

        ++pos;
    }
    return res;
}

}
