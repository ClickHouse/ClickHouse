#include <Common/hex.h>
#include <Common/StringUtils/StringUtils.h>
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
        unsigned char c = *pos;

        if (isWordCharASCII(c))
            res += c;
        else
        {
            res += '%';
            res += hexDigitUppercase(c / 16);
            res += hexDigitUppercase(c % 16);
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
        if (!(*pos == '%' && pos + 2 < end))
        {
            res += *pos;
            ++pos;
        }
        else
        {
            ++pos;
            res += unhex2(pos);
            pos += 2;
        }
    }
    return res;
}

}
