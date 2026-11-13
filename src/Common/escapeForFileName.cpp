#include <Common/Hex.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>

namespace DB
{

std::string escapeForFileName(std::string_view s)
{
    const char * pos = s.data();
    const char * end = pos + s.size();

    /// Column and substream names are mostly plain identifiers, and the readers and writers of parts escape them per
    /// substream of every granule, so a name with nothing to escape is copied whole instead of character by character.
    while (pos != end && isWordCharASCII(static_cast<unsigned char>(*pos)))
        ++pos;
    if (pos == end)
        return s;

    std::string res;
    res.reserve(s.size() + 2);
    res.append(s.data(), pos);

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
