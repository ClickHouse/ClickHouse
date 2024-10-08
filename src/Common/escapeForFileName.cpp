#include <base/hex.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <fmt/format.h>

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

const std::string & escapeForLogs(const std::string & in, size_t max_length, std::string & buffer)
{
    const char * const begin = in.data();

    if (max_length == 0)
        max_length = in.size();
    else
        max_length = std::min(max_length, in.size());

    size_t truncated_length = in.size() - max_length;

    const char * const end = begin + max_length;

    const char * pos = begin;
    for (; pos != end; ++pos)
    {
        if (!isWordCharASCII(*pos))
            break;
    }

    if (pos == in.data() + in.size())
        return in;

    if (pos == end)
    {
        buffer = in.substr(0, max_length);
        if (truncated_length)
            buffer.append(fmt::format("... ({} more bytes)", truncated_length));
        return buffer;
    }

    buffer.clear();
    buffer.reserve(max_length * 3);
    buffer.append(begin, pos);

    for (; pos != end; ++pos)
    {
        unsigned char c = *pos;
        if (c == '\\')
        {
            buffer.push_back('\\');
            buffer.push_back('\\');
        }
        else if (isWordCharASCII(c))
        {
            buffer.push_back(c);
        }
        else
        {
            buffer.push_back('\\');
            buffer.push_back('x');
            buffer.push_back(hexDigitUppercase(c / 16));
            buffer.push_back(hexDigitUppercase(c % 16));
        }
    }

    if (truncated_length)
        buffer.append(fmt::format("... ({} more bytes)", truncated_length));

    return buffer;
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
