#pragma once

#include <string>
#include <string_view>


namespace DB
{

/// POSIX-compliant single-quote escaping: wrap in single quotes and replace any
/// embedded single quote with '\''. Safe against arbitrary byte content.
inline std::string shellQuote(std::string_view s)
{
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('\'');
    for (char c : s)
    {
        if (c == '\'')
            out.append("'\\''");
        else
            out.push_back(c);
    }
    out.push_back('\'');
    return out;
}

}
