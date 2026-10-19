#pragma once

#include <string>
#include <string_view>


namespace DB
{

/// `Poco::URI` rejects spaces, but they are common in object keys (e.g. `a b.csv`).
/// Percent-encode them as `%20`, like `curl` does. Other characters, including `%`, are kept as is.
inline std::string encodeSpacesInURI(std::string_view uri)
{
    std::string result;
    result.reserve(uri.size());
    for (char c : uri)
    {
        if (c == ' ')
            result += "%20";
        else
            result += c;
    }
    return result;
}

}
