#pragma once

#include <string>
#include <vector>

namespace DB
{

struct HTTPHeaderEntry
{
    std::string name;
    std::string value;

    HTTPHeaderEntry(const std::string & name_, const std::string & value_) : name(name_), value(value_) {}
    bool operator==(const HTTPHeaderEntry & other) const { return name == other.name && value == other.value; }
};

using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Lower-case every header name in place, for code that classifies a name by a literal prefix.
/// `HTTPHeaderFilter` needs the original case for `(?-i)` regexps, so this is not done on construction.
void normalizeHeaderNames(HTTPHeaderEntries & headers);

}
