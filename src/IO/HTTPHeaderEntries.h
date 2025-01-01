#pragma once
#include <string>

namespace DB
{

struct HTTPHeaderEntry
{
    std::string name;
    std::string value;

    HTTPHeaderEntry(const std::string & name_, const std::string & value_) : name(name_), value(value_) {}
    bool operator==(const HTTPHeaderEntry & other) const { return name == other.name && value == other.value; }
};

using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>;

}
