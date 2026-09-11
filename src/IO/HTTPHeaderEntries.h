#pragma once

#include <string>
#include <vector>

#include <base/types.h>
#include <Common/SensitiveString.h>

namespace DB
{

struct HTTPHeaderEntry
{
    String name;
    SensitiveString value;

    HTTPHeaderEntry(std::string_view name_, std::string_view value_) : name(name_), value(value_) {}
    bool operator==(const HTTPHeaderEntry & other) const = default;
};

using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

}
