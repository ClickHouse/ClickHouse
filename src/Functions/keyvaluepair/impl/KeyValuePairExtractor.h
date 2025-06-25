#pragma once

#include <Columns/ColumnString.h>

#include <string>
#include <string_view>

namespace DB
{

struct KeyValuePairExtractor
{
    virtual ~KeyValuePairExtractor() = default;

    virtual uint64_t extract(const std::string & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) = 0;

    virtual uint64_t extract(std::string_view data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values) = 0;
};

}
