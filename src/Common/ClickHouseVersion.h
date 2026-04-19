#pragma once

#include <compare>
#include <string>
#include <vector>
#include <string_view>

namespace DB
{

class ClickHouseVersion
{
public:
    explicit ClickHouseVersion(std::string_view version);

    std::string toString() const;

    std::strong_ordering operator<=>(const ClickHouseVersion & other) const;

private:
    std::vector<size_t> components;
    /// Non-numeric suffix (e.g. "altinityantalya" for "26.1.3.20001.altinityantalya"); empty for standard versions
    std::string suffix;
};

}
