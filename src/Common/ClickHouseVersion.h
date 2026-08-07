#pragma once

#include <compare>
#include <vector>
#include <string_view>

namespace DB
{

class ClickHouseVersion
{
public:
    explicit ClickHouseVersion(std::string_view version);

    std::string toString() const;

    std::strong_ordering operator<=>(const ClickHouseVersion & other) const = default;

private:
    std::vector<size_t> components;
};

}
