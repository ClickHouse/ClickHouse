#pragma once

#include <vector>
#include <string>

namespace DB
{

struct SortColumnDesc
{
    std::string column_name; /// The name of the column.
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.

    bool operator==(const SortColumnDesc & other) const
    {
        return column_name == other.column_name && direction == other.direction && nulls_direction == other.nulls_direction;
    }

    bool operator != (const SortColumnDesc & other) const
    {
        return !(*this == other);
    }
};

using SortDesc = std::vector<SortColumnDesc>;

struct SortProperty : std::vector<SortDesc>
{
    void add(const SortDescription & sort_description)
    {
        SortDesc sort_desc;
        for (const auto & sort_column_desc : sort_description)
        {
            sort_desc.push_back({sort_column_desc.column_name, sort_column_desc.direction, sort_column_desc.nulls_direction});
        }
        emplace_back(sort_desc);
    }
};

SortDesc commonPrefix(const SortDesc & lhs, const SortDesc & rhs)
{
    size_t i = 0;
    for (; i < std::min(lhs.size(), rhs.size()); ++i)
    {
        if (lhs[i] != rhs[i])
            break;
    }

    auto res = lhs;
    res.erase(res.begin() + i, res.end());
    return res;
}

}
