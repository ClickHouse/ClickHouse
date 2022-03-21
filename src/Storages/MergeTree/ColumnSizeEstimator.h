#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>


namespace DB
{

/* Allow to compute more accurate progress statistics */
class ColumnSizeEstimator
{
    using ColumnToSize = MergeTreeDataPartInMemory::ColumnToSize;
    ColumnToSize map;
public:

    /// Stores approximate size of columns in bytes
    /// Exact values are not required since it used for relative values estimation (progress).
    size_t sum_total = 0;
    size_t sum_index_columns = 0;
    size_t sum_ordinary_columns = 0;

    ColumnSizeEstimator(ColumnToSize && map_, const Names & key_columns, const Names & ordinary_columns)
        : map(std::move(map_))
    {
        for (const auto & name : key_columns)
            if (!map.count(name)) map[name] = 0;
        for (const auto & name : ordinary_columns)
            if (!map.count(name)) map[name] = 0;

        for (const auto & name : key_columns)
            sum_index_columns += map.at(name);

        for (const auto & name : ordinary_columns)
            sum_ordinary_columns += map.at(name);

        sum_total = std::max(static_cast<decltype(sum_index_columns)>(1), sum_index_columns + sum_ordinary_columns);
    }

    Float64 columnWeight(const String & column) const
    {
        return static_cast<Float64>(map.at(column)) / sum_total;
    }

    Float64 keyColumnsWeight() const
    {
        return static_cast<Float64>(sum_index_columns) / sum_total;
    }
};
}
