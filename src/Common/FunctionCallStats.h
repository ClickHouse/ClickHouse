#pragma once

#include <base/types.h>
#include <unordered_map>

namespace DB
{

struct FunctionCallStats
{
    struct Stats
    {
        UInt64 blocks = 0;
        UInt64 rows = 0;
        UInt64 bytes = 0;
    };

    std::unordered_map<std::string, Stats> data;

    void increment(const std::string & function_name, UInt64 num_rows, UInt64 num_bytes)
    {
        auto & stats = data[function_name];
        ++stats.blocks;
        stats.rows += num_rows;
        stats.bytes += num_bytes;
    }

    bool empty() const { return data.empty(); }

    void merge(const FunctionCallStats & other)
    {
        for (const auto & [name, stats] : other.data)
        {
            auto & my_stats = data[name];
            my_stats.blocks += stats.blocks;
            my_stats.rows += stats.rows;
            my_stats.bytes += stats.bytes;
        }
    }
};

}
