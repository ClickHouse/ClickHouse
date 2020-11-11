#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/ArenaAllocator.h>

#include <numeric>
#include <algorithm>

namespace DB 
{

template <typename Values, typename Ranks>
void computeRanks(const Values & values, Ranks & out) {
    std::vector<size_t> indexes(values.size());
    std::iota(indexes.begin(), indexes.end(), 0);
    std::sort(indexes.begin(), indexes.end(),
                [&] (size_t lhs, size_t rhs) { return values[lhs] < values[rhs]; });

    size_t left = 0;
    while (left < indexes.size()) {
        size_t right = left;
        while (right < indexes.size() && values[indexes[left]] == values[indexes[right]]) {
            ++right;
        }
        auto adjusted = (left + right + 1.) / 2.;
        for (size_t iter = left; iter < right; ++iter) {
            out[indexes[iter]] = adjusted;
        }
        left = right;
    }
}


template <typename T = double>
struct StatisticalSampleData
{
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using SampleArray = PODArray<T, 32, Allocator>;

    SampleArray first;
    SampleArray second;

    void add(T x, T y)
    {
        first.push_back(x);
        second.push_back(y);
    }

    void merge(const StatisticalSampleData & rhs)
    {
        first.insert(first.end(), rhs.first.begin(), rhs.first.end());
        second.insert(second.end(), rhs.second.begin(), rhs.second.end());
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf) const 
    {
        readPODBinary(*this, buf);
    }
};

}

