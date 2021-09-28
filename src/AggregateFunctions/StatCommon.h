#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/ArenaAllocator.h>

#include <numeric>
#include <algorithm>
#include <utility>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename F>
static Float64 integrateSimpson(Float64 a, Float64 b, F && func)
{
    const size_t iterations = std::max(1e6, 1e4 * std::abs(std::round(b) - std::round(a)));
    const long double h = (b - a) / iterations;
    Float64 sum_odds = 0.0;
    for (size_t i = 1; i < iterations; i += 2)
        sum_odds += func(a + i * h);
    Float64 sum_evens = 0.0;
    for (size_t i = 2; i < iterations; i += 2)
        sum_evens += func(a + i * h);
    return (func(a) + func(b) + 2 * sum_evens + 4 * sum_odds) * h / 3;
}

/// Because ranks are adjusted, we have to store each of them in Float type.
using RanksArray = std::vector<Float64>;

template <typename Values>
std::pair<RanksArray, Float64> computeRanksAndTieCorrection(const Values & values)
{
    const size_t size = values.size();
    /// Save initial positions, than sort indices according to the values.
    std::vector<size_t> indexes(size);
    std::iota(indexes.begin(), indexes.end(), 0);
    std::sort(indexes.begin(), indexes.end(),
                [&] (size_t lhs, size_t rhs) { return values[lhs] < values[rhs]; });

    size_t left = 0;
    Float64 tie_numenator = 0;
    RanksArray out(size);
    while (left < size)
    {
        size_t right = left;
        while (right < size && values[indexes[left]] == values[indexes[right]])
            ++right;
        auto adjusted = (left + right + 1.) / 2.;
        auto count_equal = right - left;

        /// Scipy implementation throws exception in this case too.
        if (count_equal == size)
            throw Exception("All numbers in both samples are identical", ErrorCodes::BAD_ARGUMENTS);

        tie_numenator += std::pow(count_equal, 3) - count_equal;
        for (size_t iter = left; iter < right; ++iter)
            out[indexes[iter]] = adjusted;
        left = right;
    }
    return {out, 1 - (tie_numenator / (std::pow(size, 3) - size))};
}


template <typename X, typename Y>
struct StatisticalSample
{
    using AllocatorXSample = MixedAlignedArenaAllocator<alignof(X), 4096>;
    using SampleX = PODArray<X, 32, AllocatorXSample>;

    using AllocatorYSample = MixedAlignedArenaAllocator<alignof(Y), 4096>;
    using SampleY = PODArray<Y, 32, AllocatorYSample>;

    SampleX x{};
    SampleY y{};
    size_t size_x{0};
    size_t size_y{0};

    void addX(X value, Arena * arena)
    {
        ++size_x;
        x.push_back(value, arena);
    }

    void addY(Y value, Arena * arena)
    {
        ++size_y;
        y.push_back(value, arena);
    }

    void merge(const StatisticalSample & rhs, Arena * arena)
    {
        size_x += rhs.size_x;
        size_y += rhs.size_y;
        x.insert(rhs.x.begin(), rhs.x.end(), arena);
        y.insert(rhs.y.begin(), rhs.y.end(), arena);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size_x, buf);
        writeVarUInt(size_y, buf);
        buf.write(reinterpret_cast<const char *>(x.data()), size_x * sizeof(x[0]));
        buf.write(reinterpret_cast<const char *>(y.data()), size_y * sizeof(y[0]));
    }

    void read(ReadBuffer & buf, Arena * arena)
    {
        readVarUInt(size_x, buf);
        readVarUInt(size_y, buf);
        x.resize(size_x, arena);
        y.resize(size_y, arena);
        buf.read(reinterpret_cast<char *>(x.data()), size_x * sizeof(x[0]));
        buf.read(reinterpret_cast<char *>(y.data()), size_y * sizeof(y[0]));
    }
};

}

