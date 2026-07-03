#pragma once

#include <base/types.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <span>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

/// How many steps if we want to change lhs to rhs.
/// Details in https://en.wikipedia.org/wiki/Levenshtein_distance
size_t levenshteinDistanceCaseInsensitive(const String & lhs, const String & rhs);

/// The same function as DB::levenshteinDistanceCaseInsensitive, but case sensitive for a more generic approach
template <class Element = char>
size_t levenshteinDistance(std::span<const Element> lhs, std::span<const Element> rhs)
{
    const size_t m = lhs.size();
    const size_t n = rhs.size();
    if (m==0 || n==0)
    {
        return m + n;
    }
    PODArray<size_t> v0(n + 1);

    iota(v0.data() + 1, n, size_t(1));

    for (size_t j = 1; j <= m; ++j)
    {
        v0[0] = j;
        size_t prev = j - 1;
        for (size_t i = 1; i <= n; ++i)
        {
            size_t old = v0[i];
            v0[i] = std::min(prev + (lhs[j - 1] != rhs[i - 1]),
                    std::min(v0[i - 1], v0[i]) + 1);
            prev = old;
        }
    }
    return v0[n];
};

/// Levenshtein Distance with weights, is used to calculate custom distance from lhs to rhs
template <class Element, class Weight>
Weight levenshteinDistanceWeighted(std::span<const Element> lhs, std::span<const Element> rhs,
                                   std::span<const Weight> lhs_weights, std::span<const Weight> rhs_weights)
{
    if (lhs.size() != lhs_weights.size() || rhs.size() != rhs_weights.size())
        throw Exception(
            ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
            "Sizes of arrays don't match: {} != {} or {} != {}",
            lhs.size(),
            lhs_weights.size(),
            rhs.size(),
            rhs_weights.size());

    auto sum_span = [](const std::span<const Weight> & span) -> Weight
    {
        return std::accumulate(span.begin(), span.end(), Weight{});
    };
    if (lhs.empty() || rhs.empty())
    {
        return sum_span(lhs_weights) + sum_span(rhs_weights);
    }

    // Consume minimum memory by allocating sliding vector for min `m`
    if (lhs.size() > rhs.size())
    {
        std::swap(lhs, rhs);
        std::swap(lhs_weights, rhs_weights);
    }

    const size_t m = lhs.size();
    const size_t n = rhs.size();

    PODArray<Weight> row(m + 1);

    row[0] = 0;
    std::partial_sum(lhs_weights.begin(), lhs_weights.end(), row.begin() + 1);

    for (size_t j = 1; j <= n; ++j)
    {
        Weight prev = row[0];
        row[0] += rhs_weights[j - 1];
        for (size_t i = 1; i <= m; ++i)
        {
            if (lhs[i - 1] == rhs[j - 1])
            {
                row[i] = prev;
                continue;
            }

            prev = row[i];

            row[i] = std::min({row[i] + rhs_weights[j - 1], // deletion
                               row[i - 1] + lhs_weights[i - 1], // insertion
                               row[i - 1] + lhs_weights[i - 1] + rhs_weights[j - 1]}); // substitusion
        }
    }

    return row[m];
};

}
