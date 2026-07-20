#pragma once

#include <base/types.h>
#include <base/extended_types.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <span>

#include <algorithm>
#include <type_traits>

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

/// Accumulator type for the weighted distance. Integral weights are summed in a wide integer twice as
/// wide as the weight (at least 256-bit), so any feasible array sum stays exact and cannot overflow:
/// overflowing would need more than 2^(weight bits) elements. 256-bit weights therefore accumulate in
/// 512 bits. Floating weights are summed in Float64. The result is converted to Float64 once at the end.
template <class Weight>
using LevenshteinWeightAccumulator = std::conditional_t<
    is_integer<Weight>,
    wide::integer<std::max<size_t>(256, sizeof(Weight) * 8 * 2), std::conditional_t<is_unsigned_v<Weight>, unsigned, signed>>,
    Float64>;

/// Levenshtein Distance with weights, is used to calculate custom distance from lhs to rhs.
/// The result is Float64 (the SQL functions return Float64). To avoid overflow/UB on weights near the
/// type maximum, the dynamic-programming sums use a wide internal accumulator (see above): integral
/// weights stay exact, then the final value is cast to Float64 only once.
template <class Element, class Weight>
Float64 levenshteinDistanceWeighted(std::span<const Element> lhs, std::span<const Element> rhs,
                                    std::span<const Weight> lhs_weights, std::span<const Weight> rhs_weights)
{
    using Acc = LevenshteinWeightAccumulator<Weight>;

    if (lhs.size() != lhs_weights.size() || rhs.size() != rhs_weights.size())
        throw Exception(
            ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
            "Sizes of arrays don't match: {} != {} or {} != {}",
            lhs.size(),
            lhs_weights.size(),
            rhs.size(),
            rhs_weights.size());

    auto sum_span = [](const std::span<const Weight> & span) -> Acc
    {
        Acc sum = 0;
        for (const auto & weight : span)
            sum += static_cast<Acc>(weight);
        return sum;
    };
    if (lhs.empty() || rhs.empty())
    {
        return static_cast<Float64>(sum_span(lhs_weights) + sum_span(rhs_weights));
    }

    // Consume minimum memory by allocating sliding vector for min `m`
    if (lhs.size() > rhs.size())
    {
        std::swap(lhs, rhs);
        std::swap(lhs_weights, rhs_weights);
    }

    const size_t m = lhs.size();
    const size_t n = rhs.size();

    PODArray<Acc> row(m + 1);

    row[0] = 0;
    for (size_t i = 1; i <= m; ++i)
        row[i] = row[i - 1] + static_cast<Acc>(lhs_weights[i - 1]);

    for (size_t j = 1; j <= n; ++j)
    {
        Acc prev = row[0];
        row[0] += static_cast<Acc>(rhs_weights[j - 1]);
        for (size_t i = 1; i <= m; ++i)
        {
            Acc old = row[i];
            if (lhs[i - 1] == rhs[j - 1])
            {
                row[i] = prev;
                prev = old;
                continue;
            }

            row[i] = std::min(
                {old + static_cast<Acc>(rhs_weights[j - 1]), // deletion
                 row[i - 1] + static_cast<Acc>(lhs_weights[i - 1]), // insertion
                 prev + static_cast<Acc>(lhs_weights[i - 1]) + static_cast<Acc>(rhs_weights[j - 1])}); // substitution
            prev = old;
        }
    }

    return static_cast<Float64>(row[m]);
};

}
