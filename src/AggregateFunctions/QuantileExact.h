#pragma once

#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/Decimal.h>
#include <base/extended_types.h>
#include <base/sort.h>
#include <base/types.h>

#include <array>
#include <bit>
#include <tuple>

#define QUANTILE_EXACT_MAX_ARRAY_SIZE 1'000'000'000


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/// Values whose order matches the order of an integer key of at most 64 bits, so order
/// statistics can be found with radix drill-down instead of comparison-based selection.
template <typename Value>
constexpr bool is_radix_selectable =
    (std::is_integral_v<Value> || is_decimal<Value>) && sizeof(NativeType<Value>) <= 8;

namespace QuantileExactImpl
{

/// A request for the element at the given rank, and which output slot receives it.
struct SelectTarget
{
    size_t rank;
    size_t out_index;
};

/// Order-preserving mapping of a value to an unsigned 64-bit key, and back.
template <typename Value>
constexpr UInt64 radixSignMask()
{
    using Unsigned = std::make_unsigned_t<NativeType<Value>>;
    return std::is_signed_v<NativeType<Value>> ? (Unsigned(1) << (sizeof(Unsigned) * 8 - 1)) : Unsigned(0);
}

template <typename Value>
UInt64 radixKey(const Value & v)
{
    using Unsigned = std::make_unsigned_t<NativeType<Value>>;
    if constexpr (is_decimal<Value>)
        return static_cast<Unsigned>(v.value) ^ radixSignMask<Value>();
    else
        return static_cast<Unsigned>(v) ^ radixSignMask<Value>();
}

template <typename Value>
Value radixKeyToValue(UInt64 k)
{
    using Unsigned = std::make_unsigned_t<NativeType<Value>>;
    return Value(static_cast<NativeType<Value>>(static_cast<Unsigned>(k ^ radixSignMask<Value>())));
}

constexpr size_t radix_buckets = 256;

/// MSD radix drill-down: finds the elements at the ranks in `targets` (sorted by rank).
/// Each level takes 2 linear scans (histogram, gather) regardless of the number of
/// targets, then recurses into only the buckets that contain targets. The bucket width
/// comes from a deterministic strided sample of the key range: interior buckets divide
/// the sampled range [base, sample_max] into 2^7 slices at least, while bucket 0 catches
/// keys below `base` and bucket 255 keys past the last slice (both simply recurse).
/// The sampled range never exceeds the real one, so at least two buckets are non-empty
/// whenever the keys differ, and every level strictly shrinks the candidate set.
/// Does not modify the input array.
template <typename Value>
void radixSelect(const Value * data, size_t size, SelectTarget * targets, size_t targets_count, Value * out, size_t * histogram)
{
    /// Small candidate sets are cheaper to sort than to histogram.
    if (size <= 256)
    {
        VectorWithMemoryTracking<Value> sorted(data, data + size);
        ::sort(sorted.begin(), sorted.end());
        for (size_t t = 0; t < targets_count; ++t)
            out[targets[t].out_index] = sorted[targets[t].rank];
        return;
    }

    UInt64 base = std::numeric_limits<UInt64>::max();
    UInt64 sample_max = 0;
    size_t stride = size / 64 + 1;
    for (size_t j = 0; j < size; j += stride)
    {
        UInt64 k = radixKey(data[j]);
        base = std::min(base, k);
        sample_max = std::max(sample_max, k);
    }
    size_t range_width = std::bit_width(sample_max - base);
    size_t shift = range_width > 7 ? range_width - 7 : 0;

    /// The clamp compares before adding 1 so that (k - base) >> shift == UInt64(-1) (possible
    /// at shift 0) saturates to the high overflow bucket instead of wrapping to bucket 0.
    auto bucket_of = [base, shift](UInt64 k)
    {
        if (k < base)
            return UInt64(0);
        UInt64 slice = (k - base) >> shift;
        return slice >= radix_buckets - 2 ? UInt64(radix_buckets - 1) : slice + 1;
    };

    std::fill(histogram, histogram + radix_buckets, 0);
    for (size_t j = 0; j < size; ++j)
        ++histogram[bucket_of(radixKey(data[j]))];

    /// Locate each target's bucket and rebase its rank within that bucket. Targets sharing
    /// a bucket form contiguous groups because targets are sorted by rank.
    VectorWithMemoryTracking<size_t> group_bucket;
    VectorWithMemoryTracking<size_t> group_first_target;
    {
        size_t cum = 0;
        size_t bucket = 0;
        for (size_t t = 0; t < targets_count; ++t)
        {
            while (cum + histogram[bucket] <= targets[t].rank)
                cum += histogram[bucket++];
            targets[t].rank -= cum;
            if (group_bucket.empty() || group_bucket.back() != bucket)
            {
                group_bucket.push_back(bucket);
                group_first_target.push_back(t);
            }
        }
        group_first_target.push_back(targets_count);
    }

    /// At shift 0 an interior bucket holds the single key base + bucket - 1, so those
    /// answers need no gathering; only the open-ended edge buckets recurse.
    size_t num_groups = group_bucket.size();
    /// The gathered buckets are the only allocation proportional to the data (a clustered
    /// distribution can put most of the array in one bucket), so track it with throwing.
    VectorWithMemoryTracking<VectorWithMemoryTracking<Value>> group_elements(num_groups);
    std::array<UInt16, radix_buckets> bucket_group{}; /// group index + 1, or 0 when the bucket needs no gathering
    bool any_gathers = false;
    for (size_t g = 0; g < num_groups; ++g)
    {
        size_t bucket = group_bucket[g];
        if (shift == 0 && bucket > 0 && bucket < radix_buckets - 1)
        {
            for (size_t t = group_first_target[g]; t < group_first_target[g + 1]; ++t)
                out[targets[t].out_index] = radixKeyToValue<Value>(base + bucket - 1);
        }
        else
        {
            group_elements[g].reserve(histogram[bucket]);
            bucket_group[bucket] = static_cast<UInt16>(g + 1);
            any_gathers = true;
        }
    }
    if (!any_gathers)
        return;

    /// One pass gathers the contents of every bucket that still needs drilling.
    for (size_t j = 0; j < size; ++j)
    {
        size_t bucket = bucket_of(radixKey(data[j]));
        if (size_t g = bucket_group[bucket])
            group_elements[g - 1].push_back(data[j]);
    }

    for (size_t g = 0; g < num_groups; ++g)
    {
        if (!bucket_group[group_bucket[g]])
            continue;
        radixSelect(group_elements[g].data(), group_elements[g].size(),
                    targets + group_first_target[g], group_first_target[g + 1] - group_first_target[g],
                    out, histogram);
        group_elements[g] = {};
    }
}

}

/// Writes to out[i] the element the array would have at positions[i] if it were sorted.
/// Positions may repeat and come in any order. May reorder the array.
/// For types whose order matches an integer key, large arrays use a radix drill-down whose
/// cost does not depend on the input order, on duplicates, or on the number of positions,
/// unlike chained nth_element; this also keeps timings stable across runs. Small arrays
/// keep the nth_element chain, where the histogram pass would dominate (states produced
/// under GROUP BY are often tiny).
template <typename Array, typename Value>
void selectAtPositions(Array & array, const size_t * positions, size_t count, Value * out)
{
    /// A stack buffer for the common counts, so that a small state costs no allocation.
    std::array<QuantileExactImpl::SelectTarget, 16> small_targets{};
    VectorWithMemoryTracking<QuantileExactImpl::SelectTarget> large_targets(count > small_targets.size() ? count : 0);
    QuantileExactImpl::SelectTarget * targets = count > small_targets.size() ? large_targets.data() : small_targets.data();
    for (size_t i = 0; i < count; ++i)
        targets[i] = {positions[i], i};
    ::sort(targets, targets + count, [](const auto & a, const auto & b) { return a.rank < b.rank; });

    if constexpr (is_radix_selectable<Value>)
    {
        /// Measured break-even vs the nth_element chain; states produced under GROUP BY
        /// are often tiny and must not pay for histograms.
        if (array.size() >= 4096)
        {
            std::array<size_t, QuantileExactImpl::radix_buckets> histogram{};
            QuantileExactImpl::radixSelect(array.data(), array.size(), targets, count, out, histogram.data());
            return;
        }
    }

    size_t prev_n = 0;
    for (size_t t = 0; t < count; ++t)
    {
        size_t n = targets[t].rank;
        if (t == 0 || n != prev_n)
            ::nth_element(array.begin() + prev_n, array.begin() + n, array.end());
        out[targets[t].out_index] = array[n];
        prev_n = n;
    }
}

/// Interpolation delta `b - a` as Float64. Integral Value subtracts in Int128 (the widest
/// integral Value is (U)Int64, so the difference is exact and cannot overflow) before casting;
/// floating Value subtracts in Float64.
template <typename Value>
static inline Float64 quantileExactInterpolationDelta(const Value & a, const Value & b)
{
    if constexpr (is_integer<Value>)
        return static_cast<Float64>(static_cast<Int128>(b) - static_cast<Int128>(a));
    else
        return static_cast<Float64>(b) - static_cast<Float64>(a);
}


template <typename Value, typename Derived>
struct QuantileExactBase
{
    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Value>);
    using Array = PODArrayWithStackMemory<Value, bytes_in_arena>;
    Array array;

    void add(const Value & x)
    {
        /// We must skip NaNs as they are not compatible with comparison sorting.
        if (!isNaN(x))
            array.push_back(x);
    }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method add with weight is not implemented for QuantileExact");
    }

    void merge(const QuantileExactBase & rhs) { array.insert(rhs.array.begin(), rhs.array.end()); }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(array.data()), size * sizeof(array[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        if (unlikely(size > QUANTILE_EXACT_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", QUANTILE_EXACT_MAX_ARRAY_SIZE);
        array.resize(size);
        buf.readStrict(reinterpret_cast<char *>(array.data()), size * sizeof(array[0]));
    }

    Value get(Float64 level)
    {
        auto derived = static_cast<Derived*>(this);
        return derived->getImpl(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        auto derived = static_cast<Derived*>(this);
        return derived->getManyImpl(levels, indices, size, result);
    }
};

/** Calculates quantile by collecting all values into array
  *  and applying n-th element (introselect) algorithm for the resulting array.
  *
  * It uses O(N) memory and it is very inefficient in case of high amount of identical values.
  * But it is very CPU efficient for not large datasets.
  */
template <typename Value>
struct QuantileExact : QuantileExactBase<Value, QuantileExact<Value>>
{
    using QuantileExactBase<Value, QuantileExact<Value>>::array;

    // Get the value of the `level` quantile. The level must be between 0 and 1.
    Value getImpl(Float64 level)
    {
        if (!array.empty())
        {
            size_t n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
            Value value{};
            selectAtPositions(array, &n, 1, &value);
            return value;
        }

        return std::numeric_limits<Value>::quiet_NaN();
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
            return;
        }

        VectorWithMemoryTracking<size_t> ns(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto level = levels[indices[i]];
            ns[i] = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
        }

        VectorWithMemoryTracking<Value> values(size);
        selectAtPositions(array, ns.data(), size, values.data());
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = values[i];
    }
};

/// QuantileExactExclusive is equivalent to Excel PERCENTILE.EXC, R-6, SAS-4, SciPy-(0,0)
template <typename Value>
/// There are no virtual-like functions. So we don't inherit from QuantileExactBase.
struct QuantileExactExclusive : public QuantileExact<Value>
{
    using QuantileExact<Value>::array;

    /// Get the value of the `level` quantile. The level must be between 0 and 1 excluding bounds.
    Float64 getFloat(Float64 level)
    {
        if (!array.empty())
        {
            if (level == 0. || level == 1.)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "QuantileExactExclusive cannot interpolate for the percentiles 1 and 0");

            Float64 h = level * static_cast<Float64>(array.size() + 1);
            auto n = static_cast<size_t>(h);

            if (n >= array.size())
                return static_cast<Float64>(*std::max_element(array.begin(), array.end()));
            if (n < 1)
                return static_cast<Float64>(*std::min_element(array.begin(), array.end()));

            size_t positions[2] = {n - 1, n};
            Value values[2];
            selectAtPositions(array, positions, 2, values);
            return static_cast<Float64>(values[0]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(values[0], values[1]);
        }

        return std::numeric_limits<Float64>::quiet_NaN();
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
            return;
        }

        /// Interpolating levels need the order statistics at positions n - 1 and n.
        VectorWithMemoryTracking<size_t> positions;
        VectorWithMemoryTracking<std::tuple<size_t, Float64, size_t>> interpolated; /// (i, h, n)
        for (size_t i = 0; i < size; ++i)
        {
            auto level = levels[indices[i]];
            if (level == 0. || level == 1.)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "QuantileExactExclusive cannot interpolate for the percentiles 1 and 0");

            Float64 h = level * static_cast<Float64>(array.size() + 1);
            auto n = static_cast<size_t>(h);

            if (n >= array.size())
                result[indices[i]] = static_cast<Float64>(*std::max_element(array.begin(), array.end()));
            else if (n < 1)
                result[indices[i]] = static_cast<Float64>(*std::min_element(array.begin(), array.end()));
            else
            {
                interpolated.emplace_back(i, h, n);
                positions.push_back(n - 1);
                positions.push_back(n);
            }
        }

        VectorWithMemoryTracking<Value> values(positions.size());
        if (!positions.empty())
            selectAtPositions(array, positions.data(), positions.size(), values.data());
        for (size_t j = 0; j < interpolated.size(); ++j)
        {
            const auto & [i, h, n] = interpolated[j];
            result[indices[i]] = static_cast<Float64>(values[2 * j]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(values[2 * j], values[2 * j + 1]);
        }
    }
};

/// QuantileExactInclusive is equivalent to Excel PERCENTILE and PERCENTILE.INC, R-7, SciPy-(1,1)
template <typename Value>
/// There are no virtual-like functions. So we don't inherit from QuantileExactBase.
struct QuantileExactInclusive : public QuantileExact<Value>
{
    using QuantileExact<Value>::array;

    /// Get the value of the `level` quantile. The level must be between 0 and 1 including bounds.
    Float64 getFloat(Float64 level)
    {
        if (!array.empty())
        {
            Float64 h = level * static_cast<Float64>(array.size() - 1) + 1;
            auto n = static_cast<size_t>(h);

            if (n >= array.size())
                return static_cast<Float64>(*std::max_element(array.begin(), array.end()));
            if (n < 1)
                return static_cast<Float64>(*std::min_element(array.begin(), array.end()));

            size_t positions[2] = {n - 1, n};
            Value values[2];
            selectAtPositions(array, positions, 2, values);
            return static_cast<Float64>(values[0]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(values[0], values[1]);
        }

        return std::numeric_limits<Float64>::quiet_NaN();
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
            return;
        }

        /// Interpolating levels need the order statistics at positions n - 1 and n.
        VectorWithMemoryTracking<size_t> positions;
        VectorWithMemoryTracking<std::tuple<size_t, Float64, size_t>> interpolated; /// (i, h, n)
        for (size_t i = 0; i < size; ++i)
        {
            auto level = levels[indices[i]];

            Float64 h = level * static_cast<Float64>(array.size() - 1) + 1;
            auto n = static_cast<size_t>(h);

            if (n >= array.size())
                result[indices[i]] = static_cast<Float64>(*std::max_element(array.begin(), array.end()));
            else if (n < 1)
                result[indices[i]] = static_cast<Float64>(*std::min_element(array.begin(), array.end()));
            else
            {
                interpolated.emplace_back(i, h, n);
                positions.push_back(n - 1);
                positions.push_back(n);
            }
        }

        VectorWithMemoryTracking<Value> values(positions.size());
        if (!positions.empty())
            selectAtPositions(array, positions.data(), positions.size(), values.data());
        for (size_t j = 0; j < interpolated.size(); ++j)
        {
            const auto & [i, h, n] = interpolated[j];
            result[indices[i]] = static_cast<Float64>(values[2 * j])
                + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(values[2 * j], values[2 * j + 1]);
        }
    }
};

// QuantileExactLow returns the low median of given data.
// Implementation is as per "medium_low" function from python:
// https://docs.python.org/3/library/statistics.html#statistics.median_low
template <typename Value>
struct QuantileExactLow : public QuantileExactBase<Value, QuantileExactLow<Value>>
{
    using QuantileExactBase<Value, QuantileExactLow<Value>>::array;

    Value getImpl(Float64 level)
    {
        if (!array.empty())
        {
            size_t n = 0;
            // if level is 0.5 then compute the "low" median of the sorted array
            // by the method of rounding.
            if (level == 0.5)
            {
                auto s = array.size();
                if (s % 2 == 1)
                {
                    n = static_cast<size_t>(floor(s / 2));
                }
                else
                {
                    n = static_cast<size_t>((floor(s / 2)) - 1);
                }
            }
            else
            {
                // else quantile is the nth index of the sorted array obtained by multiplying
                // level and size of array. Example if level = 0.1 and size of array is 10,
                // then return array[1].
                n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
            }
            Value value{};
            selectAtPositions(array, &n, 1, &value);
            return value;
        }
        return std::numeric_limits<Value>::quiet_NaN();
    }

    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
            return;
        }

        VectorWithMemoryTracking<size_t> ns(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto level = levels[indices[i]];
            size_t n = 0;
            // if level is 0.5 then compute the "low" median of the sorted array
            // by the method of rounding.
            if (level == 0.5)
            {
                auto s = array.size();
                if (s % 2 == 1)
                {
                    n = static_cast<size_t>(floor(s / 2));
                }
                else
                {
                    n = static_cast<size_t>(floor((s / 2) - 1));
                }
            }
            else
            {
                // else quantile is the nth index of the sorted array obtained by multiplying
                // level and size of array. Example if level = 0.1 and size of array is 10.
                n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
            }
            ns[i] = n;
        }

        VectorWithMemoryTracking<Value> values(size);
        selectAtPositions(array, ns.data(), size, values.data());
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = values[i];
    }
};

// QuantileExactLow returns the high median of given data.
// Implementation is as per "medium_high function from python:
// https://docs.python.org/3/library/statistics.html#statistics.median_high
template <typename Value>
struct QuantileExactHigh : public QuantileExactBase<Value, QuantileExactHigh<Value>>
{
    using QuantileExactBase<Value, QuantileExactHigh<Value>>::array;

    Value getImpl(Float64 level)
    {
        if (!array.empty())
        {
            size_t n = 0;
            // if level is 0.5 then compute the "high" median of the sorted array
            // by the method of rounding.
            if (level == 0.5)
            {
                auto s = array.size();
                n = static_cast<size_t>(floor(s / 2));
            }
            else
            {
                // else quantile is the nth index of the sorted array obtained by multiplying
                // level and size of array. Example if level = 0.1 and size of array is 10.
                n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
            }
            Value value{};
            selectAtPositions(array, &n, 1, &value);
            return value;
        }
        return std::numeric_limits<Value>::quiet_NaN();
    }

    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
            return;
        }

        VectorWithMemoryTracking<size_t> ns(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto level = levels[indices[i]];
            size_t n = 0;
            // if level is 0.5 then compute the "high" median of the sorted array
            // by the method of rounding.
            if (level == 0.5)
            {
                auto s = array.size();
                n = static_cast<size_t>(floor(s / 2));
            }
            else
            {
                // else quantile is the nth index of the sorted array obtained by multiplying
                // level and size of array. Example if level = 0.1 and size of array is 10.
                n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
            }
            ns[i] = n;
        }

        VectorWithMemoryTracking<Value> values(size);
        selectAtPositions(array, ns.data(), size, values.data());
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = values[i];
    }
};

}
