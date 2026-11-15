#pragma once

#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <base/extended_types.h>
#include <base/sort.h>
#include <base/types.h>

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

    /// Adds `count` values, skipping NaNs.
    void addMany(const Value * __restrict values, size_t count)
    {
        if constexpr (is_floating_point<Value>)
        {
            /// Not `std::any_of`: a loop without an early exit is vectorized.
            bool has_nan = false;
            for (size_t i = 0; i < count; ++i)
                has_nan |= isNaN(values[i]);
            if (has_nan)
            {
                addManyIf(values, count, [](size_t) { return true; });
                return;
            }
        }
        reserveForMore(count);
        array.insert_assume_reserved(values, values + count);
    }

    /// Adds the values where `(mask[i] != 0) == keep_nonzero`, skipping NaNs:
    /// an `-If` condition keeps non-zero rows, a null map keeps zero rows.
    template <bool keep_nonzero>
    void addManyFiltered(const Value * __restrict values, size_t count, const UInt8 * __restrict mask)
    {
        addManyIf(values, count, [mask](size_t i) { return (mask[i] != 0) == keep_nonzero; });
    }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method add with weight is not implemented for QuantileExact");
    }

    void merge(const QuantileExactBase & rhs)
    {
        /// Exact only into an empty state: merging many states into one would otherwise reallocate on every merge.
        if (array.empty())
            array.reserve_exact(rhs.array.size());
        array.insert(rhs.array.begin(), rhs.array.end());
    }

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
        array.resize_exact(size);
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

private:
    /// Doubles the capacity as `push_back` does, starting from the inline capacity. Not `reserve`: it rounds up
    /// to a power of two bytes, so a batch could make the array larger than adding the same values one by one.
    void reserveForMore(size_t count)
    {
        size_t required = array.size() + count;
        size_t capacity = std::max<size_t>(array.capacity(), (bytes_in_arena + sizeof(Value) - 1) / sizeof(Value));
        while (capacity < required)
            capacity *= 2;
        array.reserve_exact(capacity);
    }

    /// Reserves only for the kept values, so rejected rows never grow the state.
    template <typename Keep>
    void addManyIf(const Value * __restrict values, size_t count, Keep && keep)
    {
        auto is_kept = [&](size_t i) { return static_cast<size_t>(!isNaN(values[i])) & static_cast<size_t>(keep(i)); };

        size_t size = array.size();
        if (array.capacity() - size < count)
        {
            /// The compaction writes every row at the next free slot, so it must end at a kept row to stay within the kept count.
            while (count > 0 && !is_kept(count - 1))
                --count;

            size_t kept = 0;
            for (size_t i = 0; i < count; ++i)
                kept += is_kept(i);
            if (kept == 0)
                return;
            reserveForMore(kept);
        }

        Value * __restrict out = array.data();
        for (size_t i = 0; i < count; ++i)
        {
            out[size] = values[i];
            size += is_kept(i);
        }
        array.resize_assume_reserved(size);
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
            ::nth_element(array.begin(), array.begin() + n, array.end());  /// NOTE: You can think of the radix-select algorithm.
            return array[n];
        }

        return std::numeric_limits<Value>::quiet_NaN();
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
            for (size_t i = 0; i < size; ++i)
            {
                auto level = levels[indices[i]];

                size_t n = level < 1 ? static_cast<size_t>(level * static_cast<Float64>(array.size())) : (array.size() - 1);
                ::nth_element(array.begin() + prev_n, array.begin() + n, array.end());
                result[indices[i]] = array[n];
                prev_n = n;
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
        }
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

            ::nth_element(array.begin(), array.begin() + n - 1, array.end());
            auto * nth_elem = std::min_element(array.begin() + n, array.end());

            return static_cast<Float64>(array[n - 1]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(array[n - 1], *nth_elem);
        }

        return std::numeric_limits<Float64>::quiet_NaN();
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
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
                    ::nth_element(array.begin() + prev_n, array.begin() + n - 1, array.end());
                    auto * nth_elem = std::min_element(array.begin() + n, array.end());

                    result[indices[i]] = static_cast<Float64>(array[n - 1]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(array[n - 1], *nth_elem);
                    prev_n = n - 1;
                }
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
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
            ::nth_element(array.begin(), array.begin() + n - 1, array.end());
            auto * nth_elem = std::min_element(array.begin() + n, array.end());

            return static_cast<Float64>(array[n - 1]) + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(array[n - 1], *nth_elem);
        }

        return std::numeric_limits<Float64>::quiet_NaN();
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
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
                    ::nth_element(array.begin() + prev_n, array.begin() + n - 1, array.end());
                    auto * nth_elem = std::min_element(array.begin() + n, array.end());

                    result[indices[i]] = static_cast<Float64>(array[n - 1])
                        + (h - static_cast<Float64>(n)) * quantileExactInterpolationDelta(array[n - 1], *nth_elem);
                    prev_n = n - 1;
                }
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
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
            ::nth_element(array.begin(), array.begin() + n, array.end());
            return array[n];
        }
        return std::numeric_limits<Value>::quiet_NaN();
    }

    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
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
                ::nth_element(array.begin() + prev_n, array.begin() + n, array.end());
                result[indices[i]] = array[n];
                prev_n = n;
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
        }
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
            ::nth_element(array.begin(), array.begin() + n, array.end());
            return array[n];
        }
        return std::numeric_limits<Value>::quiet_NaN();
    }

    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
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
                ::nth_element(array.begin() + prev_n, array.begin() + n, array.end());
                result[indices[i]] = array[n];
                prev_n = n;
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
        }
    }
};

}
