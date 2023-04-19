#pragma once

#include <base/types.h>
#include <base/bit_cast.h>
#include <base/sort.h>
#include <Common/HashTable/HashMap.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/** `bfloat16` is a 16-bit floating point data type that is the same as the corresponding most significant 16 bits of the `float`.
  * https://en.wikipedia.org/wiki/Bfloat16_floating-point_format
  *
  * To calculate quantile, simply convert input value to 16 bit (convert to float, then take the most significant 16 bits),
  * and calculate the histogram of these values.
  *
  * Hash table is the preferred way to store histogram, because the number of distinct values is small:
  * ```
  * SELECT uniq(bfloat)
  * FROM
  * (
  *     SELECT
  *         number,
  *         toFloat32(number) AS f,
  *         bitShiftRight(bitAnd(reinterpretAsUInt32(reinterpretAsFixedString(f)), 4294901760) AS cut, 16),
  *         reinterpretAsFloat32(reinterpretAsFixedString(cut)) AS bfloat
  *     FROM numbers(100000000)
  * )
  *
  * ┌─uniq(bfloat)─┐
  * │         2623 │
  * └──────────────┘
  * ```
  * (when increasing the range of values 1000 times, the number of distinct bfloat16 values increases just by 1280).
  *
  * Then calculate quantile from the histogram.
  *
  * This sketch is very simple and rough. Its relative precision is constant 1 / 256 = 0.390625%.
  */
template <typename Value>
struct QuantileBFloat16Histogram
{
    using BFloat16 = UInt16;
    using Weight = UInt64;

    /// Make automatic memory for 16 elements to avoid allocations for small states.
    /// The usage of trivial hash is ok, because we effectively take logarithm of the values and pathological cases are unlikely.
    using Data = HashMapWithStackMemory<BFloat16, Weight, TrivialHash, 4>;

    Data data;

    void add(const Value & x)
    {
        add(x, 1);
    }

    void add(const Value & x, Weight w)
    {
        if (!isNaN(x))
            data[toBFloat16(x)] += w;
    }

    void merge(const QuantileBFloat16Histogram & rhs)
    {
        for (const auto & pair : rhs.data)
            data[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const
    {
        data.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        data.read(buf);
    }

    Value get(Float64 level) const
    {
        return getImpl<Value>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

    Float64 getFloat(Float64 level) const
    {
        return getImpl<Float64>(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

private:
    /// Take the most significant 16 bits of the floating point number.
    BFloat16 toBFloat16(const Value & x) const
    {
        return bit_cast<UInt32>(static_cast<Float32>(x)) >> 16;
    }

    /// Put the bits into most significant 16 bits of the floating point number and fill other bits with zeros.
    Float32 toFloat32(const BFloat16 & x) const
    {
        return bit_cast<Float32>(x << 16);
    }

    using Pair = PairNoInit<Float32, Weight>;

    template <typename T>
    T getImpl(Float64 level) const
    {
        size_t size = data.size();

        if (0 == size)
            return std::numeric_limits<T>::quiet_NaN();

        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        Float64 sum_weight = 0;
        Pair * arr_it = array;
        for (const auto & pair : data)
        {
            sum_weight += pair.getMapped();
            *arr_it = {toFloat32(pair.getKey()), pair.getMapped()};
            ++arr_it;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 threshold = std::ceil(sum_weight * level);
        Float64 accumulated = 0;

        for (const Pair * p = array; p != (array + size); ++p)
        {
            accumulated += p->second;

            if (accumulated >= threshold)
                return p->first;
        }

        return array[size - 1].first;
    }

    template <typename T>
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t num_levels, T * result) const
    {
        size_t size = data.size();

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = std::numeric_limits<T>::quiet_NaN();

            return;
        }

        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        Float64 sum_weight = 0;
        Pair * arr_it = array;
        for (const auto & pair : data)
        {
            sum_weight += pair.getMapped();
            *arr_it = {toFloat32(pair.getKey()), pair.getMapped()};
            ++arr_it;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        size_t level_index = 0;
        Float64 accumulated = 0;
        Float64 threshold = std::ceil(sum_weight * levels[indices[level_index]]);

        for (const Pair * p = array; p != (array + size); ++p)
        {
            accumulated += p->second;

            while (accumulated >= threshold)
            {
                result[indices[level_index]] = p->first;
                ++level_index;

                if (level_index == num_levels)
                    return;

                threshold = std::ceil(sum_weight * levels[indices[level_index]]);
            }
        }

        while (level_index < num_levels)
        {
            result[indices[level_index]] = array[size - 1].first;
            ++level_index;
        }
    }
};

}
