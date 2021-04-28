#pragma once

#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>
#include <common/types.h>
#include <ext/bit_cast.h>

namespace DB
{
template <typename Value>
struct QuantileBFloat16Histogram
{
    using bfloat16 = UInt16;
    using Weight = UInt64;
    using Data = HashMap<bfloat16, Weight>;
    Data data;

    void add(const Value & x) { add(x, 1); }

    void add(const Value & x, Weight w)
    {
        if (!isNaN(x))
            data[to_bfloat16(x)] += w;
    }

    void merge(const QuantileBFloat16Histogram & rhs)
    {
        for (const auto & pair : rhs.data)
            data[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const { data.write(buf); }

    void deserialize(ReadBuffer & buf) { data.read(buf); }

    Value get(Float64 level) const { return getImpl<Value>(level); }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

    Float64 getFloat(Float64 level) const { return getImpl<Float64>(level); }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

private:
    bfloat16 to_bfloat16(const Value & x) const { return ext::bit_cast<UInt32>(static_cast<Float32>(x)) >> 16; }

    Float32 to_Float32(const bfloat16 & x) const { return ext::bit_cast<Float32>(x << 16); }

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
            *arr_it = {to_Float32(pair.getKey()), pair.getMapped()};
            ++arr_it;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

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
            *arr_it = {to_Float32(pair.getKey()), pair.getMapped()};
            ++arr_it;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

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
