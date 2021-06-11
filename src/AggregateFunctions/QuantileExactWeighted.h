#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** Calculates quantile by counting number of occurrences for each value in a hash map.
  *
  * It uses O(distinct(N)) memory. Can be naturally applied for values with weight.
  * In case of many identical values, it can be more efficient than QuantileExact even when weight is not used.
  */
template <typename Value>
struct QuantileExactWeighted
{
    struct Int128Hash
    {
        size_t operator()(Int128 x) const
        {
            return CityHash_v1_0_2::Hash128to64({x >> 64, x & 0xffffffffffffffffll});
        }
    };

    using Weight = UInt64;
    using UnderlyingType = typename NativeType<Value>::Type;
    using Hasher = std::conditional_t<std::is_same_v<Value, Decimal128>, Int128Hash, HashCRC32<UnderlyingType>>;

    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<UnderlyingType, Weight, Hasher, 4>;

    Map map;

    void add(const Value & x)
    {
        /// We must skip NaNs as they are not compatible with comparison sorting.
        if (!isNaN(x))
            ++map[x];
    }

    void add(const Value & x, Weight weight)
    {
        if (!isNaN(x))
            map[x] += weight;
    }

    void merge(const QuantileExactWeighted & rhs)
    {
        for (const auto & pair : rhs.map)
            map[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const
    {
        map.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        typename Map::Reader reader(buf);
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level) const
    {
        size_t size = map.size();

        if (0 == size)
            return std::numeric_limits<Value>::quiet_NaN();

        /// Copy the data to a temporary array to get the element you need in order.
        using Pair = typename Map::value_type;
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        UInt64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            array[i] = pair.getValue();
            ++i;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        UInt64 threshold = std::ceil(sum_weight * level);
        UInt64 accumulated = 0;

        const Pair * it = array;
        const Pair * end = array + size;
        while (it < end)
        {
            accumulated += it->second;

            if (accumulated >= threshold)
                break;

            ++it;
        }

        if (it == end)
            --it;

        return it->first;
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    {
        size_t size = map.size();

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = Value();
            return;
        }

        /// Copy the data to a temporary array to get the element you need in order.
        using Pair = typename Map::value_type;
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        UInt64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            array[i] = pair.getValue();
            ++i;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        UInt64 accumulated = 0;

        const Pair * it = array;
        const Pair * end = array + size;

        size_t level_index = 0;
        UInt64 threshold = std::ceil(sum_weight * levels[indices[level_index]]);

        while (it < end)
        {
            accumulated += it->second;

            while (accumulated >= threshold)
            {
                result[indices[level_index]] = it->first;
                ++level_index;

                if (level_index == num_levels)
                    return;

                threshold = std::ceil(sum_weight * levels[indices[level_index]]);
            }

            ++it;
        }

        while (level_index < num_levels)
        {
            result[indices[level_index]] = array[size - 1].first;
            ++level_index;
        }
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64) const
    {
        throw Exception("Method getFloat is not implemented for QuantileExact", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getManyFloat(const Float64 *, const size_t *, size_t, Float64 *) const
    {
        throw Exception("Method getManyFloat is not implemented for QuantileExact", ErrorCodes::NOT_IMPLEMENTED);
    }
};

}
