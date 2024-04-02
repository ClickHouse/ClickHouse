#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>

#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

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
    using UnderlyingType = NativeType<Value>;
    using Hasher = HashCRC32<UnderlyingType>;

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

        /// Note: 64-bit integer weight can overflow.
        /// We do some implementation specific behaviour (return approximate or garbage results).
        /// Float64 is used as accumulator here to get approximate results.
        /// But weight can be already overflowed in computations in 'add' and 'merge' methods.
        /// It will be reasonable to change the type of weight to Float64 in the map,
        /// but we don't do that for compatibility of serialized data.

        size_t i = 0;
        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 threshold = std::ceil(sum_weight * level);
        Float64 accumulated = 0;

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
        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 accumulated = 0;

        const Pair * it = array;
        const Pair * end = array + size;

        size_t level_index = 0;
        Float64 threshold = std::ceil(sum_weight * levels[indices[level_index]]);

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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFloat is not implemented for QuantileExact");
    }

    void getManyFloat(const Float64 *, const size_t *, size_t, Float64 *) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getManyFloat is not implemented for QuantileExact");
    }
};


template <typename Value, bool _> using FuncQuantileExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantileExactWeighted, true, void, false, false>;
template <typename Value, bool _> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantilesExactWeighted, true, void, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false>>(argument_types, params);

    if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<UInt128, true>>(argument_types, params);
    if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, true>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantileExactWeighted::name, createAggregateFunctionQuantile<FuncQuantileExactWeighted>);
    factory.registerFunction(NameQuantilesExactWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesExactWeighted>, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianExactWeighted", NameQuantileExactWeighted::name);
}

}
