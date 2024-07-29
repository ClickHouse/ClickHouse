#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>

#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>

#include <numeric>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
    using Pair = typename Map::value_type;

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

        Float64 res = getFloat(level);
        if constexpr (is_decimal<Value>)
            return Value(static_cast<typename Value::NativeType>(res));
        else
            return static_cast<Value>(res);
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

        std::unique_ptr<Float64 []> res_holder(new Float64[num_levels]);
        Float64 * res = res_holder.get();
        getManyFloat(levels, indices, num_levels, res);
        for (size_t i = 0; i < num_levels; ++i)
        {
            if constexpr (is_decimal<Value>)
                result[i] = Value(static_cast<typename Value::NativeType>(res[i]));
            else
                result[i] = Value(res[i]);
        }
    }

    Float64 NO_SANITIZE_UNDEFINED getFloatImpl(const Pair * array, size_t size, Float64 position) const
    {
        /*
        for (size_t i = 0; i < size; ++i)
            std::cout << "array[" << i << "]: " << toString(Field(array[i].first)) << ", " << array[i].second << std::endl;
        std::cout << "position: " << position << std::endl;
        */
        size_t lower = static_cast<size_t>(std::floor(position));
        size_t higher = static_cast<size_t>(std::ceil(position));
        // std::cout << "lower: " << lower << ", higher: " << higher << std::endl;

        const auto * lower_it = std::lower_bound(array, array + size, lower + 1, [](const Pair & a, size_t b) { return a.second < b; });
        const auto * higher_it = std::lower_bound(array, array + size, higher + 1, [](const Pair & a, size_t b) { return a.second < b; });
        // std::cout << "lower_index:" << lower_it - array << ", higher_index:" << higher_it - array << std::endl;

        UnderlyingType lower_key = lower_it->first;
        UnderlyingType higher_key = higher_it->first;

        if (lower == higher)
            return static_cast<Float64>(lower_key);
        if (lower_key == higher_key)
            return static_cast<Float64>(lower_key);

        return (static_cast<Float64>(higher) - position) * lower_key + (position - static_cast<Float64>(lower)) * higher_key;
    }

    Float64 getFloat(Float64 level) const
    {
        size_t size = map.size();

        if (0 == size)
            return std::numeric_limits<Float64>::quiet_NaN();

        /// Copy the data to a temporary array to get the element you need in order.
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        for (const auto & pair : map)
        {
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });
        std::partial_sum(array, array + size, array, [](const Pair & acc, const Pair & p) { return Pair(p.first, acc.second + p.second); });
        Weight max_position = array[size - 1].second - 1;
        Float64 position = max_position * level;
        return getFloatImpl(array, size, position);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t num_levels, Float64 * result) const
    {
        size_t size = map.size();
        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
            return;
        }

        /// Copy the data to a temporary array to get the element you need in order.
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        for (const auto & pair : map)
        {
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });
        std::partial_sum(array, array + size, array, [](Pair acc, Pair & p) { return Pair(p.first, acc.second + p.second); });
        Weight max_position = array[size - 1].second - 1;

        for (size_t j = 0; j < num_levels; ++j)
        {
            Float64 position = max_position * levels[indices[j]];
            result[indices[j]] = getFloatImpl(array, size, position);
        }
    }
};


template <typename Value, bool float_return>
using FuncQuantileExactWeighted = AggregateFunctionQuantile<
    Value,
    QuantileExactWeighted<Value>,
    NameQuantileExactWeighted,
    true,
    std::conditional_t<float_return, Float64, void>,
    false,
    false>;
template <typename Value, bool float_return>
using FuncQuantilesExactWeighted = AggregateFunctionQuantile<
    Value,
    QuantileExactWeighted<Value>,
    NameQuantilesExactWeighted,
    true,
    std::conditional_t<float_return, Float64, void>,
    true,
    false>;

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

    factory.registerAlias("quantileInterpolatedWeighted", NameQuantileExactWeighted::name);
    factory.registerAlias("quantilesInterpolatedWeighted", NameQuantilesExactWeighted::name);
    factory.registerAlias("medianInterpolatedWeighted", NameQuantileExactWeighted::name);
}
}
