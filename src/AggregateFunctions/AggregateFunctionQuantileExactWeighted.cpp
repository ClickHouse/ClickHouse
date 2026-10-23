#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
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
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/** Calculates quantile by counting number of occurrences for each value in a hash map.
  *
  * It uses O(distinct(N)) memory. Can be naturally applied for values with weight.
  * In case of many identical values, it can be more efficient than QuantileExact even when weight is not used.
  */
template <typename Value, bool interpolated>
struct QuantileExactWeighted
{
    struct Int128Hash
    {
        size_t operator()(Int128 x) const
        {
            return CityHash_v1_0_2::Hash128to64({static_cast<UInt64>(x >> 64), static_cast<UInt64>(x & 0xffffffffffffffffll)});
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
        if constexpr (!interpolated)
        {
            /// Keep compatibility for function quantilesExactWeighted.
            if (!isNaN(x))
                map[x] += weight;
        }
        else
        {
            /// Ignore values with zero weight in function quantilesExactWeightedInterpolated.
            if (!isNaN(x) && weight)
                map[x] += weight;
        }
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
        if constexpr (interpolated)
            return getInterpolatedImpl(level);
        else
            return getImpl(level);
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    {
        if constexpr (interpolated)
            getManyInterpolatedImpl(levels, indices, num_levels, result);
        else
            getManyImpl(levels, indices, num_levels, result);
    }

    Float64 getFloat(Float64 level) const
    {
        if constexpr (interpolated)
            return getFloatInterpolatedImpl(level);
        else
            return getFloatImpl(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t num_levels, Float64 * result) const
    {
        if constexpr (interpolated)
            getManyFloatInterpolatedImpl(levels, indices, num_levels, result);
        else
            getManyFloatImpl(levels, indices, num_levels, result);
    }

private:
    /// get implementation without interpolation
    Value getImpl(Float64 level) const
    requires(!interpolated)
    {
        size_t size = map.size();

        if (0 == size)
            return std::numeric_limits<Value>::quiet_NaN();

        /// Copy the data to a temporary array to get the element you need in order.
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
            sum_weight += static_cast<Float64>(pair.getMapped());
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
            accumulated += static_cast<Float64>(it->second);

            if (accumulated >= threshold)
                break;

            ++it;
        }

        if (it == end)
            --it;

        return it->first;
    }

    /// getMany implementation without interpolation
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    requires(!interpolated)
    {
        size_t size = map.size();

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = Value();
            return;
        }

        /// Copy the data to a temporary array to get the element you need in order.
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += static_cast<Float64>(pair.getMapped());
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
            accumulated += static_cast<Float64>(it->second);

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

    /// getFloat implementation without interpolation
    Float64 getFloatImpl(Float64) const
    requires(!interpolated)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFloat is not implemented for QuantileExact");
    }

    /// getManyFloat implementation without interpolation
    void getManyFloatImpl(const Float64 *, const size_t *, size_t, Float64 *) const
    requires(!interpolated)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getManyFloat is not implemented for QuantileExact");
    }

    /// get implementation with interpolation
    Value getInterpolatedImpl(Float64 level) const
    requires(interpolated)
    {
        size_t size = map.size();
        if (0 == size)
            return Value();

        Float64 res = getFloatInterpolatedImpl(level);
        if constexpr (is_decimal<Value>)
            return Value(static_cast<typename Value::NativeType>(res));
        else
            return static_cast<Value>(res);
    }

    /// getMany implementation with interpolation
    void getManyInterpolatedImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    requires(interpolated)
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
        getManyFloatInterpolatedImpl(levels, indices, num_levels, res);
        for (size_t i = 0; i < num_levels; ++i)
        {
            if constexpr (is_decimal<Value>)
                result[i] = Value(static_cast<typename Value::NativeType>(res[i]));
            else
                result[i] = Value(res[i]);
        }
    }

    /// getFloat implementation with interpolation
    Float64 getFloatInterpolatedImpl(Float64 level) const
    requires(interpolated)
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
        Float64 position = static_cast<Float64>(max_position) * level;
        return quantileInterpolated(array, size, position);
    }

    /// getManyFloat implementation with interpolation
    void getManyFloatInterpolatedImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Float64 * result) const
    requires(interpolated)
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
            Float64 position = static_cast<Float64>(max_position) * levels[indices[j]];
            result[indices[j]] = quantileInterpolated(array, size, position);
        }
    }

    /// Calculate quantile, using linear interpolation between two closest values
    Float64 NO_SANITIZE_UNDEFINED quantileInterpolated(const Pair * array, size_t size, Float64 position) const
    requires(interpolated)
    {
        size_t lower = static_cast<size_t>(std::floor(position));
        size_t higher = static_cast<size_t>(std::ceil(position));

        const auto * lower_it = std::lower_bound(array, array + size, lower + 1, [](const Pair & a, size_t b) { return a.second < b; });
        const auto * higher_it = std::lower_bound(array, array + size, higher + 1, [](const Pair & a, size_t b) { return a.second < b; });
        if (lower_it == array + size)
            lower_it = array + size - 1;
        if (higher_it == array + size)
            higher_it = array + size - 1;

        UnderlyingType lower_key = lower_it->first;
        UnderlyingType higher_key = higher_it->first;

        if (lower == higher || lower_key == higher_key)
            return static_cast<Float64>(lower_key);

        return (static_cast<Float64>(higher) - position) * static_cast<Float64>(lower_key) + (position - static_cast<Float64>(lower)) * static_cast<Float64>(higher_key);
    }
};


template <typename Value, bool return_float, bool interpolated>
using FuncQuantileExactWeighted = AggregateFunctionQuantile<
    Value,
    QuantileExactWeighted<Value, interpolated>,
    std::conditional_t<interpolated, NameQuantileExactWeightedInterpolated, NameQuantileExactWeighted>,
    UInt64,
    std::conditional_t<return_float, Float64, void>,
    false,
    false>;
template <typename Value, bool return_float, bool interpolated>
using FuncQuantilesExactWeighted = AggregateFunctionQuantile<
    Value,
    QuantileExactWeighted<Value, interpolated>,
    std::conditional_t<interpolated, NameQuantilesExactWeightedInterpolated, NameQuantilesExactWeighted>,
    UInt64,
    std::conditional_t<return_float, Float64, void>,
    true,
    false>;

template <template <typename, bool, bool> class Function, bool interpolated>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<Function<TYPE, interpolated, interpolated>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false, interpolated>>(argument_types, params);

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false, interpolated>>(argument_types, params);

    if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, interpolated, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<UInt128, interpolated, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, interpolated, interpolated>>(argument_types, params);
    if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, interpolated, interpolated>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileExactWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence, taking into account the weight of each element.

To get the exact value, all the passed values are combined into an array, which is then partially sorted.
Each value is counted with its weight, as if it is present `weight` times.
A hash table is used in the algorithm.
Because of this, if the passed values are frequently repeated, the function consumes less RAM than [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact#quantileExact).
You can use this function instead of `quantileExact` and specify the weight 1.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [quantiles](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileExactWeighted(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence members. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Quantile of the specified level.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing exact weighted quantile",
        R"(
CREATE TABLE t (
    n Int32,
    val Int32
) ENGINE = Memory;

-- Insert the sample data
INSERT INTO t VALUES
(0, 3),
(1, 2),
(2, 1),
(5, 4);

SELECT quantileExactWeighted(n, val) FROM t;
        )",
        R"(
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileExactWeighted::name, {createAggregateFunctionQuantile<FuncQuantileExactWeighted, false>, documentation});
    factory.registerFunction(NameQuantilesExactWeighted::name, {createAggregateFunctionQuantile<FuncQuantilesExactWeighted, false>, {}, properties});

    FunctionDocumentation::Description description_interpolated = R"(
Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using linear interpolation, taking into account the weight of each element.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding weights.
Quantile interpolation is then performed using the [weighted percentile method](https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method) by building a cumulative distribution based on weights and then a linear interpolation is performed using the weights and the values to compute the quantiles.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [quantiles](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.

We strongly recommend using `quantileExactWeightedInterpolated` instead of `quantileInterpolatedWeighted` because `quantileExactWeightedInterpolated` is more accurate than `quantileInterpolatedWeighted`.
See the example below for more details.
    )";
    FunctionDocumentation::Syntax syntax_interpolated = R"(
quantileExactWeightedInterpolated(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments_interpolated = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence members. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters_interpolated = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_interpolated = {"Quantile of the specified level.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples_interpolated = {
    {
        "Computing exact weighted interpolated quantile",
        R"(
SELECT quantileExactWeightedInterpolated(n, val) FROM t;
        )",
        R"(
┌─quantileExactWeightedInterpolated(n, val)─┐
│                                       1.5 │
└───────────────────────────────────────────┘
        )"
    },
    {
        "Prefer quantileExactWeightedInterpolated over quantileInterpolatedWeighted",
        R"(
SELECT
    quantileExactWeightedInterpolated(0.99)(number, 1),
    quantile(0.99)(number),
    quantileInterpolatedWeighted(0.99)(number, 1)
FROM numbers(9)
        )",
        R"(
┌─quantileExactWeightedInterpolated(0.99)(number, 1)─┬─quantile(0.99)(number)─┬─quantileInterpolatedWeighted(0.99)(number, 1)─┐
│                                               7.92 │                   7.92 │                                             8 │
└────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_interpolated = {24, 10};
    FunctionDocumentation::Category category_interpolated = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_interpolated = {description_interpolated, syntax_interpolated, arguments_interpolated, parameters_interpolated, returned_value_interpolated, examples_interpolated, introduced_in_interpolated, category_interpolated};

    factory.registerFunction(NameQuantileExactWeightedInterpolated::name, {createAggregateFunctionQuantile<FuncQuantileExactWeighted, true>, documentation_interpolated});
    factory.registerFunction(
        NameQuantilesExactWeightedInterpolated::name, {createAggregateFunctionQuantile<FuncQuantilesExactWeighted, true>, {}, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianExactWeighted", NameQuantileExactWeighted::name);
    factory.registerAlias("medianExactWeightedInterpolated", NameQuantileExactWeightedInterpolated::name);
}

}
