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

/** Approximates Quantile by:
  * - sorting input values and weights
  * - building a cumulative distribution based on weights
  * - performing linear interpolation between the weights and values
  *
  */
template <typename Value>
struct QuantileInterpolatedWeighted
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

    void merge(const QuantileInterpolatedWeighted & rhs)
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

    Value get(Float64 level) const
    {
        return getImpl<Value>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl<Value>(levels, indices, size, result);
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFloat is not implemented for QuantileInterpolatedWeighted");
    }

    void getManyFloat(const Float64 *, const size_t *, size_t, Float64 *) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getManyFloat is not implemented for QuantileInterpolatedWeighted");
    }

private:
    using Pair = typename std::pair<UnderlyingType, Float64>;

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    template <typename T>
    T getImpl(Float64 level) const
    {
        size_t size = map.size();

        if (0 == size)
            return std::numeric_limits<Value>::quiet_NaN();

        /// Maintain a vector of pair of values and weights for easier sorting and for building
        /// a cumulative distribution using the provided weights.
        VectorWithMemoryTracking<Pair> value_weight_pairs;
        value_weight_pairs.reserve(size);

        /// Note: weight provided must be a 64-bit integer
        /// Float64 is used as accumulator here to get approximate results.
        /// But weight used in the internal array is stored as Float64 as we
        /// do some quantile estimation operation which involves division and
        /// require Float64 level of precision.

        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += static_cast<Float64>(pair.getMapped());
            auto value = pair.getKey();
            auto weight = pair.getMapped();
            value_weight_pairs.push_back({value, weight});
        }

        ::sort(value_weight_pairs.begin(), value_weight_pairs.end(), [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 accumulated = 0;

        /// vector for populating and storing the cumulative sum using the provided weights.
        /// example: [0,1,2,3,4,5] -> [0,1,3,6,10,15]
        VectorWithMemoryTracking<Float64> weights_cum_sum;
        weights_cum_sum.reserve(size);

        for (size_t idx = 0; idx < size; ++idx)
        {
            accumulated += value_weight_pairs[idx].second;
            weights_cum_sum.push_back(accumulated);
        }

        /// The following estimation of quantile is general and the idea is:
        /// https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

        /// calculates a simple cumulative distribution based on weights
        if (sum_weight != 0)
        {
            for (size_t idx = 0; idx < size; ++idx)
                value_weight_pairs[idx].second = (weights_cum_sum[idx] - 0.5 * value_weight_pairs[idx].second) / sum_weight;
        }

        /// perform linear interpolation
        size_t idx = 0;
        if (size >= 2)
        {
            if (level >= value_weight_pairs[size - 2].second)
            {
                idx = size - 2;
            }
            else
            {
                size_t start = 0;
                size_t end = size - 1;
                while (start <= end)
                {
                    size_t mid = start + (end - start) / 2;
                    if (mid > size)
                        break;
                    if (level > value_weight_pairs[mid + 1].second)
                        start = mid + 1;
                    else
                    {
                        idx = mid;
                        end = mid - 1;
                    }
                }
            }
        }

        size_t left_idx = idx;
        size_t right_idx = idx + 1 < size ? idx + 1 : idx;

        Float64 lower_percentile = value_weight_pairs[left_idx].second;
        Float64 upper_percentile = value_weight_pairs[right_idx].second;
        UnderlyingType lower_value = value_weight_pairs[left_idx].first;
        UnderlyingType upper_value = value_weight_pairs[right_idx].first;

        if (level < lower_percentile)
            upper_value = lower_value;
        if (level > upper_percentile)
            lower_value = upper_value;

        return static_cast<T>(interpolate(level, lower_percentile, upper_percentile, lower_value, upper_value));
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    template <typename T>
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    {
        size_t size = map.size();

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = Value();
            return;
        }

        VectorWithMemoryTracking<Pair> value_weight_pairs;
        value_weight_pairs.reserve(size);

        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += static_cast<Float64>(pair.getMapped());
            auto value = pair.getKey();
            auto weight = pair.getMapped();
            value_weight_pairs.push_back({value, weight});
        }

        ::sort(value_weight_pairs.begin(), value_weight_pairs.end(), [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 accumulated = 0;

        /// vector for populating and storing the cumulative sum using the provided weights.
        /// example: [0,1,2,3,4,5] -> [0,1,3,6,10,15]
        VectorWithMemoryTracking<Float64> weights_cum_sum;
        weights_cum_sum.reserve(size);

        for (size_t idx = 0; idx < size; ++idx)
        {
            accumulated += value_weight_pairs[idx].second;
            weights_cum_sum.emplace_back(accumulated);
        }


        /// The following estimation of quantile is general and the idea is:
        /// https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

        /// calculates a simple cumulative distribution based on weights
        if (sum_weight != 0)
        {
            for (size_t idx = 0; idx < size; ++idx)
                value_weight_pairs[idx].second = (weights_cum_sum[idx] - 0.5 * value_weight_pairs[idx].second) / sum_weight;
        }

        for (size_t level_index = 0; level_index < num_levels; ++level_index)
        {
            /// perform linear interpolation for every level
            auto level = levels[indices[level_index]];

            size_t idx = 0;
            if (size >= 2)
            {
                if (level >= value_weight_pairs[size - 2].second)
                {
                    idx = size - 2;
                }
                else
                {
                    size_t start = 0;
                    size_t end = size - 1;
                    while (start <= end)
                    {
                        size_t mid = start + (end - start) / 2;
                        if (mid > size)
                            break;
                        if (level > value_weight_pairs[mid + 1].second)
                            start = mid + 1;
                        else
                        {
                            idx = mid;
                            end = mid - 1;
                        }
                    }
                }
            }

            size_t left_idx = idx;
            size_t right_idx = idx + 1 < size ? idx + 1 : idx;

            Float64 lower_percentile = value_weight_pairs[left_idx].second;
            Float64 upper_percentile = value_weight_pairs[right_idx].second;
            UnderlyingType lower_value = value_weight_pairs[left_idx].first;
            UnderlyingType upper_value = value_weight_pairs[right_idx].first;

            if (level < lower_percentile)
                upper_value = lower_value;
            if (level > upper_percentile)
                lower_value = upper_value;

            result[indices[level_index]] = static_cast<T>(interpolate(level, lower_percentile, upper_percentile, lower_value, upper_value));
        }
    }

    /// This ignores overflows or NaN's that might arise during add, sub and mul operations and doesn't aim to provide exact
    /// results since `the quantileInterpolatedWeighted` function itself relies mainly on approximation.
    UnderlyingType NO_SANITIZE_UNDEFINED interpolate(Float64 level, Float64 lower_percentile, Float64 upper_percentile, UnderlyingType lower_value, UnderlyingType upper_value) const
    {
        UnderlyingType value_diff = upper_value - lower_value;
        Float64 percentile_diff = upper_percentile - lower_percentile;
        percentile_diff = percentile_diff == 0 ? 1 : percentile_diff; /// to handle NaN behavior that might arise during integer division below.

        /// yl + (dy / dx) * (level - xl)
        return static_cast<UnderlyingType>(static_cast<Float64>(lower_value) + (static_cast<Float64>(value_diff) / percentile_diff) * (level - lower_percentile));
    }
};


template <typename Value, bool _> using FuncQuantileInterpolatedWeighted = AggregateFunctionQuantile<Value, QuantileInterpolatedWeighted<Value>, NameQuantileInterpolatedWeighted, UInt64, void, false, false>;
template <typename Value, bool _> using FuncQuantilesInterpolatedWeighted = AggregateFunctionQuantile<Value, QuantileInterpolatedWeighted<Value>, NameQuantilesInterpolatedWeighted, UInt64, void, true, false>;

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

void registerAggregateFunctionsQuantileInterpolatedWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using linear interpolation, taking into account the weight of each element.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding weights.
Quantile interpolation is then performed using the [weighted percentile method](https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method) by building a cumulative distribution based on weights and then a linear interpolation is performed using the weights and the values to compute the quantiles.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileInterpolatedWeighted(level)(expr, weight)
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
        "Computing interpolated weighted quantile",
        R"(
CREATE TABLE t (
    n Int32,
    val Int32
) ENGINE = Memory;

INSERT INTO t VALUES (0, 3), (1, 2), (2, 1), (5, 4);

SELECT quantileInterpolatedWeighted(n, val) FROM t;
        )",
        R"(
┌─quantileInterpolatedWeighted(n, val)─┐
│                                    1 │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileInterpolatedWeighted::name, {createAggregateFunctionQuantile<FuncQuantileInterpolatedWeighted>, documentation});
    factory.registerFunction(NameQuantilesInterpolatedWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesInterpolatedWeighted>, {}, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianInterpolatedWeighted", NameQuantileInterpolatedWeighted::name);
}

}
