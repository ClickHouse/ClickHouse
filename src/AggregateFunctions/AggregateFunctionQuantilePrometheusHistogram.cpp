#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/Helpers.h>
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

template <typename Value, typename CumulativeHistogramValue>
struct QuantilePrometheusHistogram
{
    using UnderlyingType = NativeType<Value>;
    using Hasher = HashCRC32<UnderlyingType>;

    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<UnderlyingType, CumulativeHistogramValue, Hasher, 4>;
    using Pair = typename Map::value_type;

    Map map;

    void add(const Value & x, CumulativeHistogramValue cumulative_histogram_value)
    {
        if (!isNaN(x))
            map[x] += cumulative_histogram_value;
    }

    void merge(const QuantilePrometheusHistogram & rhs)
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
            return Value();

        Value res = getInterpolatedImpl(level);
        return res;
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
        getManyInterpolatedImpl(levels, indices, num_levels, result);
    }

private:
    Value getInterpolatedImpl(Float64 level) const
    {
        size_t size = map.size();

        if (size < 2)
            return std::numeric_limits<Value>::quiet_NaN();

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
        Pair max_bucket = array[size - 1];
        if (max_bucket.first != std::numeric_limits<UnderlyingType>::infinity())
            return std::numeric_limits<Value>::quiet_NaN();
        CumulativeHistogramValue max_position = max_bucket.second;
        Float64 position = static_cast<Float64>(max_position) * level;
        return quantileInterpolated(array, size, position);
    }

    void getManyInterpolatedImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    {
        size_t size = map.size();
        if (size < 2)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = std::numeric_limits<Value>::quiet_NaN();
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
        Pair max_bucket = array[size - 1];
        CumulativeHistogramValue max_position = max_bucket.second;

        for (size_t j = 0; j < num_levels; ++j)
        {
            if (max_bucket.first != std::numeric_limits<UnderlyingType>::infinity())
            {
                result[indices[j]] = std::numeric_limits<Value>::quiet_NaN();
            }
            else
            {
                Float64 position = static_cast<Float64>(max_position) * levels[indices[j]];
                result[indices[j]] = quantileInterpolated(array, size, position);
            }
        }
    }

    /// Calculate quantile, using linear interpolation between the bucket's lower and upper bound
    Value quantileInterpolated(const Pair * array, size_t size, Float64 position) const
    {
        const auto * upper_bound_it = std::lower_bound(array, array + size, position, [](const Pair & a, Float64 b) { return static_cast<Float64>(a.second) < b; });
        if (upper_bound_it == array)
        {
            if (upper_bound_it->first > 0)
            {
                // If position is in the first bucket and the first bucket's upper bounds is positive, perform interpolation as if the first bucket's lower bounds is 0.
                return static_cast<Value>(upper_bound_it->first * (position / static_cast<Float64>(upper_bound_it->second)));
            }
            else
            {
                // Otherwise, if the first bucket's upper bounds is non-positive, return the first bucket's upper bound.
                return upper_bound_it->first;
            }
        }
        else if (upper_bound_it >= array + size - 1)
        {
            // If the position is in the +Inf bucket, return the largest finite bucket's upper bound, which is the second to last bucket's upper bound.
            return (array + size - 2)->first;
        }
        const auto * lower_bound_it = upper_bound_it - 1;

        UnderlyingType histogram_bucket_lower_bound = lower_bound_it->first;
        CumulativeHistogramValue histogram_bucket_lower_value = lower_bound_it->second;
        UnderlyingType histogram_bucket_upper_bound = upper_bound_it->first;
        CumulativeHistogramValue histogram_bucket_upper_value = upper_bound_it->second;

        // Interpolate between the lower and upper bounds of the bucket that the position is in.
        return static_cast<Value>(histogram_bucket_lower_bound + (histogram_bucket_upper_bound - histogram_bucket_lower_bound) * (position - static_cast<Float64>(histogram_bucket_lower_value)) / static_cast<Float64>(histogram_bucket_upper_value - histogram_bucket_lower_value));
    }
};

template <typename Value, typename CumulativeHistogramValue>
using FuncQuantilePrometheusHistogram = AggregateFunctionQuantile<
    Value,
    QuantilePrometheusHistogram<Value, CumulativeHistogramValue>,
    NameQuantilePrometheusHistogram,
    CumulativeHistogramValue,
    void,
    false,
    false>;
template <typename Value, typename CumulativeHistogramValue>
using FuncQuantilesPrometheusHistogram = AggregateFunctionQuantile<
    Value,
    QuantilePrometheusHistogram<Value, CumulativeHistogramValue>,
    NameQuantilesPrometheusHistogram,
    CumulativeHistogramValue,
    void,
    true,
    false>;

template <template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two arguments", name);

    const DataTypePtr & upper_bound_argument_type = argument_types[0];
    WhichDataType which_upper_bound(upper_bound_argument_type);
    const DataTypePtr & cumulative_histogram_value_argument_type = argument_types[1];
    WhichDataType which_cumulative_histogram_value(cumulative_histogram_value_argument_type);
    if (which_upper_bound.idx == TypeIndex::Float32)
    {
        if (isFloat(which_cumulative_histogram_value.idx))
            return std::make_shared<Function<Float32, Float64>>(argument_types, params);
        else if (isUInt(which_cumulative_histogram_value.idx))
            return std::make_shared<Function<Float32, UInt64>>(argument_types, params);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        cumulative_histogram_value_argument_type->getName(), name);
    }
    else if (which_upper_bound.idx == TypeIndex::Float64)
    {
        if (isFloat(which_cumulative_histogram_value.idx))
            return std::make_shared<Function<Float64, Float64>>(argument_types, params);
        else if (isUInt(which_cumulative_histogram_value.idx))
            return std::make_shared<Function<Float64, UInt64>>(argument_types, params);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    cumulative_histogram_value_argument_type->getName(), name);
    }
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    upper_bound_argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantilePrometheusHistogram(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description_quantilePrometheusHistogram = R"(
Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a histogram using linear interpolation, taking into account the cumulative value and upper bounds of each histogram bucket.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding bucket upper bound values.
Quantile interpolation is then performed similarly to the PromQL [histogram_quantile()](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) function on a classic histogram, performing a linear interpolation using the lower and upper bound of the bucket in which the quantile position is found.

**See Also**

- [median](/sql-reference/aggregate-functions/reference/median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md)
    )";
    FunctionDocumentation::Syntax syntax_quantilePrometheusHistogram = R"(
quantilePrometheusHistogram(level)(bucket_upper_bound, cumulative_bucket_value)
    )";
    FunctionDocumentation::Parameters parameters_quantilePrometheusHistogram = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: `0.5`. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).", {"Float64"}}
    };
    FunctionDocumentation::Arguments arguments_quantilePrometheusHistogram = {
        {"bucket_upper_bound", "Upper bounds of the histogram buckets. The highest bucket must have an upper bound of `+Inf`.", {"Float64"}},
        {"cumulative_bucket_value", "Cumulative values of the histogram buckets. Values must be monotonically increasing as the bucket upper bound increases.", {"(U)Int*", "Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantilePrometheusHistogram = {"Returns the quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples_quantilePrometheusHistogram = {
    {
        "Usage example",
        R"(
SELECT quantilePrometheusHistogram(bucket_upper_bound, cumulative_bucket_value)
FROM VALUES('bucket_upper_bound Float64, cumulative_bucket_value UInt64', (0, 6), (0.5, 11), (1, 14), (inf, 19));
        )",
        R"(
┌─quantilePrometheusHistogram(bucket_upper_bound, cumulative_bucket_value)─┐
│                                                                     0.35 │
└──────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantilePrometheusHistogram = {25, 10};
    FunctionDocumentation::Category category_quantilePrometheusHistogram = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantilePrometheusHistogram = {description_quantilePrometheusHistogram, syntax_quantilePrometheusHistogram, arguments_quantilePrometheusHistogram, parameters_quantilePrometheusHistogram, returned_value_quantilePrometheusHistogram, examples_quantilePrometheusHistogram, introduced_in_quantilePrometheusHistogram, category_quantilePrometheusHistogram};

    factory.registerFunction(NameQuantilePrometheusHistogram::name, {createAggregateFunctionQuantile<FuncQuantilePrometheusHistogram>, documentation_quantilePrometheusHistogram});
    factory.registerFunction(NameQuantilesPrometheusHistogram::name, {createAggregateFunctionQuantile<FuncQuantilesPrometheusHistogram>, documentation_quantilePrometheusHistogram, properties});
}

}
