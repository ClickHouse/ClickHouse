#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Field.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <numeric>
#include <type_traits>


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

    Float64 getFraction(Float64 lower, Float64 upper) const
    {
        struct FractionRank
        {
            CumulativeHistogramValue cumulative{};
            Float64 fractional = 0;
        };

        size_t size = map.size();
        if (size == 0)
            return std::numeric_limits<Float64>::quiet_NaN();

        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        for (const auto & pair : map)
            array[i++] = pair.getValue();

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        const Pair & max_bucket = array[size - 1];
        if (max_bucket.first != std::numeric_limits<UnderlyingType>::infinity())
            return std::numeric_limits<Float64>::quiet_NaN();

        CumulativeHistogramValue count = max_bucket.second;
        if (count == 0 || isNaN(lower) || isNaN(upper))
            return std::numeric_limits<Float64>::quiet_NaN();

        if (lower >= upper)
            return 0;

        CumulativeHistogramValue rank = 0;
        FractionRank lower_rank;
        FractionRank upper_rank;
        bool lower_set = false;
        bool upper_set = false;
        Float64 lower_bound = static_cast<Float64>(array[0].first) > 0
            ? 0
            : -std::numeric_limits<Float64>::infinity();

        for (size_t index = 0; index < size; ++index)
        {
            const Pair & bucket = array[index];
            Float64 upper_bound = static_cast<Float64>(bucket.first);

            auto interpolate = [&](Float64 value) -> FractionRank
            {
                if (lower_bound == -std::numeric_limits<Float64>::infinity())
                    return {bucket.second, 0};

                /// Keep the cumulative counts exact until the bucket delta is computed.
                /// This preserves one-count resolution for UInt64 counts above 2^53.
                Float64 bucket_count_delta = subtractCounts(bucket.second, rank);
                return {rank, bucket_count_delta * (value - lower_bound) / (upper_bound - lower_bound)};
            };

            if (!lower_set && lower_bound >= lower)
            {
                lower_rank = {rank, 0};
                lower_set = true;
            }
            if (!upper_set && lower_bound >= upper)
            {
                upper_rank = {rank, 0};
                upper_set = true;
            }
            if (lower_set && upper_set)
                break;

            if (!lower_set && lower_bound < lower && upper_bound > lower)
            {
                lower_rank = interpolate(lower);
                lower_set = true;
            }
            if (!upper_set && lower_bound < upper && upper_bound > upper)
            {
                upper_rank = interpolate(upper);
                upper_set = true;
            }
            if (lower_set && upper_set)
                break;

            rank = bucket.second;
            lower_bound = upper_bound;
        }

        auto clamp_rank = [&](FractionRank & rank_value, bool rank_set)
        {
            if (!rank_set
                || subtractCounts(rank_value.cumulative, count) + rank_value.fractional > 0)
                rank_value = {count, 0};
        };

        clamp_rank(lower_rank, lower_set);
        clamp_rank(upper_rank, upper_set);

        /// Subtract cumulative counts before converting them to Float64. Converting the
        /// individual UInt64 counts first loses one-count differences above 2^53.
        Float64 rank_difference = subtractCounts(upper_rank.cumulative, lower_rank.cumulative);
        rank_difference += upper_rank.fractional - lower_rank.fractional;
        return rank_difference / static_cast<Float64>(count);
    }

private:
    static Float64 subtractCounts(CumulativeHistogramValue lhs, CumulativeHistogramValue rhs)
    {
        if constexpr (std::is_same_v<CumulativeHistogramValue, UInt64>)
        {
            if (lhs >= rhs)
                return static_cast<Float64>(lhs - rhs);
            return -static_cast<Float64>(rhs - lhs);
        }
        else
        {
            return lhs - rhs;
        }
    }

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
                return static_cast<Value>(static_cast<Float64>(upper_bound_it->first) * (position / static_cast<Float64>(upper_bound_it->second)));
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

        /// Subtract in the original integer types and cast the difference to `Float64`,
        /// so we don't lose 1-count resolution above `2^53` for `UInt64` cumulative counts.
        Float64 bucket_bound_width = static_cast<Float64>(histogram_bucket_upper_bound - histogram_bucket_lower_bound);
        Float64 bucket_value_width = static_cast<Float64>(histogram_bucket_upper_value - histogram_bucket_lower_value);
        Float64 position_offset = position - static_cast<Float64>(histogram_bucket_lower_value);

        // Interpolate between the lower and upper bounds of the bucket that the position is in.
        return static_cast<Value>(static_cast<Float64>(histogram_bucket_lower_bound) + bucket_bound_width * position_offset / bucket_value_width);
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

struct NameFractionPrometheusHistogram { static constexpr auto name = "fractionPrometheusHistogram"; };

template <typename Value, typename CumulativeHistogramValue>
class AggregateFunctionFractionPrometheusHistogram final
    : public IAggregateFunctionDataHelper<
        QuantilePrometheusHistogram<Value, CumulativeHistogramValue>,
        AggregateFunctionFractionPrometheusHistogram<Value, CumulativeHistogramValue>>
{
    using Data = QuantilePrometheusHistogram<Value, CumulativeHistogramValue>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionFractionPrometheusHistogram<Value, CumulativeHistogramValue>>;

    Float64 lower;
    Float64 upper;

public:
    AggregateFunctionFractionPrometheusHistogram(const DataTypes & argument_types_, const Array & params)
        : Base(argument_types_, params, std::make_shared<DataTypeFloat64>())
    {
        if (params.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two parameters", getName());

        lower = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        upper = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[1]);
    }

    String getName() const override { return NameFractionPrometheusHistogram::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = static_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData()[row_num];
        if constexpr (std::is_same_v<CumulativeHistogramValue, UInt64>)
            this->data(place).add(value, columns[1]->getUInt(row_num));
        else
            this->data(place).add(value, columns[1]->getFloat64(row_num));
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).getFraction(lower, upper));
    }
};

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

void registerAggregateFunctionsQuantilePrometheusHistogram(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantilePrometheusHistogram(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description_quantilePrometheusHistogram = R"(
Computes [quantile](https://en.wikipedia.org/wiki/Quantile) of a histogram using linear interpolation, taking into account the cumulative value and upper bounds of each histogram bucket.

To get the interpolated value, all the passed values are combined into an array, which are then sorted by their corresponding bucket upper bound values.
Quantile interpolation is then performed similarly to the PromQL [histogram_quantile()](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) function on a classic histogram, performing a linear interpolation using the lower and upper bound of the bucket in which the quantile position is found.

**See Also**

- [median](/reference/functions/aggregate-functions/median)
- [quantiles](/reference/functions/aggregate-functions/quantiles)
    )";
    FunctionDocumentation::Syntax syntax_quantilePrometheusHistogram = R"(
quantilePrometheusHistogram(level)(bucket_upper_bound, cumulative_bucket_value)
    )";
    FunctionDocumentation::Parameters parameters_quantilePrometheusHistogram = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: `0.5`. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).", {"Float64"}}
    };
    FunctionDocumentation::Arguments arguments_quantilePrometheusHistogram = {
        {"bucket_upper_bound", "Upper bounds of the histogram buckets. The highest bucket must have an upper bound of `+Inf`.", {"Float*"}},
        {"cumulative_bucket_value", "Cumulative values of the histogram buckets. Values must be monotonically increasing as the bucket upper bound increases.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantilePrometheusHistogram = {"Returns the quantile of the specified level. The floating-point type of the result matches the type of `bucket_upper_bound`.", {"Float32", "Float64"}};
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

    FunctionDocumentation::Description description_quantilesPrometheusHistogram = R"(
Computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) of a histogram using linear interpolation at different levels simultaneously, taking into account the cumulative value and upper bounds of each histogram bucket.

This function is equivalent to [`quantilePrometheusHistogram`](/reference/functions/aggregate-functions/quantilePrometheusHistogram) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.
    )";
    FunctionDocumentation::Syntax syntax_quantilesPrometheusHistogram = R"(
quantilesPrometheusHistogram(level1, level2, ...)(bucket_upper_bound, cumulative_bucket_value)
    )";
    FunctionDocumentation::Parameters parameters_quantilesPrometheusHistogram = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float64"}}
    };
    FunctionDocumentation::Arguments arguments_quantilesPrometheusHistogram = {
        {"bucket_upper_bound", "Upper bounds of the histogram buckets. The highest bucket must have an upper bound of `+Inf`.", {"Float*"}},
        {"cumulative_bucket_value", "Cumulative values of the histogram buckets. Values must be monotonically increasing as the bucket upper bound increases.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantilesPrometheusHistogram = {"Array of quantiles of the specified levels in the same order as the levels were specified. The floating-point type of the result matches the type of `bucket_upper_bound`.", {"Array(Float32)", "Array(Float64)"}};
    FunctionDocumentation::Examples examples_quantilesPrometheusHistogram = {
    {
        "Usage example",
        R"(
SELECT quantilesPrometheusHistogram(0.25, 0.5, 0.75)(bucket_upper_bound, cumulative_bucket_value)
FROM VALUES('bucket_upper_bound Float64, cumulative_bucket_value UInt64', (0, 6), (0.5, 11), (1, 14), (inf, 19));
        )",
        R"(
┌─quantilesPrometheusHistogram(0.25, 0.5, 0.75)(bucket_upper_bound, cumulative_bucket_value)─┐
│ [0,0.35,1]                                                                                 │
└────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantilesPrometheusHistogram = {25, 10};
    FunctionDocumentation::Category category_quantilesPrometheusHistogram = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantilesPrometheusHistogram = {description_quantilesPrometheusHistogram, syntax_quantilesPrometheusHistogram, arguments_quantilesPrometheusHistogram, parameters_quantilesPrometheusHistogram, returned_value_quantilesPrometheusHistogram, examples_quantilesPrometheusHistogram, introduced_in_quantilesPrometheusHistogram, category_quantilesPrometheusHistogram};

    factory.registerFunction(NameQuantilesPrometheusHistogram::name, {createAggregateFunctionQuantile<FuncQuantilesPrometheusHistogram>, documentation_quantilesPrometheusHistogram, properties});

    factory.registerFunction(NameFractionPrometheusHistogram::name, {
        createAggregateFunctionQuantile<AggregateFunctionFractionPrometheusHistogram>,
        FunctionDocumentation::INTERNAL_FUNCTION_DOCS});
}

}
