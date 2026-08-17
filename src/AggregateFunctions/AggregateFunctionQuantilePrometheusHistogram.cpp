#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <numeric>
#include <utility>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, typename CumulativeHistogramValue, typename Pair>
Value quantilePrometheusHistogramInterpolated(const Pair * array, size_t size, Float64 position)
{
    using UnderlyingType = NativeType<Value>;

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
        return quantilePrometheusHistogramInterpolated<Value, CumulativeHistogramValue>(array, size, position);
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

template <typename CumulativeHistogramValue, bool is_float>
struct QuantilePrometheusHistogramArrayData
{
    using UnderlyingType = Float64;

    struct Bucket
    {
        PODArray<CumulativeHistogramValue> values;
        PODArray<UInt8> present;

        void resize(size_t size)
        {
            values.resize_fill(size);
            present.resize_fill(size);
        }
    };

    using Hasher = HashCRC32<UnderlyingType>;
    using Map = HashMapWithStackMemory<UnderlyingType, Bucket, Hasher, 4>;

    static constexpr UInt8 FORMAT_VERSION = 1;

    Map buckets;
    PODArray<UInt8> has_values;
    size_t grid_size = 0;

    void ensureGridSize(size_t new_grid_size)
    {
        if (new_grid_size <= grid_size)
            return;

        for (auto & pair : buckets)
            pair.getMapped().resize(new_grid_size);

        has_values.resize_fill(new_grid_size);
        grid_size = new_grid_size;
    }

    void add(const IColumn ** columns, size_t row_num)
    {
        const Float64 le = columns[0]->getFloat64(row_num);

        const auto & values_column = assert_cast<const ColumnArray &>(*columns[1]);
        const auto & offsets = values_column.getOffsets();
        const size_t values_begin = row_num == 0 ? 0 : offsets[row_num - 1];
        const size_t values_end = offsets[row_num];
        const size_t num_steps = values_end - values_begin;

        if (num_steps == 0)
            return;

        ensureGridSize(num_steps);

        const IColumn * values_nested = &values_column.getData();
        const NullMap * null_map = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(values_nested))
        {
            null_map = &nullable->getNullMapData();
            values_nested = &nullable->getNestedColumn();
        }

        Bucket * bucket = nullptr;
        if (!isNaN(le))
        {
            auto & bucket_ref = buckets[le];
            bucket_ref.resize(grid_size);
            bucket = &bucket_ref;
        }

        for (size_t t = 0; t < num_steps; ++t)
        {
            const size_t value_index = values_begin + t;
            if (null_map && (*null_map)[value_index])
                continue;

            /// Keep this separate from `bucket->present`: a non-null value with a NaN `le`
            /// still makes the result non-null. The result for a grid position with no valid
            /// bucket bounds matches the nested aggregate's default value.
            has_values[t] = 1;

            if (bucket)
            {
                if constexpr (is_float)
                    bucket->values[t] += values_nested->getFloat64(value_index);
                else
                    bucket->values[t] += values_nested->getUInt(value_index);
                bucket->present[t] = 1;
            }
        }
    }

    void merge(const QuantilePrometheusHistogramArrayData & rhs)
    {
        ensureGridSize(rhs.grid_size);

        for (size_t t = 0; t < rhs.grid_size; ++t)
            has_values[t] |= rhs.has_values[t];

        for (const auto & rhs_pair : rhs.buckets)
        {
            auto & bucket = buckets[rhs_pair.getKey()];
            bucket.resize(grid_size);

            const auto & rhs_bucket = rhs_pair.getMapped();
            for (size_t t = 0; t < rhs.grid_size; ++t)
            {
                if (rhs_bucket.present[t])
                {
                    bucket.values[t] += rhs_bucket.values[t];
                    bucket.present[t] = 1;
                }
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeVarUInt(grid_size, buf);
        writeVarUInt(buckets.size(), buf);

        for (size_t t = 0; t < grid_size; ++t)
            writeBinaryLittleEndian(has_values[t], buf);

        for (const auto & pair : buckets)
        {
            writeBinaryLittleEndian(pair.getKey(), buf);

            const auto & bucket = pair.getMapped();
            for (size_t t = 0; t < grid_size; ++t)
                writeBinaryLittleEndian(bucket.values[t], buf);
            for (size_t t = 0; t < grid_size; ++t)
                writeBinaryLittleEndian(bucket.present[t], buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        UInt8 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize state of aggregate function quantilePrometheusHistogramArray: expected format version {}, got {}",
                UInt32{FORMAT_VERSION}, UInt32{format_version});

        buckets.clear();
        grid_size = 0;
        has_values.clear();

        size_t new_grid_size = 0;
        readVarUInt(new_grid_size, buf);
        ensureGridSize(new_grid_size);

        size_t num_buckets = 0;
        readVarUInt(num_buckets, buf);

        for (size_t t = 0; t < grid_size; ++t)
            readBinaryLittleEndian(has_values[t], buf);

        for (size_t i = 0; i < num_buckets; ++i)
        {
            UnderlyingType le = 0;
            readBinaryLittleEndian(le, buf);

            auto & bucket = buckets[le];
            bucket.resize(grid_size);
            for (size_t t = 0; t < grid_size; ++t)
                readBinaryLittleEndian(bucket.values[t], buf);
            for (size_t t = 0; t < grid_size; ++t)
                readBinaryLittleEndian(bucket.present[t], buf);
        }
    }

    void insertResultInto(IColumn & to, Float64 level) const
    {
        auto & result = assert_cast<ColumnArray &>(to);
        auto & result_data = assert_cast<ColumnNullable &>(result.getData());
        auto & result_values = assert_cast<ColumnFloat64 &>(result_data.getNestedColumn()).getData();
        auto & result_null_map = result_data.getNullMapData();

        result.getOffsets().push_back(result.getOffsets().back() + grid_size);

        using Pair = std::pair<UnderlyingType, CumulativeHistogramValue>;
        struct SortedBucket
        {
            UnderlyingType le;
            const Bucket * bucket;
        };

        VectorWithMemoryTracking<SortedBucket> sorted_buckets;
        sorted_buckets.reserve(buckets.size());
        for (const auto & pair : buckets)
            sorted_buckets.push_back({pair.getKey(), &pair.getMapped()});

        std::sort(sorted_buckets.begin(), sorted_buckets.end(), [](const auto & lhs, const auto & rhs) { return lhs.le < rhs.le; });

        VectorWithMemoryTracking<Pair> sorted_values;
        sorted_values.reserve(buckets.size());

        for (size_t t = 0; t < grid_size; ++t)
        {
            if (!has_values[t])
            {
                result_values.push_back(0);
                result_null_map.push_back(UInt8{1});
                continue;
            }

            sorted_values.clear();
            for (const auto & sorted_bucket : sorted_buckets)
            {
                if (sorted_bucket.bucket->present[t])
                    sorted_values.emplace_back(sorted_bucket.le, sorted_bucket.bucket->values[t]);
            }

            Float64 result_value = std::numeric_limits<Float64>::quiet_NaN();
            if (sorted_values.empty())
            {
                /// Match the nested aggregate's empty-state result when all non-NULL values had
                /// an invalid upper bound. The PromQL lowering filters those rows before this
                /// aggregate is called, but keeping the direct aggregate equivalent is useful.
                result_value = 0;
            }
            else if (sorted_values.size() >= 2 && sorted_values.back().first == std::numeric_limits<UnderlyingType>::infinity())
            {
                const auto max_position = sorted_values.back().second;
                const Float64 position = static_cast<Float64>(max_position) * level;
                result_value = quantilePrometheusHistogramInterpolated<Float64, CumulativeHistogramValue>(sorted_values.data(), sorted_values.size(), position);
            }

            result_values.push_back(result_value);
            result_null_map.push_back(UInt8{0});
        }
    }
};

struct NameQuantilePrometheusHistogramArray
{
    static constexpr auto name = "quantilePrometheusHistogramArray";
};

template <typename CumulativeHistogramValue, bool is_float>
class AggregateFunctionQuantilePrometheusHistogramArray final
    : public IAggregateFunctionDataHelper<
        QuantilePrometheusHistogramArrayData<CumulativeHistogramValue, is_float>,
        AggregateFunctionQuantilePrometheusHistogramArray<CumulativeHistogramValue, is_float>>
{
    using Data = QuantilePrometheusHistogramArrayData<CumulativeHistogramValue, is_float>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionQuantilePrometheusHistogramArray<CumulativeHistogramValue, is_float>>;

    Float64 level = 0.5;

public:
    AggregateFunctionQuantilePrometheusHistogramArray(const DataTypes & argument_types_, const Array & params)
        : Base(argument_types_, params, std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())))
    {
        if (!params.empty())
            level = params[0].safeGet<Float64>();
    }

    String getName() const override { return NameQuantilePrometheusHistogramArray::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to, level);
    }
};

template <typename CumulativeHistogramValue, bool is_float>
using FuncQuantilePrometheusHistogramArray = AggregateFunctionQuantilePrometheusHistogramArray<CumulativeHistogramValue, is_float>;

AggregateFunctionPtr createAggregateFunctionQuantilePrometheusHistogramArray(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two arguments", name);

    WhichDataType which_upper_bound(argument_types[0]);
    if (which_upper_bound.idx != TypeIndex::Float64)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument for aggregate function {}", argument_types[0]->getName(), name);

    const auto * values_array_type = typeid_cast<const DataTypeArray *>(argument_types[1].get());
    if (!values_array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for aggregate function {} must be an array", name);

    WhichDataType which_value(removeNullable(values_array_type->getNestedType()));
    if (which_value.isFloat())
        return std::make_shared<FuncQuantilePrometheusHistogramArray<Float64, true>>(argument_types, params);
    if (which_value.isUInt())
        return std::make_shared<FuncQuantilePrometheusHistogramArray<UInt64, false>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument for aggregate function {}", argument_types[1]->getName(), name);
}

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
    factory.registerFunction(NameQuantilePrometheusHistogramArray::name, {createAggregateFunctionQuantilePrometheusHistogramArray, FunctionDocumentation::INTERNAL_FUNCTION_DOCS});

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
}

}
