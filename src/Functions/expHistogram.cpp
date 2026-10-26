#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantileDD.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <optional>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// OpenTelemetry exponential histogram: bucket `i` of scale `s` holds the values in (γⁱ, γⁱ⁺¹], γ = 2^(2⁻ˢ)
/// https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram
///
/// A `quantileDD` sketch with the same γ has the same bins: bin `k` holds [γᵏ, γᵏ⁺¹), so bucket `i` is bin `i`. Only the
/// boundary values differ:
///
///                      γⁱ                          γⁱ⁺¹
/// OpenTelemetry bucket (───────────────────────────]   γⁱ < x ≤ γⁱ⁺¹
/// quantileDD bin       [───────────────────────────)   γⁱ ≤ x < γⁱ⁺¹
///
/// Other scales give an unsupported relative accuracy: e.g. 1.0 at scale -6, below `QuantileDD::min_relative_accuracy`
/// at scale 19
constexpr Int64 min_supported_scale = -5;
constexpr Int64 max_supported_scale = 18;
/// The maximum scale of the OpenTelemetry specification, the default maximum of the SDKs. Scales 19 and 20 are downscaled
/// to `max_supported_scale` exactly: 2 or 4 buckets make one
constexpr Int64 max_input_scale = 20;

template <typename Value>
const DDSketchDenseLogarithmic & getSketchImpl(ConstAggregateDataPtr place)
{
    return reinterpret_cast<const QuantileDD<Value> *>(place)->getSketch();
}

using GetSketch = const DDSketchDenseLogarithmic & (*)(ConstAggregateDataPtr);

template <typename Value, bool float_return>
GetSketch getSketchFunctionFor(const IAggregateFunction & function)
{
    if (typeid_cast<const FuncQuantileDD<Value, float_return> *>(&function)
        || typeid_cast<const FuncQuantilesDD<Value, float_return> *>(&function))
        return &getSketchImpl<Value>;
    return nullptr;
}

/// Only the functions of `createAggregateFunctionQuantile`, whose places are exactly `QuantileDD<Value>`. Not wrappers with
/// the same name: e.g. over `Nullable`, the state has a prefix
GetSketch getSketchFunction(const DataTypeAggregateFunction & type)
{
    const IAggregateFunction & function = type.getFunction()->getBaseAggregateFunctionWithSameStateRepresentation();
    GetSketch result = nullptr;
#define DISPATCH(TYPE) \
    if (!result) \
        result = getSketchFunctionFor<TYPE, true>(function);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (!result)
        result = getSketchFunctionFor<DataTypeDate::FieldType, false>(function);
    if (!result)
        result = getSketchFunctionFor<DataTypeDateTime::FieldType, false>(function);
    return result;
}

/// Scale and offsets: integers that fit into Int64
bool isSignedIntegerArgument(const IDataType & type)
{
    return isNativeInt(type) || (isNativeUInt(type) && type.getSizeOfValueInMemory() < sizeof(UInt64));
}

/// `Array(Nothing)` is the type of an empty array literal
bool isBucketCountsArgument(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return array_type && (isNativeUInt(array_type->getNestedType()) || isNothing(array_type->getNestedType()));
}

class FunctionFromExpHistogram : public IFunction
{
public:
    static constexpr auto name = "fromExpHistogram";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromExpHistogram>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    /// `AggregateFunction` cannot be inside `Nullable`, so the default implementation would read NULLs as zeros
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {6}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 6 && arguments.size() != 7) [[unlikely]]
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 6 or 7 arguments: scale, zero_count, positive_offset, positive_bucket_counts, "
                "negative_offset, negative_bucket_counts[, relative_accuracy], but {} were passed",
                getName(),
                arguments.size());

        const auto check = [&](size_t index, bool valid, std::string_view expected)
        {
            if (!valid) [[unlikely]]
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be {}, but it has type {}",
                    index + 1,
                    getName(),
                    expected,
                    arguments[index].type->getName());
        };
        check(0, isSignedIntegerArgument(*arguments[0].type), "an integer (scale)");
        check(1, isNativeUInt(arguments[1].type), "an unsigned integer (zero_count)");
        check(2, isSignedIntegerArgument(*arguments[2].type), "an integer (positive_offset)");
        check(3, isBucketCountsArgument(*arguments[3].type), "an array of unsigned integers (positive_bucket_counts)");
        check(4, isSignedIntegerArgument(*arguments[4].type), "an integer (negative_offset)");
        check(5, isBucketCountsArgument(*arguments[5].type), "an array of unsigned integers (negative_bucket_counts)");

        /// The finest, so that copies of the states keep the resolution: a copy is a merge into a new state
        Float64 relative_accuracy = QuantileDD<Float64>::min_relative_accuracy;
        if (arguments.size() == 7)
        {
            /// Not `Float32`: 0.01 would be 0.009999999776482582 in the type
            check(6, WhichDataType(arguments[6].type).isFloat64(), "a constant Float64 number (relative_accuracy)");
            if (!arguments[6].column || !isColumnConst(*arguments[6].column)) [[unlikely]]
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 7 (relative_accuracy) of function {} must be constant", getName());
            relative_accuracy = arguments[6].column->getFloat64(0);
        }

        return std::make_shared<DataTypeAggregateFunction>(getAggregateFunction(relative_accuracy), DataTypes{std::make_shared<DataTypeFloat64>()}, Array{relative_accuracy});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto int64_type = std::make_shared<DataTypeInt64>();
        const auto uint64_type = std::make_shared<DataTypeUInt64>();
        const auto counts_type = std::make_shared<DataTypeArray>(uint64_type);
        const auto cast = [&](size_t index, const DataTypePtr & type) { return castColumn(arguments[index], type)->convertToFullColumnIfConst(); };

        const auto scale_column = cast(0, int64_type);
        const auto zero_count_column = cast(1, uint64_type);
        const auto positive_offset_column = cast(2, int64_type);
        const auto positive_counts_column = cast(3, counts_type);
        const auto negative_offset_column = cast(4, int64_type);
        const auto negative_counts_column = cast(5, counts_type);

        const auto & scales = assert_cast<const ColumnInt64 &>(*scale_column).getData();
        const auto & zero_counts = assert_cast<const ColumnUInt64 &>(*zero_count_column).getData();
        const auto & positive_offsets = assert_cast<const ColumnInt64 &>(*positive_offset_column).getData();
        const auto & positive_counts = assert_cast<const ColumnArray &>(*positive_counts_column);
        const auto & negative_offsets = assert_cast<const ColumnInt64 &>(*negative_offset_column).getData();
        const auto & negative_counts = assert_cast<const ColumnArray &>(*negative_counts_column);

        const AggregateFunctionPtr function = assert_cast<const DataTypeAggregateFunction &>(*result_type).getFunction();
        /// The places are cast to `QuantileDD<Float64>` below
        if (!getSketchFunctionFor<Float64, true>(*function)) [[unlikely]]
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Function {} got aggregate function {} instead of `quantileDD`", getName(), function->getName());
        auto result = ColumnAggregateFunction::create(function);
        result->reserve(input_rows_count);

        std::array<std::optional<DDSketchLogarithmicMapping>, max_supported_scale - min_supported_scale + 1> mappings;
        const auto get_mapping = [&](Int64 sketch_scale) -> const DDSketchLogarithmicMapping &
        {
            if (sketch_scale < min_supported_scale) [[unlikely]]
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} lowered the scale below {}", getName(), min_supported_scale);
            auto & mapping = mappings[sketch_scale - min_supported_scale];
            if (!mapping)
                mapping = DDSketchLogarithmicMapping::ofScale(sketch_scale);
            return *mapping;
        };

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Int64 scale = scales[row];
            if (scale < min_supported_scale || scale > max_input_scale) [[unlikely]]
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Function {} supports scales from {} to {}, but the scale is {}",
                    getName(),
                    min_supported_scale,
                    max_input_scale,
                    scale);
            const Buckets positive = getBuckets(positive_offsets[row], positive_counts, row, scale);
            const Buckets negative = getBuckets(negative_offsets[row], negative_counts, row, scale);

            Int64 sketch_scale = std::min(scale, max_supported_scale);
            /// Lower the scale until the buckets fit, as `merge` does. Scale 5 always fits: its bins hold all finite numbers
            while (!fitsIntoState(positive, scale - sketch_scale, get_mapping(sketch_scale))
                   || !fitsIntoState(negative, scale - sketch_scale, get_mapping(sketch_scale)))
                --sketch_scale;
            const DDSketchLogarithmicMapping & mapping = get_mapping(sketch_scale);

            result->insertDefault();
            DDSketchDenseLogarithmic & sketch = reinterpret_cast<QuantileDD<Float64> *>(result->getData().back())->getSketch();
            sketch.reset(mapping);

            addBuckets(sketch, false, positive, scale - sketch_scale, mapping);
            addBuckets(sketch, true, negative, scale - sketch_scale, mapping);
            sketch.addZeroCount(static_cast<Float64>(zero_counts[row]));
        }

        return result;
    }

private:
    static AggregateFunctionPtr getAggregateFunction(Float64 relative_accuracy)
    {
        AggregateFunctionProperties properties;
        return AggregateFunctionFactory::instance().get(
            NameQuantileDD::name, NullsAction::EMPTY, {std::make_shared<DataTypeFloat64>()}, Array{relative_accuracy}, properties);
    }

    /// The buckets of a row from the lowest to the highest nonempty one
    struct Buckets
    {
        /// The index of `counts[0]`
        Int64 offset;
        std::span<const UInt64> counts;
    };

    Buckets getBuckets(Int64 offset, const ColumnArray & counts, size_t row, Int64 scale) const
    {
        const auto & data = assert_cast<const ColumnUInt64 &>(counts.getData()).getData();
        const auto & offsets = counts.getOffsets();
        const size_t row_begin = offsets[row - 1];
        /// Trim the empty buckets at the ends: their indexes are not checked
        size_t begin = row_begin;
        size_t end = offsets[row];
        while (begin < end && data[begin] == 0)
            ++begin;
        while (end > begin && data[end - 1] == 0)
            --end;
        if (begin == end)
            return {0, {}};

        Int64 min_nonempty = 0;
        Int64 max_nonempty = 0;
        if (common::addOverflow(offset, static_cast<Int64>(begin - row_begin), min_nonempty)
            || common::addOverflow(offset, static_cast<Int64>(end - 1 - row_begin), max_nonempty)) [[unlikely]]
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket index overflow in function {}", getName());

        /// The buckets that hold finite numbers: from the bucket of the smallest subnormal number 2⁻¹⁰⁷⁴ to the bucket of the
        /// largest finite number, below 2¹⁰²⁴. The products are exact
        const Float64 buckets_per_octave = std::exp2(static_cast<Float64>(scale));
        const Int64 min_index = static_cast<Int64>(std::ceil(-1074.0 * buckets_per_octave)) - 1;
        const Int64 max_index = static_cast<Int64>(std::ceil(1024.0 * buckets_per_octave)) - 1;
        for (const Int64 index : {min_nonempty, max_nonempty})
            if (index < min_index || index > max_index) [[unlikely]]
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Bucket {} of scale {} passed to function {} does not hold finite numbers: they are in buckets from {} to {}",
                    index,
                    scale,
                    getName(),
                    min_index,
                    max_index);
        return {min_nonempty, std::span<const UInt64>(data.data() + begin, end - begin)};
    }

    /// Whether the bins of `addBuckets` fit into a state
    static bool fitsIntoState(const Buckets & buckets, Int64 shift, const DDSketchLogarithmicMapping & mapping)
    {
        if (buckets.counts.empty())
            return true;
        const Int64 max_index = buckets.offset + static_cast<Int64>(buckets.counts.size()) - 1;
        const Int64 min_key = mapping.downscaledKey(buckets.offset, shift);
        const Int64 max_key = mapping.downscaledKey(max_index, shift);
        return max_key - min_key + 1 <= DDSketchDenseStore::max_key_range;
    }

    /// Bucket `i` goes to bin `i >> shift`, clamped to the valid keys: e.g. the buckets of subnormal numbers go into the lowest
    /// bin. `getBuckets` has checked the indexes
    static void addBuckets(
        DDSketchDenseLogarithmic & sketch, bool negative, const Buckets & buckets, Int64 shift, const DDSketchLogarithmicMapping & mapping)
    {
        for (size_t i = 0; i < buckets.counts.size(); ++i)
        {
            if (buckets.counts[i] == 0)
                continue;
            const Int64 index = buckets.offset + static_cast<Int64>(i);
            sketch.addToBin(negative, mapping.downscaledKey(index, shift), static_cast<Float64>(buckets.counts[i]));
        }
    }
};


class FunctionToExpHistogram : public IFunction
{
public:
    static constexpr auto name = "toExpHistogram";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToExpHistogram>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2) [[unlikely]]
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments: state[, scale], but {} were passed",
                getName(),
                arguments.size());

        const auto * state_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!state_type || !getSketchFunction(*state_type)) [[unlikely]]
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} must be a state of `quantileDD` or `quantilesDD` over a non-`Nullable` "
                "argument, but it has type {}",
                getName(),
                arguments[0]->getName());

        if (arguments.size() == 2 && !isSignedIntegerArgument(*arguments[1])) [[unlikely]]
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The second argument (scale) of function {} must be an integer, but it has type {}",
                getName(),
                arguments[1]->getName());

        const auto counts_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        return std::make_shared<DataTypeTuple>(
            DataTypes{
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeInt32>(),
                counts_type,
                std::make_shared<DataTypeInt32>(),
                counts_type},
            Strings{"count", "scale", "zero_count", "positive_offset", "positive_bucket_counts", "negative_offset", "negative_bucket_counts"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto state_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto & states = assert_cast<const ColumnAggregateFunction &>(*state_holder).getData();
        const auto & state_type = assert_cast<const DataTypeAggregateFunction &>(*arguments[0].type);
        const GetSketch get_sketch = getSketchFunction(state_type);

        ColumnPtr target_scale_column;
        if (arguments.size() == 2)
            target_scale_column = castColumn(arguments[1], std::make_shared<DataTypeInt64>())->convertToFullColumnIfConst();

        auto count_column = ColumnUInt64::create();
        auto scale_column = ColumnInt32::create();
        auto zero_count_column = ColumnUInt64::create();
        auto positive_offset_column = ColumnInt32::create();
        auto positive_counts_column = ColumnArray::create(ColumnUInt64::create());
        auto negative_offset_column = ColumnInt32::create();
        auto negative_counts_column = ColumnArray::create(ColumnUInt64::create());

        BucketBuffers buffers;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const DDSketchDenseLogarithmic & sketch = get_sketch(states[row]);

            const StateScale state_scale = getStateScale(sketch.getGamma());
            /// A sketch from another producer can be finer than the finest scale of OpenTelemetry
            const Int64 finest_scale = std::min(state_scale.scale, max_input_scale);
            Int64 scale = finest_scale;
            if (target_scale_column)
            {
                scale = assert_cast<const ColumnInt64 &>(*target_scale_column).getData()[row];
                if (scale > finest_scale) [[unlikely]]
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Scale {} passed to function {} is finer than the state supports, the finest scale for it is {}",
                        scale,
                        getName(),
                        finest_scale);
            }
            if (scale < min_supported_scale) [[unlikely]]
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Function {} supports scales from {}, but the scale is {}",
                    getName(),
                    min_supported_scale,
                    scale);

            const UInt64 zero_count = roundCount(sketch.getZeroCount());
            zero_count_column->insertValue(zero_count);
            UInt64 count = zero_count;
            writeBuckets(sketch, false, state_scale, scale, count, buffers, *positive_offset_column, *positive_counts_column);
            writeBuckets(sketch, true, state_scale, scale, count, buffers, *negative_offset_column, *negative_counts_column);
            count_column->insertValue(count);
            scale_column->insertValue(static_cast<Int32>(scale));
        }

        return ColumnTuple::create(Columns{
            std::move(count_column),
            std::move(scale_column),
            std::move(zero_count_column),
            std::move(positive_offset_column),
            std::move(positive_counts_column),
            std::move(negative_offset_column),
            std::move(negative_counts_column)});
    }

private:
    struct StateScale
    {
        Int64 scale;
        /// Every bin `k` is bucket `k` of `scale`
        bool exact;
    };

    /// The finest scale whose buckets are at least as wide as the bins of `gamma`. It is exact if `gamma` is the gamma of a scale
    /// (`scaleOfGamma`), e.g. in a state from `fromExpHistogram`. Otherwise each bucket is 1 to 2 bins wide
    static StateScale getStateScale(Float64 gamma)
    {
        if (const auto scale = DDSketchLogarithmicMapping::scaleOfGamma(gamma))
            return {*scale, true};
        return {static_cast<Int64>(std::floor(-std::log2(std::log2(gamma)))), false};
    }

    static UInt64 roundCount(Float64 value)
    {
        const Float64 rounded = std::round(value);
        /// 2⁶⁴
        if (!(rounded >= 0 && rounded < 18446744073709551616.0)) [[unlikely]]
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DDSketch count {} does not fit into UInt64", value);
        return static_cast<UInt64>(rounded);
    }

    /// For `writeBuckets`, reused between the rows
    struct BucketBuffers
    {
        PODArray<Float64> bucket_counts;
        PODArray<UInt64> rounded;
    };

    void writeBuckets(
        const DDSketchDenseLogarithmic & sketch,
        bool negative,
        const StateScale & state_scale,
        Int64 scale,
        UInt64 & total_count,
        BucketBuffers & buffers,
        ColumnInt32 & offset_column,
        ColumnArray & counts_column) const
    {
        const Float64 log2_gamma = std::log2(sketch.getGamma());
        const Float64 scale_factor = std::exp2(static_cast<Float64>(scale));

        /// Bins come in increasing key order, so bucket indexes do not decrease
        Int64 first_index = 0;
        auto & bucket_counts = buffers.bucket_counts;
        bucket_counts.clear();
        sketch.forEachBin(
            negative,
            [&](int key, Float64 bin_count)
            {
                /// Exact: downscaling by `d` merges 2ᵈ buckets, the same as `ceil((key + 0.5) / 2^d) - 1`. Otherwise the whole bin
                /// goes to the bucket that holds its middle value γ^(key + ½)
                const Int64 index = state_scale.exact
                    ? static_cast<Int64>(key) >> (state_scale.scale - scale)
                    : static_cast<Int64>(std::ceil((static_cast<Float64>(key) + 0.5) * log2_gamma * scale_factor)) - 1;

                if (bucket_counts.empty())
                    first_index = index;
                const size_t position = static_cast<size_t>(index - first_index);
                if (position >= bucket_counts.size())
                    bucket_counts.resize_fill(position + 1, 0.0);
                bucket_counts[position] += bin_count;
            });

        auto & rounded = buffers.rounded;
        rounded.resize(bucket_counts.size());
        for (size_t i = 0; i < bucket_counts.size(); ++i)
        {
            rounded[i] = roundCount(bucket_counts[i]);
            if (common::addOverflow(total_count, rounded[i], total_count)) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Total count overflows UInt64 in function {}", getName());
        }

        /// Fractional counts (after a merge of sketches with different gammas) can round to zero at the edges
        size_t begin = 0;
        size_t end = rounded.size();
        while (begin < end && rounded[begin] == 0)
            ++begin;
        while (end > begin && rounded[end - 1] == 0)
            --end;

        const Int64 offset = begin < end ? first_index + static_cast<Int64>(begin) : 0;
        if (offset < std::numeric_limits<Int32>::min() || offset > std::numeric_limits<Int32>::max()) [[unlikely]]
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket offset {} does not fit into Int32 in function {}", offset, getName());
        offset_column.insertValue(static_cast<Int32>(offset));

        auto & data = assert_cast<ColumnUInt64 &>(counts_column.getData()).getData();
        data.insert(rounded.begin() + begin, rounded.begin() + end);
        counts_column.getOffsets().push_back(data.size());
    }
};

}

REGISTER_FUNCTION(ExpHistogram)
{
    FunctionDocumentation::Description description_from = R"DOCS_MD(
Converts an [OpenTelemetry exponential histogram](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram)
data point into a state of the [`quantileDD`](/reference/functions/aggregate-functions/quantileddsketch) aggregate function.

```text
OpenTelemetry Collector                      ClickHouse
┌───────────────────────┐  fromExpHistogram  ┌──────────────────────┐  quantileDDMerge  ┌───────────┐
│ exponential histogram ├───────────────────►│ state of quantileDD  ├──────────────────►│ quantiles │
│ data point            │◄───────────────────┤ AggregatingMergeTree │                   └───────────┘
└───────────────────────┘   toExpHistogram   └──────────────────────┘
```

The arguments match the columns that the OpenTelemetry Collector ClickHouse exporter writes into the exponential histogram
table: `Scale`, `ZeroCount`, `PositiveOffset`, `PositiveBucketCounts`, `NegativeOffset`, `NegativeBucketCounts`.
Merge the states with `quantileDDMerge` or `quantilesDDMerge` to compute quantiles over many data points.

A cumulative data point (`AggregationTemporality = 2`) holds all values since the start of its series. So, for
cumulative data, merge only the latest data point of each series. Delta data points (`AggregationTemporality = 1`) can
be merged over any time range.

Bucket i of an exponential histogram with scale s holds the values in (γⁱ, γⁱ⁺¹], where γ = 2^(2⁻ˢ).
The state uses the same bins, so the counts stay in their buckets, except in the cases below. The buckets of
`fromExpHistogram(3, 1, -2, [1, 2, 3], 5, [4])`:

```text
scale = 3:  γ = 2^(2⁻³) = ⁸√2 ≈ 1.0905

      x < 0                  x = 0                          x > 0
   ┌──────────┐             ┌─────┐           ┌──────────┬──────────┬──────────┐
   │    4     │             │  1  │           │    1     │    2     │    3     │
───┴──────────┴──── ··· ────┴─────┴─── ··· ───┴──────────┴──────────┴──────────┴────────►
  -γ⁶        -γ⁵               0              γ⁻²        γ⁻¹        1          γ
      i = 5                                      i = -2     i = -1      i = 0

negative_offset = 5,  negative_bucket_counts = [4]        bucket i: γⁱ < |x| ≤ γⁱ⁺¹, x < 0
positive_offset = -2, positive_bucket_counts = [1, 2, 3]  bucket i: γⁱ < x ≤ γⁱ⁺¹
```

The buckets of the first example below, `fromExpHistogram(1, 0, 0, [1, 2, 3, 4], 0, [])`:

```text
scale = 1:  γ = 2^(2⁻¹) = √2

                                         █████████
                               █████████ █████████
                     █████████ █████████ █████████
           █████████ █████████ █████████ █████████
          ├─────────┼─────────┼─────────┼─────────┤
          1         γ = √2    γ² = 2    γ³ = 2√2  γ⁴ = 4
             i = 0     i = 1     i = 2     i = 3    bucket i: γⁱ < x ≤ γⁱ⁺¹
```

Scales from -5 to 20 are supported. Scales 19 and 20 are downscaled to 18 (2 or 4 adjacent buckets merge into one),
because `quantileDD` does not support the relative accuracy of finer scales. Each scale lower squares γ, so its
buckets are made of whole buckets of the finer scales:

```text
              ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
scale s       │  -4   │  -3   │  -2   │  -1   │   0   │   1   │   2   │   3   │
              ├───────┴───────┼───────┴───────┼───────┴───────┼───────┴───────┤
scale s - 1   │      -2       │      -1       │       0       │       1       │
              ├───────────────┴───────────────┼───────────────┴───────────────┤
scale s - 2   │              -1               │               0               │
              └───────────────────────────────┴───────────────────────────────┘
              γ⁻⁴             γ⁻²             1               γ²              γ⁴

Scale s - d: γ^(2ᵈ) instead of γ, bucket i becomes bucket ⌊i / 2ᵈ⌋
```

`zero_count` is added as a count of exact zeros. In OpenTelemetry, the zero bucket can also hold small values, with |x|
up to `zero_threshold`. This function has no argument for `zero_threshold`, and the OpenTelemetry Collector ClickHouse
exporter does not store it. So quantiles that fall into the zero bucket are 0, and can be off by up to `zero_threshold`.

The bins of the state cover numbers from about 2⁻¹⁰²² to 2¹⁰²⁴. A bucket outside of them goes into the nearest bin. This
happens to the buckets of subnormal numbers, and at some scales to the bucket of the largest numbers. The counts are
kept, but the quantiles that fall there have the value of that bin.

A non-empty bucket with no finite numbers is an error, for example bucket 1024 of scale 0, which is (2¹⁰²⁴, 2¹⁰²⁵].
Empty buckets are ignored. Negative values follow the same rules, using |x|.

Data points of different scales can be merged. The result has the coarsest of their scales, but not finer than the
relative accuracy of the merge allows: relative accuracy `0.01` keeps the scales up to 5, `0.0001` up to 11, and
`0.000001` all supported scales. Data points with no values outside the zero bucket do not lower the scale.

```text
scale              -5         0         5          11            18  20
                    ├─┬─┬─┬─┬─┼─┬─┬─┬─┬─┼─┬─┬─┬─┬─┬─┼─┬─┬─┬─┬─┬─┬─┼─┬─┤
relative_accuracy
  0.01              █████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
  0.0001            █████████████████████████████████░░░░░░░░░░░░░░░░░░
  0.000001          ███████████████████████████████████████████████░░░░
█ kept as is   ░ downscaled exactly to the last █ (scales 19 and 20 always to 18)
```

Functions with the [`-Merge`](/reference/functions/aggregate-functions/combinators#-merge) combinator, such as
`quantileDDMerge(relative_accuracy, level)`, merge at the relative accuracy of their parameter. All other merges use the
relative accuracy of the type: the `relative_accuracy` argument of this function, `0.000001` by default. They include
the background merges of `AggregatingMergeTree` and copies of states, such as in `if`, `finalizeAggregation` or inserts
into `AggregatingMergeTree`. This function itself does not use the relative accuracy of the type: the state keeps the
scale of the data point until it is copied or merged. No merge makes the bins finer.

A state holds at most 65536 bins for positive values and 65536 for negative ones. When the values do not fit, every 2ᵈ
adjacent bins merge into one, as when OpenTelemetry SDKs downscale a histogram, and the scale goes down by d. This
happens in this function, in `quantileDD` over values, and in merges. So nothing fails, however far apart the values
are.

At scale s, values of one sign fit only when max / min < γ⁶⁵⁵³⁶ = 2^(65536 · 2⁻ˢ). This is about 1.19 at scale 18 and
2⁶⁴ at scale 10. At scale 5, all finite numbers fit. With the default relative accuracy, the scale goes down only when
the values do not fit.

```text
scale s       ├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤ 65536 bins
                 ●   ● ●   ● ●   ●   ●                               ● ◄── does not fit

scale s - 1   ├───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┼───┤ 65536 bins
                 ●   ● ●   ● ●   ●   ●                               ● ◄── fits
Every 2 adjacent bins merge into one.
```
    )DOCS_MD";
    FunctionDocumentation::Syntax syntax_from = "fromExpHistogram(scale, zero_count, positive_offset, positive_bucket_counts, negative_offset, negative_bucket_counts[, relative_accuracy])";
    FunctionDocumentation::Arguments arguments_from = {
        {"scale", "Scale of the histogram, from -5 to 20.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"}},
        {"zero_count", "Count of the zero bucket.", {"UInt*"}},
        {"positive_offset", "Index of the first bucket of positive values.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"}},
        {"positive_bucket_counts", "Counts of the buckets of positive values, starting from bucket `positive_offset`.", {"Array(UInt*)"}},
        {"negative_offset", "Index of the first bucket of negative values (by absolute value).", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"}},
        {"negative_bucket_counts", "Counts of the buckets of negative values, starting from bucket `negative_offset`.", {"Array(UInt*)"}},
        {"relative_accuracy",
         "Optional. Constant relative accuracy parameter of the returned type: the resolution of the merges that the type does "
         "by itself, such as in `AggregatingMergeTree`. Default value: `0.000001`, which keeps all supported scales.",
         {"Float64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_from = {"State of `quantileDD`.", {"AggregateFunction(quantileDD(relative_accuracy), Float64)"}};
    FunctionDocumentation::Examples examples_from = {
        {"Quantiles of a histogram",
         R"(
SELECT quantilesDDMerge(0.0001, 0.1, 0.5, 0.9)(fromExpHistogram(1, 0, 0, [1, 2, 3, 4], 0, []));
        )",
         R"(
┌─quantilesDDMerge(0.0001, 0.⋯, 0, [1, 2, 3, 4], 0, []))─┐
│ [1.17157287525381,2.34314575050762,3.3137084989847607] │
└────────────────────────────────────────────────────────┘
        )"},
        {"Quantiles of the data exported by the OpenTelemetry Collector",
         R"(
SELECT quantileDDMerge(0.0001, 0.99)(fromExpHistogram(Scale, ZeroCount, PositiveOffset, PositiveBucketCounts, NegativeOffset, NegativeBucketCounts))
FROM otel_metrics_exponential_histogram
WHERE MetricName = 'http.server.request.duration' AND AggregationTemporality = 1;
        )",
         ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_from
        = {description_from, syntax_from, arguments_from, {}, returned_value_from, examples_from, introduced_in, category};
    factory.registerFunction<FunctionFromExpHistogram>(documentation_from);

    FunctionDocumentation::Description description_to = R"DOCS_MD(
Converts a state of the [`quantileDD`](/reference/functions/aggregate-functions/quantileddsketch) aggregate function into
an [OpenTelemetry exponential histogram](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram)
data point. It is the opposite of [`fromExpHistogram`](#fromExpHistogram).

If the state came from `fromExpHistogram`, also after merges with other such states, the conversion is exact, except
for the buckets that `fromExpHistogram` moved into the nearest bin. The scale of the result can be lower than that of
the data points: when the values were too far apart for the bins of the state, or the merges had a coarser relative
accuracy (see `fromExpHistogram`). Otherwise, as for a state of `quantileDD` over values, the bins of the sketch do not
match the buckets of any scale: the result has the finest scale whose buckets are not narrower than the bins (scale 5
for `relative_accuracy = 0.01`), and every bin [γᵏ, γᵏ⁺¹) of the sketch goes into the bucket that holds its middle
value γ^(k + ½):

```text
bins of the state     ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤  bin k: [γᵏ, γᵏ⁺¹)
middle values            ●     ●     ●     ●     ●     ●     ●     ●     γ^(k + ½)
                         │     │     │     │     │     │     │     │
                         ▼     ▼     ▼     ▼     ▼     ▼     ▼     ▼
buckets of scale s    ├───────┼───────┼───────┼───────┼───────┼───────┤
bins in the bucket        1       2       1       1       2       1
```

The scale of the result is at most 20, the maximum scale of OpenTelemetry. Counts are rounded to integers. The result
has no sum, minimum or maximum, because the state does not keep them.
    )DOCS_MD";
    FunctionDocumentation::Syntax syntax_to = "toExpHistogram(state[, scale])";
    FunctionDocumentation::Arguments arguments_to = {
        {"state", "State of `quantileDD` or `quantilesDD`.", {"AggregateFunction(quantileDD(relative_accuracy), T)"}},
        {"scale", "Optional. Scale of the result, from -5 to the scale of the state.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_to = {
        "Tuple with the fields `count`, `scale`, `zero_count`, `positive_offset`, `positive_bucket_counts`, `negative_offset`, "
        "`negative_bucket_counts`.",
        {"Tuple(count UInt64, scale Int32, zero_count UInt64, positive_offset Int32, positive_bucket_counts Array(UInt64), negative_offset Int32, negative_bucket_counts Array(UInt64))"}};
    FunctionDocumentation::Examples examples_to = {
        {"Histogram of a sample at scale 1",
         "SELECT toExpHistogram(quantileDDState(0.01)(number), 1) FROM numbers(10);",
         R"(
┌─toExpHistogram(quantileDDState(0.01)(number), 1)─────────────────────────────────────────────────────────────────┐
├─count─┬─scale─┬─zero_count─┬─positive_offset─┬─positive_bucket_counts─┬─negative_offset─┬─negative_bucket_counts─┤
│    10 │     1 │          1 │               0 │ [1,1,0,1,2,3,1]        │               0 │ []                     │
└───────┴───────┴────────────┴─────────────────┴────────────────────────┴─────────────────┴────────────────────────┘
        )"},
        {"Round trip",
         "SELECT toExpHistogram(fromExpHistogram(3, 1, -2, [1, 2, 3], 5, [4]));",
         R"(
┌─toExpHistogram(fromExpHistogram(3, 1, -2, [1, 2, 3], 5, [4]))────────────────────────────────────────────────────┐
├─count─┬─scale─┬─zero_count─┬─positive_offset─┬─positive_bucket_counts─┬─negative_offset─┬─negative_bucket_counts─┤
│    11 │     3 │          1 │              -2 │ [1,2,3]                │               5 │ [4]                    │
└───────┴───────┴────────────┴─────────────────┴────────────────────────┴─────────────────┴────────────────────────┘
        )"}};
    FunctionDocumentation documentation_to
        = {description_to, syntax_to, arguments_to, {}, returned_value_to, examples_to, introduced_in, category};
    factory.registerFunction<FunctionToExpHistogram>(documentation_to);
}

}
