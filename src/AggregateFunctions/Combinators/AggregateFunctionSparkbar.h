#pragma once

#include <array>
#include <string_view>

#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/memory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/** Aggregate function combinator -Sparkbar applies another aggregate function independently
  * to each bucket determined by the first (x-axis) argument, and renders the per-bucket
  * results as a Unicode sparkbar string.
  *
  * Usage:
  *   sumSparkbar(width, begin_x, end_x)(x_col, value_col)
  *   countSparkbar(width, begin_x, end_x)(x_col)
  *   uniqSparkbar(width, begin_x, end_x)(x_col, key_col)
  *
  * The first argument is the x-axis (bucket key) column; remaining arguments are forwarded
  * to the nested aggregate function.
  *
  * The explicit range [begin_x, end_x] is always required. Without it, bucketing cannot be
  * performed at aggregation time because the range is not known until all data is seen,
  * and the per-bucket states are irreversible once accumulated.
  */
template <typename Key>
class AggregateFunctionSparkbar final
    : public IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>
{
private:
    static constexpr size_t BAR_LEVELS = 8;
    static constexpr size_t MAX_WIDTH  = 1024;

    AggregateFunctionPtr nested_function;

    size_t width;
    Key    begin_x;
    Key    end_x;

    /// Multiplier applied to each key read from the column before bucketing. It is always 1
    /// except for DateTime64 columns whose scale is coarser than the working scale of
    /// `begin_x`/`end_x`, where keys are rescaled up so that all three are in the same unit.
    Key    key_multiplier;

    size_t align_of_data;
    size_t size_of_data;

    /// Compute the bar level of each bucket from the nested function results, keeping the
    /// scaling arithmetic in the native domain of the result type. Integer and Decimal results
    /// are scaled with exact integer arithmetic (the same way as the `sparkbar` aggregate
    /// function does), because squeezing them through Float64 would merge distinct values
    /// beyond the 53-bit exact range into the same bar. For Decimal the scale factor cancels
    /// out in the value / maximum ratio, so the raw underlying integer values are used.
    /// A bucket's level stays 0 (rendered blank) when its result is NULL, NaN, or not positive.
    template <typename T>
    void computeLevels(
        const typename ColumnVectorOrDecimal<T>::Container & data,
        const NullMap * null_map,
        PaddedPODArray<UInt8> & levels) const
    {
        const auto value_of = [&](size_t i)
        {
            if constexpr (is_decimal<T>)
                return data[i].value;
            else
                return data[i];
        };
        using V = decltype(value_of(0));

        V y_max{};
        for (size_t i = 0; i < width; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;
            const V v = value_of(i);
            if (isNaN(v) || v <= V{})
                continue;
            y_max = std::max(y_max, v);
        }

        if (y_max == V{})
            return;

        for (size_t i = 0; i < width; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;
            const V v = value_of(i);
            if (isNaN(v) || v <= V{})
                continue;

            if constexpr (is_floating_point<V>)
            {
                /// Widening to Float64 is exact for Float32 and BFloat16. v == y_max maps to
                /// exactly BAR_LEVELS. An infinite maximum makes the ratio NaN (for the
                /// infinite bucket) or 0 (for finite buckets), so the guard below keeps the
                /// infinite bucket blank instead of casting NaN to an out-of-range index.
                const Float64 scaled
                    = static_cast<Float64>(v) / static_cast<Float64>(y_max) * static_cast<Float64>(BAR_LEVELS - 1) + 1;
                if (!std::isnan(scaled) && scaled >= 1 && scaled <= BAR_LEVELS)
                    levels[i] = static_cast<UInt8>(scaled);
            }
            else
            {
                const V levels_num = V{BAR_LEVELS - 1};
                V level{};
                V scaled{};
                if (common::mulOverflow(v, levels_num, scaled))
                    /// v * (BAR_LEVELS - 1) overflowed, which implies y_max >= v is at least
                    /// max<V> / (BAR_LEVELS - 1), so y_max / levels_num is never zero here.
                    level = v / (y_max / levels_num) + V{1};
                else
                    level = scaled / y_max + V{1};
                levels[i] = level > V{BAR_LEVELS} ? UInt8{BAR_LEVELS} : static_cast<UInt8>(level);
            }
        }
    }

    /// Render one Unicode bar per bucket from precomputed bar levels:
    /// 0 renders as a blank, 1..BAR_LEVELS as increasing bar heights.
    void render(ColumnString & to_column, const PaddedPODArray<UInt8> & levels) const
    {
        static constexpr std::array<std::string_view, BAR_LEVELS + 1> bars{" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};

        auto & chars   = to_column.getChars();
        auto & offsets = to_column.getOffsets();

        /// If no bucket has a positive value, the result is an empty string, not `width` blanks.
        bool has_data = false;
        for (UInt8 level : levels)
            has_data |= level > 0;

        if (has_data)
        {
            for (UInt8 level : levels)
            {
                const auto & bar = bars[level];
                chars.insert(bar.begin(), bar.end());
            }
        }

        offsets.push_back(chars.size());
    }

public:
    AggregateFunctionSparkbar(
        AggregateFunctionPtr nested_function_,
        size_t width_,
        Key begin_x_,
        Key end_x_,
        Key key_multiplier_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>{arguments, params, std::make_shared<DataTypeString>()}
        , nested_function{nested_function_}
        , width{width_}
        , begin_x{begin_x_}
        , end_x{end_x_}
        , key_multiplier{key_multiplier_}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{(nested_function->sizeOfData() + align_of_data - 1) / align_of_data * align_of_data}
    {
        if (width < 2 || width > MAX_WIDTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter width for aggregate function {} must be in range [2, {}]",
                getName(), MAX_WIDTH);

        if (begin_x >= end_x)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter begin_x must be strictly less than end_x for aggregate function {}",
                getName());
    }

    String getName() const override
    {
        return nested_function->getName() + "Sparkbar";
    }

    bool isState() const override { return nested_function->isState(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return width * size_of_data; }

    size_t alignOfData() const override { return align_of_data; }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < width; ++i)
        {
            try
            {
                nested_function->create(place + i * size_of_data);
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(place + j * size_of_data);
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroy(place + i * size_of_data);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroyUpToState(place + i * size_of_data);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// Always read via getInt: ColumnDecimal (DateTime64) implements getInt but not getUInt.
        /// For unsigned Key types the static_cast is safe because valid x-axis values fit in
        /// the chosen unsigned type (UInt64 for Date/DateTime/DateTime64/UInt*, Int32/Int64 for
        /// the signed branches).
        Key key = static_cast<Key>(columns[0]->getInt(row_num));

        /// Rescale the key up to the working scale of begin_x/end_x (DateTime64 with a coarser
        /// column scale). If the rescaled key overflows the Key type it cannot lie inside the
        /// representable [begin_x, end_x] range, so it is safely skipped.
        if (key_multiplier != 1)
        {
            Key scaled = 0;
            if (common::mulOverflow(key, key_multiplier, scaled))
                return;
            key = scaled;
        }

        if (key < begin_x || key > end_x)
            return;

        /// Compute the bucket index with exact integer arithmetic. The bounds check above
        /// guarantees begin_x <= key <= end_x and the constructor guarantees begin_x < end_x,
        /// so both unsigned deltas are non-negative and range is positive. Using Float64 here
        /// would lose precision once range or offset exceeds the 53-bit exact integer range,
        /// silently mis-bucketing valid UInt64/Int64/DateTime64 keys near a bucket boundary.
        const UInt64 range  = static_cast<UInt64>(end_x) - static_cast<UInt64>(begin_x);
        const UInt64 offset = static_cast<UInt64>(key)   - static_cast<UInt64>(begin_x);
        /// offset * width can exceed 64 bits (offset up to ~1.8e19, width up to MAX_WIDTH), so the
        /// product is taken in 128 bits. Since offset <= range, the quotient is at most `width`
        /// (reached only when key == end_x), and it is clamped to the last bucket.
        const size_t pos = std::min<UInt64>(
            static_cast<UInt64>(static_cast<__uint128_t>(offset) * width / range),
            static_cast<UInt64>(width) - 1);

        /// First argument (columns[0]) is the x-axis key; the rest go to the nested function.
        nested_function->add(place + pos * size_of_data, columns + 1, row_num, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->merge(place + i * size_of_data, rhs + i * size_of_data, arena);
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (!this->haveSameDefinition(rhs))
            return false;

        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        return nested_function->canMergeStateFromDifferentVariant(*rhs_nested);
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override
    {
        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        const size_t rhs_align_of_data = rhs_nested->alignOfData();
        const size_t rhs_size_of_data = ::Memory::alignUp(rhs_nested->sizeOfData(), rhs_align_of_data);

        for (size_t i = 0; i < width; ++i)
            nested_function->mergeStateFromDifferentVariant(place + i * size_of_data, *rhs_nested, rhs_place + i * rhs_size_of_data, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->serialize(place + i * size_of_data, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->deserialize(place + i * size_of_data, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        /// Collect the nested function result for each bucket into a temporary column.
        auto result_col = nested_function->getResultType()->createColumn();
        result_col->reserve(width);

        for (size_t i = 0; i < width; ++i)
            nested_function->insertResultInto(place + i * size_of_data, *result_col, arena);

        const IColumn * data_col = result_col.get();
        const NullMap * null_map = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_col))
        {
            null_map = &nullable->getNullMapData();
            data_col = &nullable->getNestedColumn();
        }

        /// Bar level of each bucket: 0 renders as a blank, 1..BAR_LEVELS as increasing heights.
        PaddedPODArray<UInt8> levels(width, 0);

        /// Dispatch by the concrete result type: the combinator accepts every type for which
        /// WhichDataType::isNumber holds, and each of them is stored either in a ColumnVector
        /// or in a ColumnDecimal.
        const auto dispatch = [&]<typename T>(T) -> bool
        {
            const auto * col = checkAndGetColumn<ColumnVectorOrDecimal<T>>(data_col);
            if (!col)
                return false;
            computeLevels<T>(col->getData(), null_map, levels);
            return true;
        };

        const bool dispatched = dispatch(UInt8{}) || dispatch(UInt16{}) || dispatch(UInt32{}) || dispatch(UInt64{})
            || dispatch(UInt128{}) || dispatch(UInt256{})
            || dispatch(Int8{}) || dispatch(Int16{}) || dispatch(Int32{}) || dispatch(Int64{})
            || dispatch(Int128{}) || dispatch(Int256{})
            || dispatch(BFloat16{}) || dispatch(Float32{}) || dispatch(Float64{})
            || dispatch(Decimal32{}) || dispatch(Decimal64{}) || dispatch(Decimal128{}) || dispatch(Decimal256{});

        if (!dispatched)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Aggregate function {} got unexpected result column {} from the nested function",
                getName(), data_col->getName());

        render(assert_cast<ColumnString &>(to), levels);
    }

    UnorderedSetWithMemoryTracking<size_t> getArgumentsThatCanBeOnlyNull() const override
    {
        auto nested_arguments = nested_function->getArgumentsThatCanBeOnlyNull();
        UnorderedSetWithMemoryTracking<size_t> result;
        result.reserve(nested_arguments.size() + 1);
        result.insert(0);

        for (const size_t argument : nested_arguments)
            result.insert(argument + 1);

        return result;
    }

    /// The combinator always renders a `String`, whatever the nested function's null semantics
    /// are. Without an own adapter the generic `Null` combinator would consult the *nested*
    /// function's `returns_default_when_only_null` property (`avg`'s, for example, is false) and
    /// wrap the whole sparkbar into `Nullable(String)`, returning `NULL` for an all-`NULL` input
    /// instead of the empty sparkbar. Force the non-nullable adapter so the advertised result
    /// type does not depend on the nested function. `serialize_flag` stays `true`, so the state
    /// layout is the one the generic adapter would have produced.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function_,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        if (arguments.size() == 1)
            return std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function_, arguments, params);
        return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function_, arguments, params);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
