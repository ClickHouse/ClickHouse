#pragma once

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <optional>

#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// Varying-`phi` sibling of `timeSeriesQuantileToGrid`; same rationale and design as
/// AggregateFunctionTimeseriesPredictLinearVarying.h, just for `phi` instead of `predict_offset`.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesQuantileVarying final :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesQuantileVarying<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesQuantileVarying>;
    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    String getName() const override { return "timeSeriesQuantileVaryingToGrid"; }

    bool shouldPrintParametersWithTypes() const override { return true; }

    explicit AggregateFunctionTimeseriesQuantileVarying(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_)
        : Base(argument_types_, parameters_, createResultType())
        , array_arguments(argument_types_[1]->getTypeId() == TypeIndex::Array)
        , step(checkStep(start_timestamp_, end_timestamp_, step_))
        , window(checkWindow(window_))
        , grid_size(gridSize(start_timestamp_, end_timestamp_, step))
        , start_timestamp(start_timestamp_)
    {
    }

    bool allocatesMemoryInArena() const override { return false; }
    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<State>; }
    size_t alignOfData() const override { return alignof(State); }
    size_t sizeOfData() const override { return sizeof(State); }

    void create(AggregateDataPtr __restrict place) const override { new (place) State{}; } /// NOLINT
    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place)->~State(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        captureOrCheckGridPhis(place, columns[2], row_num);

        if (array_arguments)
        {
            const auto & timestamp_column = typeid_cast<const ColumnArray &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColumnArray &>(*columns[1]);
            const auto & timestamp_offsets = timestamp_column.getOffsets();
            const auto & value_offsets = value_column.getOffsets();
            const auto ts_begin = row_num == 0 ? 0 : timestamp_offsets[row_num - 1];
            const auto ts_end = timestamp_offsets[row_num];
            const auto val_begin = row_num == 0 ? 0 : value_offsets[row_num - 1];
            const auto val_end = value_offsets[row_num];
            if (ts_end - ts_begin != val_end - val_begin)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Timestamp and value arrays have different sizes at row {}: {} and {}", row_num, ts_end - ts_begin, val_end - val_begin);

            const auto & timestamp_data = typeid_cast<const ColumnVectorOrDecimal<TimestampType> &>(timestamp_column.getData()).getData();
            const auto & value_data = typeid_cast<const ColumnVectorOrDecimal<ValueType> &>(value_column.getData()).getData();
            for (size_t i = ts_begin; i < ts_end; ++i)
                data(place)->samples.add(timestamp_data[i], value_data[i]);
        }
        else
        {
            const auto & timestamp_column = typeid_cast<const ColumnVectorOrDecimal<TimestampType> &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColumnVectorOrDecimal<ValueType> &>(*columns[1]);
            data(place)->samples.add(timestamp_column.getData()[row_num], value_column.getData()[row_num]);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override
    {
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place)->samples.merge(data(rhs)->samples);

        if (!data(rhs)->grid_phis_captured)
            return;

        if (!data(place)->grid_phis_captured)
        {
            data(place)->grid_phis = data(rhs)->grid_phis;
            data(place)->grid_phis_captured = true;
            return;
        }

        /// States are merged in full here: unlike `add`, merging happens once per partial state, not once per sample.
        if (!sameArray(data(place)->grid_phis, data(rhs)->grid_phis))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot merge states of aggregate function {} created with different arguments: "
                "the `phis` array must be the same for all rows", getName());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        data(place)->samples.serialize(buf);
        writeBinaryLittleEndian(data(place)->grid_phis_captured, buf);
        if (data(place)->grid_phis_captured)
        {
            writeBinaryLittleEndian(static_cast<UInt64>(data(place)->grid_phis.size()), buf);
            for (const auto phi : data(place)->grid_phis)
                writeBinaryLittleEndian(phi, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        data(place)->samples.deserialize(buf);
        readBinaryLittleEndian(data(place)->grid_phis_captured, buf);
        if (data(place)->grid_phis_captured)
        {
            UInt64 size = 0;
            readBinaryLittleEndian(size, buf);
            if (size > MAX_GRID_SIZE)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more grid phis than expected");
            data(place)->grid_phis.resize(size);
            for (auto & phi : data(place)->grid_phis)
                readBinaryLittleEndian(phi, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? grid_size : offsets_to.back() + grid_size);

        if (!grid_size)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<ColumnVectorOrDecimal<ValueType> &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        data_to.resize(old_size + grid_size);
        nulls_to.resize(old_size + grid_size);

        const State * state = data(place);
        const auto & grid_phis = state->grid_phis;
        if (state->grid_phis_captured && grid_phis.size() != grid_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "timeSeriesQuantileVaryingToGrid: phis array has {} elements, expected {} (the grid size)",
                grid_phis.size(), grid_size);

        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> sorted_samples;
        state->samples.forEachSample([&sorted_samples](TimestampType timestamp, ValueType value)
        {
            sorted_samples.emplace_back(timestamp, value);
        });

        size_t window_begin = 0;
        size_t window_end = 0;

        for (size_t grid_index = 0; grid_index < grid_size; ++grid_index)
        {
            const TimestampType grid_timestamp = timestampAtIndex(grid_index);

            while (window_end < sorted_samples.size() && sorted_samples[window_end].first <= grid_timestamp)
                ++window_end;
            while (window_begin < window_end && isSampleOutOfWindow(sorted_samples[window_begin].first, grid_timestamp))
                ++window_begin;

            const Float64 phi = state->grid_phis_captured ? grid_phis[grid_index] : 0.0;

            VectorWithMemoryTracking<ValueType> window_values;
            window_values.reserve(window_end - window_begin);
            for (size_t i = window_begin; i < window_end; ++i)
                window_values.push_back(sorted_samples[i].second);

            /// Shares the R-7 interpolation with the constant-phi aggregate (AggregateFunctionTimeseriesQuantileToGrid.h);
            /// out-of-range/NaN phi edge cases are applied by the translator's wrapping arrayMap instead.
            std::optional<ValueType> result = computeTimeseriesQuantile(std::move(window_values), phi);

            if (result)
            {
                data_to[old_size + grid_index] = *result;
                nulls_to[old_size + grid_index] = 0;
            }
            else
            {
                data_to[old_size + grid_index] = ValueType{};
                nulls_to[old_size + grid_index] = 1;
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr bool DateTime64Supported = true;

private:
    static constexpr size_t MAX_GRID_SIZE = 0xFFFFFF;

    struct State
    {
        Samples samples;
        VectorWithMemoryTracking<Float64> grid_phis;
        bool grid_phis_captured = false;
    };

    static const State * data(ConstAggregateDataPtr __restrict place) { return reinterpret_cast<const State *>(place); }
    static State * data(AggregateDataPtr __restrict place) { return reinterpret_cast<State *>(place); }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ValueType>>()));
    }

    static IntervalType checkStep(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (start_timestamp == end_timestamp)
            return 0;
        if (step <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
        return step;
    }

    static IntervalType checkWindow(IntervalType window)
    {
        if (window < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window should be non-negative");
        return window;
    }

    static size_t gridSize(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (end_timestamp < start_timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");
        if (end_timestamp == start_timestamp)
            return 1;

        const Int128 quotient = (static_cast<Int128>(static_cast<Int64>(end_timestamp))
            - static_cast<Int128>(static_cast<Int64>(start_timestamp)))
            / static_cast<Int128>(static_cast<Int64>(step));

        if (quotient >= MAX_GRID_SIZE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Number of grid points in the timeseries grid exceeds maximum ({}). "
                "Consider narrowing the [start, end] range or increasing the step.",
                MAX_GRID_SIZE);

        return static_cast<size_t>(quotient + 1);
    }

    TimestampType timestampAtIndex(size_t grid_index) const
    {
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_index) * step_bits;
        return static_cast<TimestampType>(static_cast<Int64>(result_bits));
    }

    bool isSampleOutOfWindow(TimestampType timestamp, TimestampType grid_point) const
    {
        const Int128 staleness_cutoff = static_cast<Int128>(static_cast<Int64>(timestamp)) + static_cast<Int128>(static_cast<Int64>(window));
        return staleness_cutoff <= static_cast<Int128>(static_cast<Int64>(grid_point));
    }

    /// Bitwise comparison so that arrays holding a NaN phi still compare equal to themselves.
    static bool sameValue(Float64 lhs, Float64 rhs) { return std::bit_cast<UInt64>(lhs) == std::bit_cast<UInt64>(rhs); }

    static bool sameArray(const VectorWithMemoryTracking<Float64> & lhs, const VectorWithMemoryTracking<Float64> & rhs)
    {
        return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), sameValue);
    }

    /// The 3rd argument's element type follows the value type of the time series, which can be Float32 or Float64.
    static Float64 gridValueAt(const IColumn & column, size_t index)
    {
        if (const auto * float64_column = typeid_cast<const ColumnVector<Float64> *>(&column))
            return float64_column->getData()[index];
        return static_cast<Float64>(typeid_cast<const ColumnVector<Float32> &>(column).getData()[index]);
    }

    /// Bitwise like `sameValue`, with the element type dispatched once per row instead of once per element.
    static bool sameGridValues(const IColumn & column, size_t begin, size_t size, const VectorWithMemoryTracking<Float64> & captured)
    {
        if (size != captured.size())
            return false;
        if (size == 0)
            return true;

        if (const auto * float64_column = typeid_cast<const ColumnVector<Float64> *>(&column))
            return 0 == memcmp(float64_column->getData().data() + begin, captured.data(), size * sizeof(Float64));

        const auto & float32_data = typeid_cast<const ColumnVector<Float32> &>(column).getData();
        for (size_t i = 0; i < size; ++i)
            if (!sameValue(static_cast<Float64>(float32_data[begin + i]), captured[i]))
                return false;
        return true;
    }

    /// The 3rd argument must be the same array in every row: it is captured from the first row and every later row is
    /// compared against it in full, since a fingerprint of size and endpoints accepts rows differing in the middle.
    void captureOrCheckGridPhis(AggregateDataPtr __restrict place, const IColumn * phis_column, size_t row_num) const
    {
        const auto & arr = typeid_cast<const ColumnArray &>(*phis_column);
        const auto & offsets = arr.getOffsets();
        const auto begin = row_num == 0 ? 0 : offsets[row_num - 1];
        const auto end = offsets[row_num];
        const size_t size = end - begin;
        const auto & values = arr.getData();
        auto & grid_phis = data(place)->grid_phis;

        if (data(place)->grid_phis_captured)
        {
            if (!sameGridValues(values, begin, size, grid_phis))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires the same `phis` array for all rows", getName());
            return;
        }

        if (size != grid_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} requires the `phis` array to have one value per grid point, got {} and {}",
                getName(), size, grid_size);

        grid_phis.reserve(size);
        for (auto i = begin; i < end; ++i)
            grid_phis.push_back(gridValueAt(values, i));
        data(place)->grid_phis_captured = true;
    }

    const bool array_arguments{};
    const IntervalType step{};
    const IntervalType window{};
    const size_t grid_size{};
    const TimestampType start_timestamp{};
    const TimestampType timestamp_scale_multiplier{};
};

}
