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
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesLinearRegression.h>
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

/// Varying-`predict_offset` sibling of `timeSeriesPredictLinearToGrid` (offset there is a fixed parameter).
/// Deliberately isolated from `AggregateFunctionTimeseriesBase` to avoid risk to its ~10 other users: it keeps a flat
/// sample list instead of the base's bucket map, but slides the same regression summary over it.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesPredictLinearVarying final :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesPredictLinearVarying<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesPredictLinearVarying>;
    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;
    /// Reuses the numerically-stable (Welford's algorithm) centered-moments regression summary from the
    /// constant-offset function: it is already generic (no dependency on AggregateFunctionTimeseriesBase).
    using RegressionTraits = AggregateFunctionTimeseriesLinearRegressionTraits<TimestampType, IntervalType, ValueType, /*is_predict=*/true>;
    using Summary = typename RegressionTraits::Summary;
    using Aggregator = typename RegressionTraits::Aggregator;

    String getName() const override { return "timeSeriesPredictLinearVaryingToGrid"; }

    bool shouldPrintParametersWithTypes() const override { return true; }

    explicit AggregateFunctionTimeseriesPredictLinearVarying(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_)
        : Base(argument_types_, parameters_, createResultType())
        , array_arguments(argument_types_[1]->getTypeId() == TypeIndex::Array)
        , step(checkStep(start_timestamp_, end_timestamp_, step_))
        , window(checkWindow(window_))
        , grid_size(gridSize(start_timestamp_, end_timestamp_, step))
        , start_timestamp(start_timestamp_)
        , timestamp_scale_multiplier(static_cast<TimestampType>(DecimalUtils::scaleMultiplier<Int64>(timestamp_scale_)))
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
        captureOrCheckGridOffsets(place, columns[2], row_num);

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

        if (!data(rhs)->grid_offsets_captured)
            return;

        if (!data(place)->grid_offsets_captured)
        {
            data(place)->grid_offsets = data(rhs)->grid_offsets;
            data(place)->grid_offsets_captured = true;
            return;
        }

        /// States are merged in full here: unlike `add`, merging happens once per partial state, not once per sample.
        if (!sameArray(data(place)->grid_offsets, data(rhs)->grid_offsets))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot merge states of aggregate function {} created with different arguments: "
                "the `predict_offsets` array must be the same for all rows", getName());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        data(place)->samples.serialize(buf);
        writeBinaryLittleEndian(data(place)->grid_offsets_captured, buf);
        if (data(place)->grid_offsets_captured)
        {
            writeBinaryLittleEndian(static_cast<UInt64>(data(place)->grid_offsets.size()), buf);
            for (const auto offset : data(place)->grid_offsets)
                writeBinaryLittleEndian(offset, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        data(place)->samples.deserialize(buf);
        readBinaryLittleEndian(data(place)->grid_offsets_captured, buf);
        if (data(place)->grid_offsets_captured)
        {
            UInt64 size = 0;
            readBinaryLittleEndian(size, buf);
            if (size > MAX_GRID_SIZE)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more grid offsets than expected");
            data(place)->grid_offsets.resize(size);
            for (auto & offset : data(place)->grid_offsets)
                readBinaryLittleEndian(offset, buf);
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
        const auto & grid_offsets = state->grid_offsets;
        if (state->grid_offsets_captured && grid_offsets.size() != grid_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "timeSeriesPredictLinearVaryingToGrid: predict_offsets array has {} elements, expected {} (the grid size)",
                grid_offsets.size(), grid_size);

        /// Sorted samples pulled once, then swept with a two-pointer window as the grid advances -- both
        /// edges only move forward as grid_index increases, so this is O(samples + grid_size) total.
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> sorted_samples;
        state->samples.forEachSample([&sorted_samples](TimestampType timestamp, ValueType value)
        {
            sorted_samples.emplace_back(timestamp, value);
        });

        /// The window's regression summary does not depend on the offset, so it is maintained slidingly by the
        /// same aggregator `timeSeriesPredictLinearToGrid` uses; only `getResult` below reads the offset.
        Aggregator aggregator{maxSamplesInWindow(sorted_samples), start_timestamp, 0.0, timestamp_scale_multiplier};

        size_t window_begin = 0; /// First sample index with timestamp > (grid_timestamp - window).
        size_t window_end = 0;   /// One past the last sample index with timestamp <= grid_timestamp.

        for (size_t grid_index = 0; grid_index < grid_size; ++grid_index)
        {
            const TimestampType grid_timestamp = timestampAtIndex(grid_index);

            for (; window_end < sorted_samples.size() && sorted_samples[window_end].first <= grid_timestamp; ++window_end)
            {
                Summary entering;
                entering.add(sorted_samples[window_end].first, sorted_samples[window_end].second, start_timestamp);
                aggregator.add(std::move(entering), sorted_samples[window_end].first);
            }

            const size_t stale_begin = window_begin;
            while (window_begin < window_end && isSampleOutOfWindow(sorted_samples[window_begin].first, grid_timestamp))
                ++window_begin;
            /// Cut off by the last departing sample's timestamp: the next sample's is strictly greater,
            /// otherwise it would have been dropped by the loop above too.
            if (window_begin != stale_begin)
                aggregator.removeBefore(sorted_samples[window_begin - 1].first);

            /// Offsets arrive in seconds (PromQL units); scaled to internal timestamp units here,
            /// matching the constant-offset registration's pre-multiply.
            aggregator.predict_offset = state->grid_offsets_captured
                ? grid_offsets[grid_index] * static_cast<Float64>(timestamp_scale_multiplier) : 0.0;
            const std::optional<ValueType> result = aggregator.getResult(grid_timestamp);

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
    static constexpr size_t MAX_GRID_SIZE = 0xFFFFFF; /// Matches AggregateFunctionTimeseriesBase::MAX_GRID_SIZE.

    struct State
    {
        Samples samples;
        VectorWithMemoryTracking<Float64> grid_offsets;
        bool grid_offsets_captured = false;
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

    /// Same overflow-safe Int128 computation as AggregateFunctionTimeseriesBase::gridSize.
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

    /// Same overflow-safe UInt64-wrapping computation as AggregateFunctionTimeseriesBase::timestampAtIndex.
    TimestampType timestampAtIndex(size_t grid_index) const
    {
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_index) * step_bits;
        return static_cast<TimestampType>(static_cast<Int64>(result_bits));
    }

    /// Upper bound on the samples one window can hold, used to size the sliding summary's stacks. A window ending
    /// at a grid point holds no more samples than the one ending at its last sample, which this scans for.
    size_t maxSamplesInWindow(const VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> & sorted_samples) const
    {
        size_t result = 0;
        size_t begin = 0;
        for (size_t end = 0; end < sorted_samples.size(); ++end)
        {
            while (begin < end && isSampleOutOfWindow(sorted_samples[begin].first, sorted_samples[end].first))
                ++begin;
            result = std::max(result, end - begin + 1);
        }
        return result;
    }

    /// Same Int128 staleness-cutoff comparison as AggregateFunctionTimeseriesBase::isSampleOutOfWindow.
    bool isSampleOutOfWindow(TimestampType timestamp, TimestampType grid_point) const
    {
        const Int128 staleness_cutoff = static_cast<Int128>(static_cast<Int64>(timestamp)) + static_cast<Int128>(static_cast<Int64>(window));
        return staleness_cutoff <= static_cast<Int128>(static_cast<Int64>(grid_point));
    }

    /// Bitwise comparison so that arrays holding a NaN offset still compare equal to themselves.
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
    void captureOrCheckGridOffsets(AggregateDataPtr __restrict place, const IColumn * offsets_column, size_t row_num) const
    {
        const auto & arr = typeid_cast<const ColumnArray &>(*offsets_column);
        const auto & offsets = arr.getOffsets();
        const auto begin = row_num == 0 ? 0 : offsets[row_num - 1];
        const auto end = offsets[row_num];
        const size_t size = end - begin;
        const auto & values = arr.getData();
        auto & grid_offsets = data(place)->grid_offsets;

        if (data(place)->grid_offsets_captured)
        {
            if (!sameGridValues(values, begin, size, grid_offsets))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires the same `predict_offsets` array for all rows", getName());
            return;
        }

        if (size != grid_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} requires the `predict_offsets` array to have one value per grid point, got {} and {}",
                getName(), size, grid_size);

        grid_offsets.reserve(size);
        for (auto i = begin; i < end; ++i)
            grid_offsets.push_back(gridValueAt(values, i));
        data(place)->grid_offsets_captured = true;
    }

    const bool array_arguments{};
    const IntervalType step{};
    const IntervalType window{};
    const size_t grid_size{};
    const TimestampType start_timestamp{};
    const TimestampType timestamp_scale_multiplier{};
};

}
