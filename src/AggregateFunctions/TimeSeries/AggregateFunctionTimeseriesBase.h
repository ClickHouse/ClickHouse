#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// Base class for time series aggregate functions that map values to a grid specified by start timestamp, end timestamp, step and window.
/// It implements the common logic for handling input data as either scalar timestamps and values or vectors of timestamps and values of
/// equal sizes and adding the data to the grid buckets. The actual aggregation logic within buckets is implemented in derived classes.
template <class FunctionImpl, class Traits>
class AggregateFunctionTimeseriesBase :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesBase<FunctionImpl, Traits>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesBase<FunctionImpl, Traits>>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;
    using ColVecResultType = ColumnVectorOrDecimal<ValueType>;

    using Bucket = typename Traits::Bucket;

    String getName() const override
    {
        return Traits::getName();
    }

    /// Timeseries parameters may carry DecimalField (from toDateTime64(...) casts), whose
    /// default printed form collides with String literals — so we print parameters with ::Type.
    bool shouldPrintParametersWithTypes() const override { return true; }

    explicit AggregateFunctionTimeseriesBase(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_)
        : Base(
            argument_types_,
            parameters_,
            createResultType())
        , array_arguments(argument_types_[1]->getTypeId() == TypeIndex::Array)
        , step(checkStep(start_timestamp_, end_timestamp_, step_))
        , window(checkWindow(window_))
        , grid_size(gridSize(start_timestamp_, end_timestamp_, step))
        , start_timestamp(start_timestamp_)
        , end_timestamp(alignedEndTimestamp(start_timestamp_, grid_size, step))
        , timestamp_scale_multiplier(static_cast<TimestampType>(DecimalUtils::scaleMultiplier<Int64>(timestamp_scale_)))
        , window_remainder(windowRemainder(step, window))
        , buckets_per_step(bucketsPerStep(window_remainder))
        , buckets_per_window(bucketsPerWindow(step, window, window_remainder))
        , buckets_per_first_window(bucketsPerFirstWindow(start_timestamp_, step, window_remainder, buckets_per_step, buckets_per_window))
        , bucket_count(bucketCount(grid_size, buckets_per_first_window, buckets_per_step))
        , even_bucket_width(bucketWidth(false, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , odd_bucket_width(bucketWidth(true, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , even_bucket_step(bucketStep(false, step, window_remainder, buckets_per_step, buckets_per_first_window))
        , odd_bucket_step(bucketStep(true, step, window_remainder, buckets_per_step, buckets_per_first_window))
        , first_bucket_end_time(firstBucketEndTimestamp(start_timestamp_, step, window_remainder, buckets_per_step, buckets_per_first_window))
        , first_bucket_is_clamped(firstBucketIsClamped(first_bucket_end_time, even_bucket_width))
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<State>;
    }

    size_t alignOfData() const override
    {
        return alignof(State);
    }

    size_t sizeOfData() const override
    {
        return sizeof(State);
    }

    void create(AggregateDataPtr __restrict place) const override  /// NOLINT
    {
        new (place) State{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place)->~State();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (array_arguments)
        {
            addBatchSinglePlace(row_num, row_num + 1, place, columns, arena, -1);
        }
        else
        {
            const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
            add(place, timestamp_column.getData()[row_num], value_column.getData()[row_num]);
        }
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * include_flags_data = nullptr;
        if (if_argument_pos >= 0)
        {
            const auto & flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if (row_end > flags.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "row_end {} is greater than flags column size {}", row_end, flags.size());

            include_flags_data = flags.data();
        }

        addBatchSinglePlaceWithFlags<true>(row_begin, row_end, place, columns, include_flags_data);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos)
        const override
    {
        const UInt8 * exclude_flags_data = null_map;    /// By default exclude using null_map
        std::unique_ptr<UInt8[]> combined_exclude_flags;

        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            combined_exclude_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                combined_exclude_flags[i] = (!!null_map[i]) | !if_flags[i]; /// Exclude if NULL or if condition is false
            exclude_flags_data = combined_exclude_flags.get();
        }

        addBatchSinglePlaceWithFlags<false>(row_begin, row_end, place, columns, exclude_flags_data);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & buckets = data(place)->buckets;
        const auto & rhs_buckets = data(rhs)->buckets;
        buckets.reserve(rhs_buckets.size());
        for (const auto & rhs_bucket : rhs_buckets)
        {
            auto & bucket = buckets[rhs_bucket.first];
            bucket.merge(rhs_bucket.second);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(bucket_count, buf);

        writeBinaryLittleEndian(data(place)->buckets.size(), buf);

        for (const auto & bucket : data(place)->buckets)
        {
            writeBinaryLittleEndian(bucket.first, buf);
            bucket.second.serialize(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        size_t size = 0;
        readBinaryLittleEndian(size, buf);

        if (size != bucket_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different bucket count");

        size_t buckets_size = 0;
        readBinaryLittleEndian(buckets_size, buf);

        if (buckets_size > bucket_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more buckets than expected");

        data(place)->buckets.reserve(buckets_size);

        for (size_t i = 0; i < buckets_size; ++i)
        {
            size_t bucket_index = 0;
            readBinaryLittleEndian(bucket_index, buf);

            if (bucket_index >= bucket_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with index {} greater than bucket count {}", bucket_index, bucket_count);

            auto & bucket = data(place)->buckets[bucket_index];
            bucket.deserialize(buf);

            /// Validate that each deserialized sample falls into this bucket's timestamp range.
            bucket.checkTimestampsInRange(bucketTimeRange(bucket_index));
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        derived().doInsertResultInto(place, to);
    }

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        IColumn & to,
        Arena *) const override
    {
        size_t batch_index = row_begin;
        const size_t batch_size = row_end - row_begin;

        /// Reserve offsets and values in column to
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        offsets_to.reserve(offsets_to.size() + batch_size);
        data_to.reserve(data_to.size() + batch_size * grid_size);
        nulls_to.reserve(nulls_to.size() + batch_size * grid_size);

        try
        {
            for (; batch_index < row_end; ++batch_index)
            {
                derived().doInsertResultInto(places[batch_index] + place_offset, to);
                /// For State AggregateFunction ownership of aggregate place is passed to result column after insert,
                /// so we need to destroy all states up to state of -State combinator.
                Base::destroyUpToState(places[batch_index] + place_offset);
            }
        }
        catch (...)
        {
            for (size_t destroy_index = batch_index; destroy_index < row_end; ++destroy_index)
                destroy(places[destroy_index] + place_offset);

            throw;
        }
    }

protected:
    /// Constructs the result array for one grid (one aggregate state). For each grid point the window's value is
    /// computed by a per-function sliding `Aggregator` (built by `createAggregator` in the derived class): as the
    /// grid advances, buckets entering the window are fed to `Aggregator::add` (which preaggregates them into a
    /// `Summary` where needed), buckets leaving are dropped by `removeBefore`, and `getResult` reads off the window's
    /// value. The aggregator keeps only the window's worth of data, so there is no materialization of all buckets and
    /// no global sort in the dense case.
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.empty() ? grid_size : offsets_to.back() + grid_size);

        if (!grid_size)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + grid_size);
        nulls_to.resize(old_size + grid_size);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = data(place)->buckets;
        auto aggregator = derived().createAggregator(buckets.size());

        /// Visit the populated buckets in ascending index order, feeding each into the sliding window when its
        /// grid point's window reaches it. When most bucket slots are populated (`use_range_scan`) looking each
        /// index up in the hash map is cheaper than sorting; otherwise collect the populated buckets and sort them once.
        const bool use_range_scan = (buckets.size() != 0)
            && (static_cast<double>(buckets.size()) >= static_cast<double>(bucket_count) * BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN);
        if (use_range_scan)
        {
            size_t next_bucket = 0;
            for (size_t grid_index = 0; grid_index < grid_size; ++grid_index)
            {
                const size_t window_end = bucketRangeInWindow(grid_index).second;
                for (; next_bucket < window_end; ++next_bucket)
                {
                    const auto it = buckets.find(next_bucket);
                    if (it != buckets.end())
                        aggregator.add(it->second, bucketEndTimestamp(next_bucket));
                }
                removeOutOfWindow(aggregator, grid_index);
                storeGridResult(grid_index, aggregator.getResult(timestampAtIndex(grid_index)), values, nulls);
            }
        }
        else
        {
            VectorWithMemoryTracking<std::pair<size_t, const Bucket *>> ordered_buckets;
            ordered_buckets.reserve(buckets.size());
            for (const auto & [bucket_index, bucket] : buckets)
                ordered_buckets.emplace_back(bucket_index, &bucket);
            std::sort(ordered_buckets.begin(), ordered_buckets.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

            size_t pos = 0;
            for (size_t grid_index = 0; grid_index < grid_size; ++grid_index)
            {
                const size_t window_end = bucketRangeInWindow(grid_index).second;
                for (; pos < ordered_buckets.size() && ordered_buckets[pos].first < window_end; ++pos)
                    aggregator.add(*ordered_buckets[pos].second, bucketEndTimestamp(ordered_buckets[pos].first));
                removeOutOfWindow(aggregator, grid_index);
                storeGridResult(grid_index, aggregator.getResult(timestampAtIndex(grid_index)), values, nulls);
            }
        }
    }

    const bool array_arguments{};           /// Whether timestamp/value arguments are arrays (one row holds a whole series) or scalars
    const IntervalType step{};              /// Grid step (0 for a single-point grid). IntervalType represents a time difference between timestamps
    const IntervalType window{};            /// Window size used by derived functions (e.g. for rate and delta calculations)
    const size_t grid_size{};               /// Number of grid points: (end - start) / step + 1
    const TimestampType start_timestamp{};  /// First timestamp in the grid
    const TimestampType end_timestamp{};    /// Last timestamp in the grid. NOTE: It is aligned down by step relative to start_timestamp
    const TimestampType timestamp_scale_multiplier{};   /// When timestamps are in DateTime64 (which is Decimal with some scale)
                                                        /// this multiplier is used for calculation rate per second (i.e. it is 1000 for
                                                        /// milliseconds or 1e6 for microseconds)
    const IntervalType window_remainder{};  /// (window % step) if (window > step)
    const size_t buckets_per_step{};        /// 2 when window_remainder != 0 (each step is split), else 1
    const size_t buckets_per_window{};      /// Number of buckets tiling each grid point's window
    const size_t buckets_per_first_window{};/// Buckets in grid point #0's window (<= buckets_per_window; leading
                                            /// buckets that would fall below the type's minimum are dropped)
    const size_t bucket_count{};            /// Number of buckets

    /// Bucket #0 properties; every other bucket follows by arithmetic (see `bucketEndTimestamp`).
    const IntervalType even_bucket_width{};       /// Width (end - start) of even-indexed buckets
    const IntervalType odd_bucket_width{};        /// Width of odd-indexed buckets (equals even_bucket_width when buckets_per_step == 1)
    const IntervalType even_bucket_step{};        /// End-to-end spacing of even-indexed buckets (equals the width unless window < step)
    const IntervalType odd_bucket_step{};         /// End-to-end spacing of odd-indexed buckets
    const TimestampType first_bucket_end_time{};  /// End timestamp of bucket #0
    const bool first_bucket_is_clamped{};         /// Whether bucket #0's start is below the type minimum (only it can be)

private:
    struct State
    {
        /// Maps bucket index to the set of all timestamps and values
        UnorderedMapWithMemoryTracking<size_t, Bucket> buckets;
    };

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ValueType>>()));
    }

    /// Upper bound on the number of grid points (the output array length) for a single grid.
    /// This prevents absurdly large grids (e.g. from adversarial input that passes extreme
    /// timestamps and a tiny step) from allocating huge amounts of memory or triggering
    /// undefined behaviour in downstream arithmetic. 16M is consistent with the
    /// `MAX_ARRAY_SIZE` used by other aggregate functions (`AggregateFunctionGroupArray`,
    /// `AggregateFunctionIntervalLengthSum`, etc.).
    static constexpr size_t MAX_GRID_SIZE = 0xFFFFFF;

    /// `doInsertResultInto` visits the populated buckets in index order. When the fraction of populated bucket
    /// slots (`populated / bucket_count`) is at least this, scanning the whole index range and looking each up in
    /// the hash map is cheaper than collecting and sorting the populated buckets; below it (sparse data) the
    /// collect-and-sort wins. The `timeseries_to_grid_range_scan_vs_std_sort` example measures the crossover density at
    /// ~0.4; it depends on the bucket map's memory layout (~0.45 when buckets sit in index order, ~0.37 when
    /// scattered as after a merge, so 0.4 is the middle). That example also shows `std::sort` beats a radix sort
    /// here, so the sparse path uses `std::sort`.
    static constexpr double BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN = 0.4;

    static constexpr UInt16 FORMAT_VERSION = FunctionImpl::FORMAT_VERSION;

    /// Validates and normalizes the grid step. For a single-point grid (`start == end`) the step is irrelevant, so it
    /// is normalized to 0 (making each window a single bucket); otherwise it must be positive.
    static IntervalType checkStep(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (start_timestamp == end_timestamp)
            return 0;
        if (step <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
        return step;
    }

    /// Validates the window size.
    static IntervalType checkWindow(IntervalType window)
    {
        if (window < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window should be non-negative");
        return window;
    }

    /// Calculates number of grid points: (end - start) / step + 1.
    static size_t gridSize(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (end_timestamp < start_timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

        if (end_timestamp == start_timestamp)
            return 1;

        chassert(step > 0);

        /// Computed in Int128 to stay overflow-safe when the [start, end] span exceeds Int64 (e.g. DateTime64 from
        /// near INT64_MIN to near INT64_MAX). Runs once per aggregator, so width is preferred over speed.
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

    /// Calculates the grid's end timestamp: `start_timestamp + (grid_size - 1) * step`, aligned down by step.
    static TimestampType alignedEndTimestamp(TimestampType start_timestamp, size_t grid_size, IntervalType step)
    {
        /// Computed in Int128 to stay overflow-safe for extreme inputs (e.g. start near INT64_MIN, large step);
        /// runs once per aggregator.
        const Int128 aligned_end = static_cast<Int128>(static_cast<Int64>(start_timestamp))
            + static_cast<Int128>(grid_size - 1) * static_cast<Int128>(static_cast<Int64>(step));
        return static_cast<TimestampType>(static_cast<Int64>(aligned_end));
    }

    /// Calculates remainder `window % step` which determines a split point for window-aligned buckets.
    /// Returns 0 if no split is needed: when `window <= step`, or when the window is a whole multiple of the step.
    static IntervalType windowRemainder(IntervalType step, IntervalType window)
    {
        if (step == 0 || window <= step)
            return 0;
        return static_cast<IntervalType>(window % step);
    }

    /// Calculates number of buckets that tile a window.
    static size_t bucketsPerStep(IntervalType window_remainder)
    {
        return window_remainder != 0 ? 2 : 1;
    }

    /// Calculates number of buckets that tile a window.
    static size_t bucketsPerWindow(IntervalType step, IntervalType window, IntervalType window_remainder)
    {
        if (step == 0 || window == 0)
            return 1;

        const size_t whole_steps = static_cast<size_t>(window / step);
        if (window_remainder != 0)
        {
            /// Cannot overflow `size_t`: `window_remainder != 0` implies `step >= 2`.
            return 2 * whole_steps + 1;
        }
        return whole_steps == 0 ? 1 : whole_steps;
    }

    /// Number of buckets in grid point #0's window. Usually `buckets_per_window`, but fewer when `start_timestamp -
    /// window` reaches below the smallest representable timestamp: those leading buckets lie entirely below the type
    /// minimum, so they can never hold a sample and are dropped (which keeps every bucket's end timestamp in range).
    static size_t bucketsPerFirstWindow(TimestampType start_timestamp, IntervalType step, IntervalType window_remainder,
        size_t buckets_per_step, size_t buckets_per_window)
    {
        if (step == 0)
            return 1;

        const Int128 min_timestamp = std::is_unsigned_v<TimestampType>
            ? static_cast<Int128>(0) : static_cast<Int128>(std::numeric_limits<Int64>::min());
        const Int128 step_128 = static_cast<Int128>(static_cast<Int64>(step));
        /// How far `start_timestamp` sits above the smallest representable timestamp.
        const Int128 headroom = static_cast<Int128>(static_cast<Int64>(start_timestamp)) - min_timestamp;
        const size_t whole_steps = static_cast<size_t>(headroom / step_128);

        /// Leading buckets reaching no deeper than `headroom` stay in range; deeper ones are dropped. A split step
        /// keeps one extra (before-split) bucket when the remainder of `headroom` still covers the split point.
        const size_t reachable = (buckets_per_step == 1)
            ? whole_steps + 1
            : 2 * whole_steps + 1 + ((headroom % step_128 >= static_cast<Int128>(static_cast<Int64>(window_remainder))) ? 1 : 0);

        return std::min(buckets_per_window, reachable);
    }

    /// Calculates number of buckets: leading buckets related to the window of grid point #0
    /// plus 1 or 2 buckets per each step.
    static size_t bucketCount(size_t grid_size, size_t buckets_per_first_window, size_t buckets_per_step)
    {
        chassert(grid_size >= 1);
        /// Cannot overflow `size_t`: `grid_size <= MAX_GRID_SIZE` (16M, enforced by `gridSize`),
        /// `buckets_per_step` is 1 or 2.
        return buckets_per_first_window + (grid_size - 1) * buckets_per_step;
    }

    /// Width (end - start) of even- or odd-indexed buckets. Static - used once, by the constructor.
    static IntervalType bucketWidth(bool odd_bucket, IntervalType step, IntervalType window,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        if (buckets_per_step == 1)
            return (step == 0 || window < step) ? window : step;
        /// Even-indexed bucket #0 is "before split" iff `buckets_per_first_window` is even; odd-indexed buckets are
        /// the opposite side of the split.
        const bool before_split = (buckets_per_first_window % 2 == 0) != odd_bucket;
        return before_split ? (step - window_remainder) : window_remainder;
    }

    /// End-to-end spacing of even- or odd-indexed buckets (how far a bucket's end is from the previous one's). Equals
    /// the bucket width, except for one bucket per step with `window < step`, where ends stay spaced by `step` even
    /// though each bucket is narrower. Static - used once, by the constructor.
    static IntervalType bucketStep(bool odd_bucket, IntervalType step,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        if (buckets_per_step == 1)
            return step;
        const bool before_split = (buckets_per_first_window % 2 == 0) != odd_bucket;
        return before_split ? (step - window_remainder) : window_remainder;
    }

    /// End timestamp of bucket #0 (the deepest in-range bucket). Static - used once, by the constructor.
    static TimestampType firstBucketEndTimestamp(TimestampType start_timestamp, IntervalType step,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        /// Grid timestamp of bucket #0's step. Bucket #0's offset is `-(buckets_per_first_window - 1) <= 0`.
        /// Computed in `Int128` to avoid overflow (this runs once, at construction, so clarity beats speed).
        const Int128 offset = -(static_cast<Int128>(buckets_per_first_window) - 1);
        const Int128 grid_index = (buckets_per_step == 1) ? offset : offset / 2;
        const Int128 grid_timestamp = static_cast<Int128>(static_cast<Int64>(start_timestamp))
            + grid_index * static_cast<Int128>(static_cast<Int64>(step));

        /// For a split step, bucket #0 ends `window_remainder` before the grid timestamp ("before split") iff
        /// `buckets_per_first_window` is even.
        const bool before_split = (buckets_per_step != 1) && (buckets_per_first_window % 2 == 0);
        return static_cast<TimestampType>(static_cast<Int64>(
            before_split ? grid_timestamp - static_cast<Int64>(window_remainder) : grid_timestamp));
    }

    /// Whether bucket #0's start (its end minus its width) falls below the smallest representable timestamp. Only
    /// bucket #0 can: buckets tile the timeline, so every later bucket starts where the previous one ends (in range).
    static bool firstBucketIsClamped(TimestampType first_bucket_end_time, IntervalType even_bucket_width)
    {
        const Int128 min_timestamp = std::is_unsigned_v<TimestampType>
            ? static_cast<Int128>(0) : static_cast<Int128>(std::numeric_limits<Int64>::min());
        return static_cast<Int128>(static_cast<Int64>(first_bucket_end_time)) - static_cast<Int64>(even_bucket_width) < min_timestamp;
    }

    /// Compute the grid timestamp for a given grid index, i.e. `start_timestamp + grid_index * step`.
    /// Uses unsigned 64-bit arithmetic internally to avoid signed overflow on extreme inputs
    /// (`start_timestamp` near `INT64_MIN` together with a `step` near `INT64_MAX`). The final
    /// cast back to `TimestampType` preserves the same bit pattern that the signed accumulator
    /// `grid_timestamp += step` would produce for normal inputs, but does not trigger UBSAN
    /// on the adversarial boundary values generated by the AST fuzzer.
    TimestampType timestampAtIndex(size_t grid_index) const
    {
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_index) * step_bits;
        const TimestampType grid_point = static_cast<TimestampType>(static_cast<Int64>(result_bits));
        return grid_point;
    }

    /// Returns whether a sample at `timestamp` is past the sliding-window cutoff for grid point `grid_timestamp`.
    bool isSampleOutOfWindow(const TimestampType timestamp, const TimestampType grid_point) const
    {
        /// Compare as Int128 to avoid signed-overflow `TimestampType` when `window` is set near `INT64_MAX`.
        const Int128 staleness_cutoff =
            static_cast<Int128>(static_cast<Int64>(timestamp)) +
            static_cast<Int128>(static_cast<Int64>(window));
        return staleness_cutoff <= static_cast<Int128>(static_cast<Int64>(grid_point));
    }

    static constexpr size_t NO_BUCKET = -1;

    /// Returns the index of the bucket a sample at `timestamp` contributes to.
    /// The function returns NO_BUCKET if the specified timestamp can't contribute to any buckets
    /// because it's too early, or too late, or already out of window.
    size_t bucketIndexForTimestamp(const TimestampType timestamp) const
    {
        if (timestamp > end_timestamp)
            return NO_BUCKET;

        /// unclamped_grid_index = ceil((timestamp - start) / step), the grid point at the upper edge
        /// of the sample's step.
        /// It's unclamped: for timestamps at or before grid point #0 `unclamped_grid_index <= 0`.
        /// Everything is computed in Int128 to stay overflow-safe when `start_timestamp` is
        /// near INT64_MIN and `step` near INT64_MAX.
        const Int128 offset = static_cast<Int128>(static_cast<Int64>(timestamp)) - static_cast<Int128>(static_cast<Int64>(start_timestamp));
        const Int128 step_128 = static_cast<Int128>(static_cast<Int64>(step));

        /// `step == 0` is possible only when `start == end` (a single grid point).
        Int128 unclamped_grid_index = 0;
        if (step > 0)
        {
            unclamped_grid_index = offset / step_128;
            if ((offset % step_128) > 0)
                ++unclamped_grid_index;
        }

        /// The related grid point's index is always non-negative.
        const size_t grid_index = (unclamped_grid_index > 0) ? static_cast<size_t>(unclamped_grid_index) : 0;

        /// Skip a sample that is already out of window.
        if (isSampleOutOfWindow(timestamp, timestampAtIndex(grid_index)))
            return NO_BUCKET;

        const Int128 leading_buckets = static_cast<Int128>(buckets_per_first_window);
        Int128 bucket_index;
        if (window_remainder == 0)
        {
            /// One bucket per step.
            bucket_index = unclamped_grid_index + leading_buckets - 1;
        }
        else
        {
            /// Each step is split at (grid timestamp - window_remainder).
            const Int128 remainder = static_cast<Int128>(static_cast<Int64>(window_remainder));

            /// `before_split_point` means timestamp <= grid timestamp - window_remainder,
            /// i.e. offset + window_remainder <= unclamped_grid_index * step.
            const bool before_split_point = (offset + remainder) <= (unclamped_grid_index * step_128);
            bucket_index = 2 * unclamped_grid_index + leading_buckets - 1 - (before_split_point ? 1 : 0);
        }

        chassert(bucket_index >= 0 && bucket_index < static_cast<Int128>(bucket_count));
        return static_cast<size_t>(bucket_index);
    }

    /// Returns a half-open range [first, last) of bucket indices that fall in a grid point's window. The range has
    /// `buckets_per_window` buckets, except early windows that are truncated at 0 by the dropped leading buckets.
    std::pair<size_t, size_t> bucketRangeInWindow(size_t grid_index) const
    {
        const size_t window_begin = grid_index * buckets_per_step;
        const size_t skipped_leading_buckets = buckets_per_window - buckets_per_first_window;
        return {window_begin > skipped_leading_buckets ? window_begin - skipped_leading_buckets : 0, window_begin + buckets_per_first_window};
    }

    /// End timestamp of bucket `bucket_index`: `first_bucket_end_time` plus the end-spacings (`even/odd_bucket_step`)
    /// of buckets 1..bucket_index. Wrapping unsigned arithmetic: the products can overflow for extreme grids, but the
    /// in-range end is recovered modulo 2^64.
    TimestampType ALWAYS_INLINE bucketEndTimestamp(size_t bucket_index) const
    {
        const UInt64 num_even_buckets = bucket_index / 2;
        const UInt64 num_odd_buckets = bucket_index - num_even_buckets;
        const UInt64 bucket_end_time = static_cast<UInt64>(static_cast<Int64>(first_bucket_end_time))
            + num_odd_buckets * odd_bucket_step + num_even_buckets * even_bucket_step;
        return static_cast<TimestampType>(static_cast<Int64>(bucket_end_time));
    }

    /// Half-open timestamp range `(start_time, end_time]` of a bucket.
    struct BucketTimeRange
    {
        std::optional<TimestampType> start_time;  /// The lower bound is optional
        TimestampType end_time;

        bool contains(const TimestampType & timestamp) const
        {
            return (!start_time || timestamp > *start_time) && timestamp <= end_time;
        }
    };

    /// Returns the timestamp range of bucket `bucket_index`.
    BucketTimeRange bucketTimeRange(size_t bucket_index) const
    {
        const TimestampType bucket_end_time = bucketEndTimestamp(bucket_index);

        /// Only the very first bucket can start below the type minimum; every later bucket starts where the previous
        /// one ends, which is in range. `first_bucket_is_clamped` records that single case.
        if (bucket_index == 0 && first_bucket_is_clamped)
            return {std::nullopt, bucket_end_time};  /// No lower bound.

        const IntervalType bucket_width = (bucket_index % 2 != 0) ? odd_bucket_width : even_bucket_width;
        const auto bucket_start_time = static_cast<TimestampType>(static_cast<Int64>(bucket_end_time) - bucket_width);
        return {bucket_start_time, bucket_end_time};
    }

    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value) const
    {
        const size_t bucket_index = bucketIndexForTimestamp(timestamp);
        if (bucket_index == NO_BUCKET)
            return;  /// The sample can't contribute to any bucket.

        auto & bucket = data(place)->buckets[bucket_index];
        bucket.add(timestamp, value);
    }

    void addMany(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyNotNull(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict null_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (!null_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyConditional(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (condition_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    /// `flag_value_to_include` parameter determines which rows are included into result.
    /// E.g. if we pass null_map as flags_data and then we want to include rows where null flag is false
    /// or we can pass boolean condition column and include rows where the flag is true
    template <bool flag_value_to_include>
    void addBatchSinglePlaceWithFlags(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * flags_data) const
    {
        if (array_arguments)
        {
            const auto & timestamp_column = typeid_cast<const ColumnArray &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColumnArray &>(*columns[1]);
            const auto & timestamp_offsets = timestamp_column.getOffsets();
            const auto & value_offsets = value_column.getOffsets();
            const TimestampType * timestamp_data = typeid_cast<const ColVecType *>(timestamp_column.getDataPtr().get())->getData().data();
            const ValueType * value_data = typeid_cast<const ColVecResultType *>(value_column.getDataPtr().get())->getData().data();

            if (flags_data)
            {
                size_t previous_timestamp_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                size_t previous_value_offset = (row_begin == 0 ? 0 : value_offsets[row_begin - 1]);
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    const auto timestamp_array_size = timestamp_offsets[i] - previous_timestamp_offset;
                    const auto value_array_size = value_offsets[i] - previous_value_offset;

                    if (flags_data[i] == flag_value_to_include)
                    {
                        /// Check that timestamp and value arrays have the same size for the selected rows
                        if (timestamp_array_size != value_array_size)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                                i, timestamp_array_size, value_array_size);

                        /// A flag is per row, and each row is a pair of arrays
                        addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, timestamp_array_size);
                    }

                    previous_timestamp_offset = timestamp_offsets[i];
                    previous_value_offset = value_offsets[i];
                }
            }
            else
            {
                {
                    /// Check that timestamp and value arrays have the same size for each row
                    size_t previous_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                    for (size_t i = row_begin; i < row_end; ++i)
                    {
                        const auto timestamp_array_size = timestamp_offsets[i] - previous_offset;
                        const auto value_array_size = value_offsets[i] - previous_offset;

                        if (timestamp_array_size != value_array_size)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                                i, timestamp_array_size, value_array_size);

                        previous_offset = timestamp_offsets[i];
                    }
                }

                const size_t data_row_begin = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                const size_t data_row_end = (row_end == 0 ? 0 : timestamp_offsets[row_end - 1]);

                addMany(place, timestamp_data, value_data, data_row_begin, data_row_end);
            }
        }
        else
        {
            const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
            const TimestampType * timestamp_data = timestamp_column.getData().data();
            const ValueType * value_data = value_column.getData().data();

            if (flags_data)
            {
                if constexpr (flag_value_to_include)
                    addManyConditional(place, timestamp_data, value_data, flags_data, row_begin, row_end);
                else
                    addManyNotNull(place, timestamp_data, value_data, flags_data, row_begin, row_end);
            }
            else
            {
                addMany(place, timestamp_data, value_data, row_begin, row_end);
            }
        }
    }

    const FunctionImpl & derived() const
    {
        return static_cast<const FunctionImpl &>(*this);
    }

    /// Drops buckets that have left grid point `grid_index`'s window from the front of the sliding `aggregator`. A bucket
    /// is out of window once all its samples are at or before the cutoff `grid_timestamp - window`; window-aligned
    /// buckets are fully in or out, so the bucket's latest timestamp decides. The cutoff is computed in `Int128`
    /// because `grid_timestamp - window` can be negative (`grid_timestamp < window`) or even underflow `Int64`
    /// (`start_timestamp` near `INT64_MIN` with a huge window). When it is below the smallest representable
    /// timestamp (`0` for unsigned `DateTime`, `INT64_MIN` for `DateTime64`) no bucket can be out of window, so
    /// `removeBefore` is skipped - which also avoids wrapping a negative cutoff cast to an unsigned `TimestampType`.
    template <typename Aggregator>
    void removeOutOfWindow(Aggregator & aggregator, size_t grid_index) const
    {
        static constexpr Int64 min_timestamp = std::is_unsigned_v<TimestampType> ? 0 : std::numeric_limits<Int64>::min();
        const Int128 cut_off = static_cast<Int128>(static_cast<Int64>(timestampAtIndex(grid_index)))
            - static_cast<Int128>(static_cast<Int64>(window));
        if (cut_off >= static_cast<Int128>(min_timestamp))
            aggregator.removeBefore(static_cast<TimestampType>(static_cast<Int64>(cut_off)));
    }

    /// Stores the window's result value (or NULL when there is no result) at grid point `grid_index`.
    void storeGridResult(size_t grid_index, const std::optional<ValueType> & result, ValueType * values, UInt8 * nulls) const
    {
        if (result)
        {
            values[grid_index] = *result;
            nulls[grid_index] = 0;
        }
        else
        {
            values[grid_index] = ValueType{};
            nulls[grid_index] = 1;
        }
    }
};

}
