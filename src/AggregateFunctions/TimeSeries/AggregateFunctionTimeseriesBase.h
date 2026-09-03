#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include <base/sort.h>

#include <libdivide-config.h>
#include <libdivide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/HashTable/HashMap.h>
#include <Common/TargetSpecific.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// Grower for the bucket maps. The stock `HashTableGrower` quadruples the buffer below `max_size_degree`,
/// which leaves the table at a load factor as low as ~0.125; with the whole `Bucket` stored in every cell that
/// wastes a lot of memory across many aggregation states. Doubling keeps the same worst-case load factor (0.5)
/// with at most half the slots of quadrupling.
struct TimeSeriesBucketsHashTableGrower : public HashTableGrower<4>
{
    void increaseSize()
    {
        ++size_degree;
    }
};

/// The bucket map of the `timeSeries*ToGrid` aggregation states: maps a bucket index in `[0, bucket_count)` to
/// the bucket's aggregate data. Also used by the `timeseries_to_grid_range_scan_vs_std_sort` example, which
/// tunes `BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN` for this exact container.
template <typename Bucket>
using TimeSeriesBucketsMap = HashMap<UInt64, Bucket, TrivialHash, TimeSeriesBucketsHashTableGrower>;

/// Base class for time series aggregate functions that map values to a grid specified by start timestamp, end timestamp, step and window.
/// It implements the common logic for handling input data as either scalar timestamps and values or vectors of timestamps and values of
/// equal sizes and adding the data to the grid buckets. The actual aggregation logic within buckets is implemented in derived classes.
template <class FunctionImpl, class Traits>
class AggregateFunctionTimeseriesBase :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesBase<FunctionImpl, Traits>>
{
public:
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
        , array_of_pairs_argument(argument_types_.size() == 1)
        , array_arguments(!array_of_pairs_argument && (argument_types_[1]->getTypeId() == TypeIndex::Array))
        , step(checkStep(start_timestamp_, end_timestamp_, step_))
        , window(checkWindow(window_))
        , grid_size(gridSize(start_timestamp_, end_timestamp_, step))
        , start_timestamp(start_timestamp_)
        , end_timestamp(alignedEndTimestamp(start_timestamp_, grid_size, step))
        , timestamp_scale_multiplier(static_cast<TimestampType>(DecimalUtils::scaleMultiplier<Int64>(timestamp_scale_)))
        , window_remainder(windowRemainder(step, window))
        , buckets_per_step(bucketsPerStep(window, window_remainder))
        , buckets_per_window(bucketsPerWindow(step, window, window_remainder))
        , buckets_per_first_window(bucketsPerFirstWindow(start_timestamp_, step, window, window_remainder, buckets_per_step, buckets_per_window))
        , bucket_count(bucketCount(grid_size, buckets_per_first_window, buckets_per_step))
        , even_bucket_width(bucketWidth(false, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , odd_bucket_width(bucketWidth(true, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , even_bucket_step(bucketStep(false, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , odd_bucket_step(bucketStep(true, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , first_bucket_end_time(firstBucketEndTimestamp(start_timestamp_, step, window, window_remainder, buckets_per_step, buckets_per_first_window))
        , first_bucket_width(firstBucketWidth(window, even_bucket_width, first_bucket_end_time))
        , first_bucket_start_time(firstBucketStartTimestamp(first_bucket_end_time, first_bucket_width))
        , step_divider(step > 0 ? static_cast<UInt64>(step) : 1)
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
        if (array_of_pairs_argument || array_arguments)
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

    /// Batch add with a per-row aggregation state. Rows reach the aggregator in storage order, and tables with
    /// time series data are typically sorted by (series, timestamp) - so rows come in long runs of the same
    /// state. The batch is split into runs of equal `places[i]` and each run goes through the batch add path,
    /// with the column casts hoisted out of the per-row loop; the generic implementation instead pays a virtual
    /// `add` with two `typeid_cast`s per row.
    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (array_of_pairs_argument || array_arguments)
        {
            /// A row of arrays holds a whole series, so the generic path's per-row overhead is amortized.
            Base::addBatch(row_begin, row_end, places, place_offset, columns, arena, if_argument_pos);
            return;
        }

        const UInt8 * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
        const TimestampType * timestamp_data = timestamp_column.getData().data();
        const ValueType * value_data = value_column.getData().data();

        size_t i = row_begin;
        while (i < row_end)
        {
            AggregateDataPtr place = places[i];
            size_t run_end = i + 1;
            while (run_end < row_end && places[run_end] == place)
                ++run_end;

            if (place)
                addSamples<true>(place + place_offset, timestamp_data, value_data, flags, i, run_end);

            i = run_end;
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
            auto & bucket = buckets[rhs_bucket.getKey()];
            bucket.merge(rhs_bucket.getMapped());
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(bucket_count, buf);

        writeBinaryLittleEndian(data(place)->buckets.size(), buf);

        for (const auto & entry : data(place)->buckets)
        {
            writeBinaryLittleEndian(entry.getKey(), buf);
            entry.getMapped().serialize(buf);
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

        /// `bucket_count` is derived from the function parameters and a huge window makes it enormous, so the
        /// number of buckets is only reserved up to a bound and the map grows while the buckets are read. That way
        /// a corrupted count fails with an end-of-buffer error instead of allocating memory for the claimed number.
        data(place)->buckets.reserve(std::min(buckets_size, MAX_BUCKETS_TO_RESERVE));

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
        auto aggregator = derived().createAggregator(getStackSizeForTwoStacks(buckets.size()));

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
                    const auto * it = buckets.find(next_bucket);
                    if (it)
                        aggregator.add(it->getMapped(), bucketEndTimestamp(next_bucket));
                }
                removeOutOfWindow(aggregator, grid_index);
                storeGridResult(grid_index, aggregator.getResult(timestampAtIndex(grid_index)), values, nulls);
            }
        }
        else
        {
            VectorWithMemoryTracking<std::pair<size_t, const Bucket *>> ordered_buckets;
            ordered_buckets.reserve(buckets.size());
            for (const auto & entry : buckets)
                ordered_buckets.emplace_back(entry.getKey(), &entry.getMapped());
            ::sort(ordered_buckets.begin(), ordered_buckets.end(),
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

    const bool array_of_pairs_argument{};   /// Whether samples are passed as a single argument of type Array(Tuple(timestamp, value))
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
    const size_t buckets_per_step{};        /// 2 when window_remainder != 0 (each step is split), else 1; 0 when window == 0
    const size_t buckets_per_window{};      /// Number of buckets tiling each grid point's window (0 when window == 0)
    const size_t buckets_per_first_window{};/// Buckets in grid point #0's window (<= buckets_per_window; leading
                                            /// buckets that would fall below the type's minimum are dropped)
    const size_t bucket_count{};            /// Number of buckets (0 when window == 0)

    /// Bucket #0 properties; every other bucket follows by arithmetic (see `bucketEndTimestamp`).
    const IntervalType even_bucket_width{};         /// Width of even-indexed buckets
    const IntervalType odd_bucket_width{};          /// Width of odd-indexed buckets (equals even_bucket_width when buckets_per_step == 1)
    const IntervalType even_bucket_step{};          /// End-to-end spacing of even-indexed buckets (equals the width unless window < step)
    const IntervalType odd_bucket_step{};           /// End-to-end spacing of odd-indexed buckets
    const TimestampType first_bucket_end_time{};    /// End timestamp of bucket #0
    const IntervalType first_bucket_width{};        /// Width of bucket #0: `even_bucket_width`, shortened when bucket #0
                                                    /// is clamped at the type minimum.
    const TimestampType first_bucket_start_time{};  /// Start (inclusive) of bucket #0.
                                                    /// Samples before it are out of window for every grid point.

    /// Reciprocal of `step` for `classifySample` (`step` is fixed at construction).
    const libdivide::divider<UInt64> step_divider{1};

private:
    /// `HashMap` relocates cells with `memcpy`, so it requires position-independent buckets: trivially
    /// copyable or declaring `is_position_independent`.
    static_assert(
        std::is_trivially_copyable_v<Bucket> || requires { requires Bucket::is_position_independent; },
        "Bucket must be position independent (memmove-able) to be stored in a HashMap");

    struct State
    {
        /// Maps bucket index to the set of all timestamps and values
        TimeSeriesBucketsMap<Bucket> buckets;
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
    /// collect-and-sort wins. The `timeseries_to_grid_range_scan_vs_std_sort` example measures the two strategies
    /// over the production bucket map (`HashMap` with `TrivialHash`): the range scan still wins at density 0.35
    /// (by ~6-8%) and loses at 0.30 (~9%), so the threshold is 0.35. With `TrivialHash` open addressing a cell's
    /// position is determined by the key, not the insertion order, so the crossover does not depend on how the map
    /// was filled (bulk adds or a merge). That example also shows comparison sorting beats a radix sort here, so
    /// the sparse path uses `::sort` (pdqsort).
    static constexpr double BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN = 0.35;

    /// The serialized state is the set of buckets, so the format version is defined by the traits
    /// (which define the bucket type).
    static constexpr UInt16 FORMAT_VERSION = Traits::FORMAT_VERSION;

    /// How many buckets `deserialize` reserves before reading the data. Bigger states grow while they are read.
    static constexpr size_t MAX_BUCKETS_TO_RESERVE = 4096;

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
        const Int128 quotient = (static_cast<Int128>(toInt64(end_timestamp)) - toInt64(start_timestamp))
            / static_cast<Int64>(step);

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
        const Int128 aligned_end = toInt64(start_timestamp)
            + static_cast<Int128>(grid_size - 1) * static_cast<Int64>(step);
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

    /// Number of buckets that tile one step.
    /// Returns 2 when the step is split, else 1, and 0 when window == 0 (so no buckets at all).
    static size_t bucketsPerStep(IntervalType window, IntervalType window_remainder)
    {
        if (window == 0)
            return 0;
        return window_remainder != 0 ? 2 : 1;
    }

    /// Calculates number of buckets that tile a window.
    static size_t bucketsPerWindow(IntervalType step, IntervalType window, IntervalType window_remainder)
    {
        if (window == 0)
            return 0;  /// window == 0 means no buckets at all.
        if (step == 0)
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
    static size_t bucketsPerFirstWindow(TimestampType start_timestamp, IntervalType step, IntervalType window,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_window)
    {
        if (window == 0)
            return 0;  /// window == 0 means no buckets at all.
        if (step == 0)
            return 1;

        const Int128 min_timestamp = toInt64(minTimestamp());
        const Int128 step_128 = static_cast<Int64>(step);
        /// How far `start_timestamp` sits above the smallest representable timestamp.
        const Int128 headroom = toInt64(start_timestamp) - min_timestamp;
        const size_t whole_steps = static_cast<size_t>(headroom / step_128);

        /// Leading buckets reaching no deeper than `headroom` stay in range; deeper ones are dropped. A split step
        /// keeps one extra (before-split) bucket when the remainder of `headroom` still covers the split point.
        const size_t reachable = (buckets_per_step == 1)
            ? whole_steps + 1
            : 2 * whole_steps + 1 + ((headroom % step_128 >= static_cast<Int64>(window_remainder)) ? 1 : 0);

        return std::min(buckets_per_window, reachable);
    }

    /// Calculates number of buckets: leading buckets related to the window of grid point #0
    /// plus 1 or 2 buckets per each step.
    static size_t bucketCount(size_t grid_size, size_t buckets_per_first_window, size_t buckets_per_step)
    {
        chassert(grid_size >= 1);
        /// Cannot overflow `size_t`: `grid_size <= MAX_GRID_SIZE` (16M, enforced by `gridSize`),
        /// `buckets_per_step` is 0, 1 or 2.
        return buckets_per_first_window + (grid_size - 1) * buckets_per_step;
    }

    /// Width (end - start) of even- or odd-indexed buckets. Static - used once, by the constructor.
    static IntervalType bucketWidth(bool odd_bucket, IntervalType step, IntervalType window,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        if (window == 0)
        {
            /// window == 0 means no buckets at all, so this width doesn't describe a real bucket.
            return 0;
        }
        /// Every real bucket has a width of at least 1: the branches below return `window`, `step`,
        /// `window_remainder` or `step - window_remainder`, all positive when `window > 0`.
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
    static IntervalType bucketStep(bool odd_bucket, IntervalType step, IntervalType window,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        if (window == 0)
            return 0;
        if (buckets_per_step == 1)
            return step;
        const bool before_split = (buckets_per_first_window % 2 == 0) != odd_bucket;
        return before_split ? (step - window_remainder) : window_remainder;
    }

    /// End timestamp of bucket #0 (the deepest in-range bucket). Static - used once, by the constructor.
    static TimestampType firstBucketEndTimestamp(TimestampType start_timestamp, IntervalType step, IntervalType window,
        IntervalType window_remainder, size_t buckets_per_step, size_t buckets_per_first_window)
    {
        if (window == 0)
        {
            /// window == 0 means no buckets at all, so there is no bucket #0.
            return start_timestamp;
        }

        /// Grid timestamp of bucket #0's step. Bucket #0's offset is `-(buckets_per_first_window - 1) <= 0`.
        /// Computed in `Int128` to avoid overflow (this runs once, at construction, so clarity beats speed).
        const Int128 offset = -(static_cast<Int128>(buckets_per_first_window) - 1);
        const Int128 grid_index = (buckets_per_step == 1) ? offset : offset / 2;
        const Int128 grid_timestamp = toInt64(start_timestamp)
            + grid_index * static_cast<Int64>(step);

        /// For a split step, bucket #0 ends `window_remainder` before the grid timestamp ("before split") iff
        /// `buckets_per_first_window` is even.
        const bool before_split = (buckets_per_step != 1) && (buckets_per_first_window % 2 == 0);
        return static_cast<TimestampType>(static_cast<Int64>(
            before_split ? grid_timestamp - static_cast<Int64>(window_remainder) : grid_timestamp));
    }

    /// Width of bucket #0: `even_bucket_width`, shortened when bucket #0's start falls below the smallest
    /// representable timestamp - bucket #0 then covers everything from that minimum up to its end. The
    /// shortened width fits `IntervalType`: such clamping implies it is not greater than `window`.
    static IntervalType firstBucketWidth(IntervalType window, IntervalType even_bucket_width, TimestampType first_bucket_end_time)
    {
        if (window == 0)
        {
            /// window == 0 means no buckets at all, so there is no bucket #0.
            return 0;
        }
        /// Every real bucket has a width of at least 1: `even_bucket_width >= 1` (see `bucketWidth`), and
        /// `clamped_width >= 1` because bucket #0's end is always representable (`bucketsPerFirstWindow`
        /// drops the buckets lying entirely below the type minimum).
        const Int128 min_timestamp = toInt64(minTimestamp());
        const Int128 clamped_width = toInt64(first_bucket_end_time) - min_timestamp + 1;
        return static_cast<IntervalType>(static_cast<Int64>(
            std::min(static_cast<Int128>(even_bucket_width), clamped_width)));
    }

    /// Calculates the start (inclusive) of bucket #0.
    static TimestampType firstBucketStartTimestamp(TimestampType first_bucket_end_time, IntervalType first_bucket_width)
    {
        if (first_bucket_width == 0)
        {
            /// window == 0 means no buckets at all.
            /// `classifySample` rejects every sample of such a grid.
            return first_bucket_end_time;
        }
        /// The start is always representable (the width is shortened when bucket #0 is clamped at the type minimum),
        /// so computing it as `end - (width - 1)` can't overflow.
        return static_cast<TimestampType>(toInt64(first_bucket_end_time) - (static_cast<Int64>(first_bucket_width) - 1));
    }

    /// Compute the grid timestamp for a given grid index, i.e. `start_timestamp + grid_index * step`.
    /// Uses unsigned 64-bit arithmetic internally to avoid signed overflow on extreme inputs
    /// (`start_timestamp` near `INT64_MIN` together with a `step` near `INT64_MAX`). The final
    /// cast back to `TimestampType` preserves the same bit pattern that the signed accumulator
    /// `grid_timestamp += step` would produce for normal inputs, but does not trigger UBSAN
    /// on the adversarial boundary values generated by the AST fuzzer.
    TimestampType timestampAtIndex(size_t grid_index) const
    {
        chassert(grid_index < grid_size);
        const UInt64 start_bits = static_cast<UInt64>(toInt64(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(step);
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_index) * step_bits;
        const TimestampType grid_point = static_cast<TimestampType>(static_cast<Int64>(result_bits));
        return grid_point;
    }

    static constexpr size_t NO_BUCKET = -1;

    /// Closed timestamp range `[start_time, end_time]` of a bucket.
    struct BucketTimeRange
    {
        TimestampType start_time;
        TimestampType end_time;

        bool contains(const TimestampType & timestamp) const
        {
            return timestamp >= start_time && timestamp <= end_time;
        }
    };

    /// What the batch bucketing kernel (`addSamplesToBucketsImpl`) does with a sample: add it to bucket
    /// `bucket_index`, or skip it (`bucket_index == NO_BUCKET`). Either way `time_range` is a timestamp
    /// range whose samples all share this fate - the bucket's time range for an accepted sample, a range
    /// of rejected timestamps for a skipped one - so the kernel handles the run of consecutive samples
    /// within the range in one go. The classified sample itself is always inside the range.
    struct SampleClass
    {
        size_t bucket_index;
        BucketTimeRange time_range;
    };

    /// Classifies a sample: the bucket index to add it to (or NO_BUCKET to skip it), plus its `time_range`
    /// for run detection. With `ReturnType == size_t` the range is neither computed nor returned - the
    /// function returns just the bucket index (see `bucketIndexForTimestamp`).
    template <typename ReturnType = SampleClass>
    ALWAYS_INLINE ReturnType classifySample(const TimestampType timestamp) const
    {
        static_assert(std::is_same_v<ReturnType, SampleClass> || std::is_same_v<ReturnType, size_t>);
        constexpr bool return_index = std::is_same_v<ReturnType, size_t>;

        if (timestamp > end_timestamp)
        {
            if constexpr (return_index)
                return NO_BUCKET;
            else if (bucket_count == 0)
                return {NO_BUCKET, {minTimestamp(), maxTimestamp()}};  /// A grid without buckets (`window == 0`) rejects everything.
            else
                return {NO_BUCKET, {static_cast<TimestampType>(toInt64(end_timestamp) + 1), maxTimestamp()}};  /// `end < timestamp`, so no overflow
        }

        /// A sample before bucket #0's start is out of window for every grid point (samples older than
        /// the whole grid's windows are a common case). A start clamped to the type minimum rejects
        /// nothing - then every timestamp is in window.
        if (timestamp < first_bucket_start_time)
        {
            if constexpr (return_index)
                return NO_BUCKET;
            else if (bucket_count == 0)
                return {NO_BUCKET, {minTimestamp(), maxTimestamp()}};  /// A grid without buckets (`window == 0`) rejects everything.
            else
                return {NO_BUCKET, {minTimestamp(), static_cast<TimestampType>(toInt64(first_bucket_start_time) - 1)}};  /// The check passed, so no underflow
        }

        /// All the arithmetic is 64-bit for any grid parameters: a difference of two Int64 timestamps can
        /// overflow Int64, but every value computed here is non-negative and less than 2^64, so UInt64
        /// arithmetic is exact throughout.
        const Int64 ts = toInt64(timestamp);
        const Int64 start = toInt64(start_timestamp);
        const UInt64 step_u64 = static_cast<UInt64>(step);
        const UInt64 window_u64 = static_cast<UInt64>(window);  /// `window >= 0`, see `checkWindow`
        const UInt64 window_remainder_u64 = static_cast<UInt64>(window_remainder);
        const UInt64 leading_buckets = buckets_per_first_window;  /// >= 1 when the grid has buckets

        UInt64 bucket_index = 0;

        if (ts > start)
        {
            /// The sample's grid point is #ceil(offset / step), counted up from grid point #0.
            /// 0 < offset <= end - start, and `step > 0` because `start < end` (see `checkStep`).
            const UInt64 offset = static_cast<UInt64>(ts) - static_cast<UInt64>(start);
            const UInt64 whole_steps = offset / step_divider;
            /// Distance down to the grid timestamp at or below the sample, in [0, step).
            const UInt64 distance_from_grid_point = offset - whole_steps * step_u64;
            /// Distance from the sample up to its grid point, in [0, step).
            const UInt64 distance_to_grid_point = (distance_from_grid_point == 0) ? 0 : (step_u64 - distance_from_grid_point);

            /// A sample out of its grid point's window is out of every window (windows of later grid points
            /// start even higher), and so is everything in its step down to the window's start. Possible only
            /// when `window < step` and the grid point is at or above `start + step`, so the bounds fit Int64.
            /// (A zero window rejects every sample of this branch here.)
            if (distance_to_grid_point >= window_u64)
            {
                if constexpr (return_index)
                    return NO_BUCKET;
                else
                {
                    const Int64 grid_timestamp = ts + static_cast<Int64>(distance_to_grid_point);
                    return {NO_BUCKET, {static_cast<TimestampType>(grid_timestamp - static_cast<Int64>(step) + 1),
                        static_cast<TimestampType>(grid_timestamp - static_cast<Int64>(window))}};
                }
            }

            const UInt64 grid_index = whole_steps + (distance_from_grid_point != 0);

            if (window_remainder == 0)
            {
                /// One bucket per step.
                bucket_index = grid_index + leading_buckets - 1;
            }
            else
            {
                /// Each step is split at (grid timestamp - window_remainder): `before_split_point` means
                /// timestamp <= grid timestamp - window_remainder.
                const bool before_split_point = distance_to_grid_point >= window_remainder_u64;
                bucket_index = 2 * grid_index + leading_buckets - 1 - (before_split_point ? 1 : 0);
            }
        }
        else
        {
            /// A grid without buckets (`window == 0`) accepts nothing; the only such sample reaching this
            /// branch is one exactly at `start` (everything below was rejected by the check against
            /// bucket #0's start, which equals `start` then).
            if (bucket_count == 0)
            {
                if constexpr (return_index)
                    return NO_BUCKET;
                else
                    return {NO_BUCKET, {minTimestamp(), maxTimestamp()}};
            }

            /// The sample's grid point is #0, and its bucket is one of the leading buckets: 1 or 2 buckets
            /// per whole step down from bucket #(leading_buckets - 1). The index can't go below 0: a sample
            /// within the window and above the type minimum has its bucket (see `bucketsPerFirstWindow`).
            /// 0 <= offset_below_start < window: samples at or before `start - window` were rejected by the early check.
            const UInt64 offset_below_start = static_cast<UInt64>(start) - static_cast<UInt64>(ts);

            if (step_u64 == 0)
            {
                /// A single-point grid (`start == end`, see `checkStep`) has a single bucket taking everything in window.
                chassert(leading_buckets == 1);
                bucket_index = 0;
            }
            else
            {
                const UInt64 whole_steps_below = offset_below_start / step_divider;
                /// Distance from the sample up to the nearest step-aligned timestamp at or below `start`, in [0, step).
                const UInt64 distance_to_grid_point = offset_below_start - whole_steps_below * step_u64;

                if (window_remainder == 0)
                {
                    bucket_index = leading_buckets - 1 - whole_steps_below;
                }
                else
                {
                    const bool before_split_point = distance_to_grid_point >= window_remainder_u64;
                    bucket_index = leading_buckets - 1 - 2 * whole_steps_below - (before_split_point ? 1 : 0);
                }
            }
        }

        chassert(bucket_index < bucket_count);
        if constexpr (return_index)
        {
            return static_cast<size_t>(bucket_index);
        }
        else
        {
            const BucketTimeRange time_range = bucketTimeRange(static_cast<size_t>(bucket_index));
            chassert(ts >= toInt64(time_range.start_time) && ts <= toInt64(time_range.end_time));
            return {static_cast<size_t>(bucket_index), time_range};
        }
    }

    /// Returns the index of the bucket a sample at `timestamp` contributes to.
    /// The function returns NO_BUCKET if the specified timestamp can't contribute to any buckets
    /// because it's too early, or too late, or already out of window.
    size_t ALWAYS_INLINE bucketIndexForTimestamp(const TimestampType timestamp) const
    {
        return classifySample<size_t>(timestamp);
    }

    /// Returns a half-open range [first, last) of bucket indices that fall in a grid point's window. The range has
    /// `buckets_per_window` buckets, except early windows that are truncated at 0 by the dropped leading buckets.
    std::pair<size_t, size_t> bucketRangeInWindow(size_t grid_index) const
    {
        chassert(grid_index < grid_size);
        const size_t window_begin = grid_index * buckets_per_step;
        const size_t skipped_leading_buckets = buckets_per_window - buckets_per_first_window;
        return {window_begin > skipped_leading_buckets ? window_begin - skipped_leading_buckets : 0, window_begin + buckets_per_first_window};
    }

    /// End timestamp of bucket `bucket_index`: `first_bucket_end_time` plus the end-spacings (`even/odd_bucket_step`)
    /// of buckets 1..bucket_index. Wrapping unsigned arithmetic: the products can overflow for extreme grids, but the
    /// in-range end is recovered modulo 2^64.
    TimestampType ALWAYS_INLINE bucketEndTimestamp(size_t bucket_index) const
    {
        chassert(bucket_index < bucket_count);
        const UInt64 num_even_buckets = bucket_index / 2;
        const UInt64 num_odd_buckets = bucket_index - num_even_buckets;
        const UInt64 bucket_end_time = static_cast<UInt64>(toInt64(first_bucket_end_time))
            + num_odd_buckets * odd_bucket_step + num_even_buckets * even_bucket_step;
        return static_cast<TimestampType>(static_cast<Int64>(bucket_end_time));
    }

    /// Returns the closed timestamp range of bucket `bucket_index`.
    ALWAYS_INLINE BucketTimeRange bucketTimeRange(size_t bucket_index) const
    {
        chassert(bucket_index < bucket_count);
        const TimestampType end_time = bucketEndTimestamp(bucket_index);
        const Int64 bucket_width = static_cast<Int64>(bucket_index == 0
            ? first_bucket_width
            : ((bucket_index % 2 != 0) ? odd_bucket_width : even_bucket_width));
        /// `end - (width - 1)` is the bucket's start, which is always representable (`width >= 1`), so the arithmetic can't overflow.
        return {static_cast<TimestampType>(toInt64(end_time) - (bucket_width - 1)), end_time};
    }

    /// Chooses the two-stacks queue size for a sliding aggregator that combines per-bucket `Traits::Summary`
    /// values with a non-invertible `SlidingSum`; the result is passed to the derived class's `createAggregator`.
    /// Traits opt into the two-stacks strategy by defining the thresholds; otherwise the stack size is unused and 0 is returned.
    /// The enable threshold is compared to the average number of populated buckets per window (not the dense
    /// maximum), so sparse data stays on the cheaper recompute path; the hard cap still forces two-stacks
    /// for large windows, where a low average can hide a locally dense window.
    size_t getStackSizeForTwoStacks(size_t num_populated_buckets) const
    {
        if constexpr (requires { Traits::AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS; Traits::BPW_TO_FORCE_TWO_STACKS; })
        {
            const size_t avg_buckets_in_window = bucket_count
                ? static_cast<size_t>(static_cast<double>(buckets_per_window) * static_cast<double>(num_populated_buckets)
                    / static_cast<double>(bucket_count))
                : 0;
            const bool use_two_stacks = avg_buckets_in_window >= Traits::AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS
                || buckets_per_window >= Traits::BPW_TO_FORCE_TWO_STACKS;
            /// Reserve at most `buckets_per_window`, but capped by `num_populated_buckets` - else a huge window
            /// (forced onto two-stacks by the hard cap) would `reserve(~INT64_MAX)` and fail to allocate.
            return use_two_stacks ? std::min(buckets_per_window, num_populated_buckets) : 0;
        }
        else
        {
            return 0;
        }
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

    static constexpr ALWAYS_INLINE Int64 toInt64(TimestampType timestamp)
    {
        return static_cast<Int64>(timestamp);
    }

    /// The smallest representable timestamp.
    static constexpr TimestampType minTimestamp()
    {
        if constexpr (std::is_unsigned_v<TimestampType>)
            return 0;
        else
            return static_cast<TimestampType>(std::numeric_limits<Int64>::min());
    }

    /// The largest representable timestamp.
    static constexpr TimestampType maxTimestamp()
    {
        if constexpr (std::is_unsigned_v<TimestampType>)
            return std::numeric_limits<TimestampType>::max();
        else
            return static_cast<TimestampType>(std::numeric_limits<Int64>::max());
    }

    /// Returns the number of leading samples of `timestamps[0, count)` with timestamps in `[lo, hi]`.
    /// Checked in blocks so that the loop vectorizes; the samples of a partial block are re-checked one by one.
    static ALWAYS_INLINE size_t scanSamplesInRange(const TimestampType * __restrict timestamps, size_t count, const BucketTimeRange & range)
    {
        /// Converted to Int64 once, outside the loops: comparing `TimestampType` (a `Decimal` for
        /// `DateTime64`) per sample would call out-of-line comparison operators and prevent vectorization.
        const Int64 lo = toInt64(range.start_time);
        const Int64 hi = toInt64(range.end_time);

        constexpr size_t block_size = 16;
        size_t scanned = 0;
        while (scanned + block_size <= count)
        {
            UInt8 all_in_range = 1;
            for (size_t j = 0; j < block_size; ++j)
            {
                const Int64 timestamp = toInt64(timestamps[scanned + j]);
                all_in_range &= static_cast<UInt8>(timestamp >= lo) & static_cast<UInt8>(timestamp <= hi);
            }
            if (!all_in_range)
                break;
            scanned += block_size;
        }
        while (scanned < count)
        {
            const Int64 timestamp = toInt64(timestamps[scanned]);
            if (!(timestamp >= lo && timestamp <= hi))
                break;
            ++scanned;
        }
        return scanned;
    }

    MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(void NO_INLINE),
    addSamplesToBucketsImpl,
    MULTITARGET_FUNCTION_BODY((State * __restrict state,
        const TimestampType * __restrict timestamps,
        const ValueType * __restrict values,
        size_t row_begin,
        size_t row_end) const /// NOLINT
    {
        size_t i = row_begin;
        while (i < row_end)
        {
            const SampleClass sample_class = classifySample(timestamps[i]);
            const size_t run = scanSamplesInRange(timestamps + i, row_end - i, sample_class.time_range);
            /// The classified sample is inside its own range, so `run >= 1`; `max` only guards
            /// against an infinite loop.
            const size_t count = std::max<size_t>(run, static_cast<size_t>(1));
            if (sample_class.bucket_index != NO_BUCKET)
                state->buckets[sample_class.bucket_index].addMany(timestamps + i, values + i, count);
            i += count;
        }
    })
    )

    /// Entry point of the batch add path; `flags == nullptr` means every sample is included.
    /// Flagged samples (a NULL map of a `Nullable` value column or a `-If` condition) stay on the
    /// plain per-sample path.
    template <bool flag_value_to_include>
    void addSamples(AggregateDataPtr __restrict place,
        const TimestampType * __restrict timestamps,
        const ValueType * __restrict values,
        const UInt8 * __restrict flags,
        size_t row_begin,
        size_t row_end) const
    {
        if (flags)
        {
            for (size_t i = row_begin; i < row_end; ++i)
                if (!flags || (flags[i] != 0) == flag_value_to_include)
                    add(place, timestamps[i], values[i]);
            return;
        }

        State * state = data(place);

#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            addSamplesToBucketsImpl_x86_64_v4(state, timestamps, values, row_begin, row_end);
            return;
        }
#endif
        addSamplesToBucketsImpl(state, timestamps, values, row_begin, row_end);
    }

    void addMany(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, size_t start, size_t end) const
    {
        addSamples<true>(place, timestamp_ptr, value_ptr, nullptr, start, end);
    }

    void addManyNotNull(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict null_map, size_t start, size_t end) const
    {
        addSamples<false>(place, timestamp_ptr, value_ptr, null_map, start, end);
    }

    void addManyConditional(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) const
    {
        addSamples<true>(place, timestamp_ptr, value_ptr, condition_map, start, end);
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
        if (!array_of_pairs_argument && !array_arguments)
        {
            /// Each row holds a single sample.
            const TimestampType * timestamp_data = typeid_cast<const ColVecType &>(*columns[0]).getData().data();
            const ValueType * value_data = typeid_cast<const ColVecResultType &>(*columns[1]).getData().data();

            if (!flags_data)
                addMany(place, timestamp_data, value_data, row_begin, row_end);
            else if constexpr (flag_value_to_include)
                addManyConditional(place, timestamp_data, value_data, flags_data, row_begin, row_end);
            else
                addManyNotNull(place, timestamp_data, value_data, flags_data, row_begin, row_end);

            return;
        }

        /// Each row holds a whole series.
        const ColumnArray::Offset * timestamp_offsets = nullptr;
        const ColumnArray::Offset * value_offsets = nullptr;
        const TimestampType * timestamp_data = nullptr;
        const ValueType * value_data = nullptr;

        if (array_of_pairs_argument)
        {
            const auto & array_column = typeid_cast<const ColumnArray &>(*columns[0]);
            const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());

            /// The timestamps and the values are stored in the same array, so they share the offsets.
            timestamp_offsets = array_column.getOffsets().data();
            value_offsets = timestamp_offsets;
            timestamp_data = typeid_cast<const ColVecType &>(tuple_column.getColumn(0)).getData().data();
            value_data = typeid_cast<const ColVecResultType &>(tuple_column.getColumn(1)).getData().data();
        }
        else
        {
            const auto & timestamp_array_column = typeid_cast<const ColumnArray &>(*columns[0]);
            const auto & value_array_column = typeid_cast<const ColumnArray &>(*columns[1]);

            timestamp_offsets = timestamp_array_column.getOffsets().data();
            value_offsets = value_array_column.getOffsets().data();
            timestamp_data = typeid_cast<const ColVecType &>(timestamp_array_column.getData()).getData().data();
            value_data = typeid_cast<const ColVecResultType &>(value_array_column.getData()).getData().data();
        }

        size_t previous_timestamp_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
        size_t previous_value_offset = (row_begin == 0 ? 0 : value_offsets[row_begin - 1]);

        if (!flags_data)
        {
            checkSeriesSizes(row_begin, row_end, timestamp_offsets, value_offsets);

            /// No row is skipped, so the samples of all the rows are stored contiguously and are added
            /// in a single call, which lets the vectorized kernel work on the whole batch.
            const size_t samples_count = (row_end == 0 ? 0 : timestamp_offsets[row_end - 1]) - previous_timestamp_offset;
            addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, samples_count);
            return;
        }

        for (size_t i = row_begin; i < row_end; ++i)
        {
            const size_t timestamp_array_size = timestamp_offsets[i] - previous_timestamp_offset;
            const size_t value_array_size = value_offsets[i] - previous_value_offset;

            /// A flag is per row, and each row holds a whole series
            if (flags_data[i] == flag_value_to_include)
            {
                /// Check that timestamp and value arrays have the same size for the selected rows
                if (timestamp_array_size != value_array_size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                        i, timestamp_array_size, value_array_size);

                addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, timestamp_array_size);
            }

            previous_timestamp_offset = timestamp_offsets[i];
            previous_value_offset = value_offsets[i];
        }
    }

    /// Checks that the timestamp array and the value array have the same size in each row.
    static void checkSeriesSizes(size_t row_begin, size_t row_end,
        const ColumnArray::Offset * timestamp_offsets, const ColumnArray::Offset * value_offsets)
    {
        size_t previous_timestamp_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
        size_t previous_value_offset = (row_begin == 0 ? 0 : value_offsets[row_begin - 1]);

        for (size_t i = row_begin; i < row_end; ++i)
        {
            const size_t timestamp_array_size = timestamp_offsets[i] - previous_timestamp_offset;
            const size_t value_array_size = value_offsets[i] - previous_value_offset;

            if (timestamp_array_size != value_array_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                    i, timestamp_array_size, value_array_size);

            previous_timestamp_offset = timestamp_offsets[i];
            previous_value_offset = value_offsets[i];
        }
    }

    const FunctionImpl & derived() const
    {
        return static_cast<const FunctionImpl &>(*this);
    }

    /// Drops buckets that have left grid point `grid_index`'s window from the front of the sliding `aggregator`. A bucket
    /// is out of window once all its samples are at or before the cutoff `grid_timestamp - window`; window-aligned
    /// buckets are fully in or out, so the bucket's latest timestamp decides.
    template <typename Aggregator>
    void removeOutOfWindow(Aggregator & aggregator, size_t grid_index) const
    {
        /// A cutoff below the smallest representable timestamp can't drop anything and is skipped;
        /// the check is rearranged as `grid_timestamp >= min_timestamp + window` so that neither side can overflow.
        chassert(grid_index < grid_size);
        static constexpr Int64 min_timestamp = toInt64(minTimestamp());
        const Int64 grid_timestamp = toInt64(timestampAtIndex(grid_index));
        if (grid_timestamp >= min_timestamp + static_cast<Int64>(window))
            aggregator.removeBefore(static_cast<TimestampType>(grid_timestamp - static_cast<Int64>(window)));
    }

    /// Stores the window's result value (or NULL when there is no result) at grid point `grid_index`.
    void storeGridResult(size_t grid_index, const std::optional<ValueType> & result, ValueType * values, UInt8 * nulls) const
    {
        chassert(grid_index < grid_size);
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
