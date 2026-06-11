#pragma once

#include <algorithm>
#include <cstddef>

#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeBuckets.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeOperations.h>


namespace DB
{

/// Aligned fast-path drivers and `Traits` aliases for the `_stats` family of
/// `_over_time` aggregates. The three drivers in this file specialize the
/// generic `AggregateFunctionTimeseriesBase` for grids where `window` is a
/// positive integer multiple of `grid_step`, exploiting the bucket layout to
/// run in `O(grid)` instead of `O(grid * samples_in_window)`:
///
///   * `AggregateFunctionTimeseriesOverTimeStatsAligned`
///       Running `(count, sum, sum_sq)` Accumulator, slid in O(1) per grid.
///       Powers `count / sum / avg / stddev / stdvar / present / absent`.
///   * `AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque`
///       Monotonic deque of bucket indices, amortized O(1) per grid.
///       Powers `min / max / ts_of_min / ts_of_max`.
///   * `AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl`
///       Single pointer along sorted-active-buckets, O(grid + active).
///       Powers `first / last / ts_of_first / ts_of_last`.
///
/// `Traits` aliases below bind each Operation to the appropriate bucket type;
/// `AggregateFunctionTimeseriesOverTime.cpp` then plugs them into the
/// `createAggregateFunctionTimeseries` factory under the `_stats` suffix.


/// Traits variant for the stats-bucket aligned fast path. The two differences from the
/// baseline `AggregateFunctionTimeseriesOverTimeTraits` are:
///   1. `Bucket = TimeseriesOverTimeAddableStatsBucket<...>` — running `(count, sum, sum_sq)`.
///   2. `strict_pre_window_filter = true` — `Base::add` drops samples falling in
///      `(start_timestamp - window - step, start_timestamp - step]`, so bucket 0 only holds
///      `(start - step, start]`. This is required for the bucket-level driver because
///      `AddableStatsBucket` cannot sub-filter pre-window samples after merging them into
///      running stats. Samples in `(start_timestamp - window, start_timestamp - step]`
///      that the baseline `_over_time` path would have included in grid 0's window are
///      dropped here — by design, since the aligned path's contract is "first sample must
///      satisfy `ts > start_timestamp - step`".
template <
    bool has_array_arguments,
    typename TimestampTypeT,
    typename IntervalTypeT,
    typename ValueTypeT,
    typename OperationT,
    template <typename, typename> class BucketT>
struct AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase
{
    static constexpr bool array_arguments = has_array_arguments;
    static constexpr bool strict_pre_window_filter = true;

    using TimestampType = TimestampTypeT;
    using IntervalType = IntervalTypeT;
    using ValueType = ValueTypeT;
    using Operation = OperationT;
    using Bucket = BucketT<TimestampType, ValueType>;

    static String getName() { return Operation::getName(); }
};

/// Template aliases matching the 5-parameter template template signature required by
/// `createAggregateFunctionTimeseries`. The 5th bool parameter is unused for _over_time functions.
template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesCountOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesCountOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeAddableStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesSumOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesSumOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeAddableStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAvgOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAvgOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeAddableStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStdvarOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStdvarPopOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeAddableStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesStddevOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesStddevPopOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeAddableStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesPresentOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesPresentOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimePresenceStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesAbsentOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesAbsentOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimePresenceStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMinOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMinOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMinMaxStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesMaxOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesMaxOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeMinMaxStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMinOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMinOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeTsAtExtremeStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfMaxOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfMaxOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeTsAtExtremeStatsBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesFirstOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesFirstOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeFirstSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfFirstOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfFirstOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeFirstSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesLastOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesLastOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;

template <bool array_arguments, typename TimestampType, typename IntervalType, typename ValueType, bool /*unused*/>
using AggregateFunctionTimeseriesTsOfLastOverTimeStatsAlignedTraits = AggregateFunctionTimeseriesOverTimeStatsAlignedTraitsBase<
    array_arguments, TimestampType, IntervalType, ValueType,
    AggregateFunctionTimeseriesTsOfLastOverTimeStatsAlignedOperation<TimestampType, ValueType>,
    TimeseriesOverTimeSingleSampleBucket>;


/// Stats-bucket aligned aggregate driven by a running `(count, sum, sum_sq)` accumulator
/// (or analogous structures); powers the additive and presence aggregates.
///
/// Contract:
///   - `Traits::Bucket` must expose `addBucket(Bucket) / subBucket(Bucket)` through a
///     `Traits::Operation::Accumulator` type — running stats are slid across the aligned
///     window in `O(grid)`.
///   - `Traits::strict_pre_window_filter == true` — bucket 0 has the same width as every
///     other bucket; see `TimeseriesOverTimeAddableStatsBucket` for rationale.
///   - `window % step == 0 && window >= step`; the constructor rejects everything else
///     with `BAD_ARGUMENTS`, redirecting callers to the baseline `_over_time` function.
///
/// Baseline and `_stats` `_over_time` aggregates share `FORMAT_VERSION = 1` (first release).
template <typename Traits>
class AggregateFunctionTimeseriesOverTimeStatsAligned final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAligned<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAligned<Traits>, Traits>;
    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesOverTimeStatsAligned(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_,
        UInt32 timestamp_scale_, Float64 extra_param_ = 0)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , extra_param(extra_param_)
    {
        if (step_ <= 0 || window_ < step_ || (window_ % step_) != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: this stats-bucket fast path requires `window` to be a positive integer multiple of `grid_step` "
                "and `window >= grid_step`; got window={}, grid_step={}. Use the baseline function (drop the "
                "`_stats` suffix) for unaligned or degenerate windows.",
                Traits::getName(), window_, step_);
    }

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        bucket.serialize(buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        bucket.deserialize(buf, [&](TimestampType timestamp)
        {
            Base::checkTimestampInRange(timestamp, bucket_index);
        });
    }

    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = Base::data(place)->buckets;

        chassert(Base::step > 0);
        chassert(Base::window >= Base::step);
        chassert((Base::window % Base::step) == 0);
        const UInt32 k = static_cast<UInt32>(Base::window / Base::step);

        typename Traits::Operation::Accumulator running;

        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            /// Bucket `i` newly enters the active window at grid `i`.
            if (auto it = buckets.find(i); it != buckets.end())
            {
                it->second.flushDedup();
                running.addBucket(it->second);
            }

            /// Bucket `i - k` just left the active window at grid `i`.
            if (i >= k)
            {
                if (auto it = buckets.find(i - k); it != buckets.end())
                {
                    /// Already flushed when first added; safe to subtract directly.
                    running.subBucket(it->second);
                }
            }

            Traits::Operation::fillResultValue(running, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

private:
    Float64 extra_param;
};


/// Mono-deque-driven stats-bucket aligned aggregate.
/// Powers `min_over_time`, `max_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`.
///
/// Algorithm (`O(grid)` amortized over the bucket count):
///   1. Pop the front of the deque while its bucket index has fallen out of the active
///      window (`front + k <= i`).
///   2. If bucket `i` is active, pop the back while `Operation::shouldEvictBack(back, current)`
///      holds; then push `i`.
///   3. Read the result from the bucket at `deque.front()`.
///
/// For `ts_of_min` / `ts_of_max`, `shouldEvictBack` returns `true` on equal values so that
/// the **later** bucket wins ties, matching VictoriaMetrics `rollupTmin / rollupTmax`.
template <typename Traits>
class AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque<Traits>, Traits>;
    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_,
        UInt32 timestamp_scale_, Float64 extra_param_ = 0)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , extra_param(extra_param_)
    {
        if (step_ <= 0 || window_ < step_ || (window_ % step_) != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: this stats-bucket fast path requires `window` to be a positive integer multiple of `grid_step` "
                "and `window >= grid_step`; got window={}, grid_step={}. Use the baseline function (drop the "
                "`_stats` suffix) for unaligned or degenerate windows.",
                Traits::getName(), window_, step_);
    }

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        bucket.serialize(buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        bucket.deserialize(buf, [&](TimestampType timestamp)
        {
            Base::checkTimestampInRange(timestamp, bucket_index);
        });
    }

    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = Base::data(place)->buckets;

        chassert(Base::step > 0);
        chassert(Base::window >= Base::step);
        chassert((Base::window % Base::step) == 0);
        const UInt32 k = static_cast<UInt32>(Base::window / Base::step);

        DequeWithMemoryTracking<UInt32> dq;

        for (UInt32 i = 0; i < Base::bucket_count; ++i)
        {
            /// (1) Pop expired front (bucket index ≤ i - k).
            while (!dq.empty() && dq.front() + k <= i)
                dq.pop_front();

            /// (2) Insert bucket i if active.
            auto it = buckets.find(i);
            if (it != buckets.end() && it->second.has_sample)
            {
                while (!dq.empty())
                {
                    auto back_it = buckets.find(dq.back());
                    if (back_it == buckets.end())
                        break;
                    if (Traits::Operation::shouldEvictBack(back_it->second, it->second))
                        dq.pop_back();
                    else
                        break;
                }
                dq.push_back(i);
            }

            /// (3) Read result.
            if (dq.empty())
            {
                values[i] = 0;
                nulls[i] = 1;
            }
            else if (auto front_it = buckets.find(dq.front()); front_it != buckets.end())
            {
                Traits::Operation::fillResultValue(front_it->second, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
            }
            else
            {
                values[i] = 0;
                nulls[i] = 1;
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

private:
    Float64 extra_param;
};


/// Two-pointer-driven stats-bucket aligned aggregate, parameterised by direction:
///   - `RightMost = true`  → last / ts_of_last (rightmost active bucket in window)
///   - `RightMost = false` → first / ts_of_first (leftmost active bucket in window)
///
/// Algorithm (`O(grid + active_buckets)`):
///   - Pre-sort the active bucket indices once.
///   - For each grid `i`, advance a single pointer along the sorted active list; if the
///     pointed bucket is in `[i-k+1, i]`, it is the answer (a unique winner exists because
///     buckets are mutually disjoint in time).
template <typename Traits, bool RightMost>
class AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl<Traits, RightMost>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl<Traits, RightMost>, Traits>;
    using Bucket = typename Base::Bucket;

    explicit AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_,
        UInt32 timestamp_scale_, Float64 extra_param_ = 0)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , extra_param(extra_param_)
    {
        if (step_ <= 0 || window_ < step_ || (window_ % step_) != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: this stats-bucket fast path requires `window` to be a positive integer multiple of `grid_step` "
                "and `window >= grid_step`; got window={}, grid_step={}. Use the baseline function (drop the "
                "`_stats` suffix) for unaligned or degenerate windows.",
                Traits::getName(), window_, step_);
    }

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        bucket.serialize(buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        bucket.deserialize(buf, [&](TimestampType timestamp)
        {
            Base::checkTimestampInRange(timestamp, bucket_index);
        });
    }

    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + Base::bucket_count);

        if (!Base::bucket_count)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::bucket_count);
        nulls_to.resize(old_size + Base::bucket_count);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = Base::data(place)->buckets;

        chassert(Base::step > 0);
        chassert(Base::window >= Base::step);
        chassert((Base::window % Base::step) == 0);
        const UInt32 k = static_cast<UInt32>(Base::window / Base::step);

        /// Build sorted list of active bucket indices once. The aligned hash-map walk-then-sort
        /// is `O(active + active*log(active))`, smaller than the grid in practice
        /// (`active <= bucket_count` and usually << grid).
        VectorWithMemoryTracking<std::pair<UInt32, const Bucket *>> sorted_active;
        sorted_active.reserve(buckets.size());
        for (const auto & [idx, bucket] : buckets)
        {
            if (bucket.has_sample)
                sorted_active.emplace_back(static_cast<UInt32>(idx), &bucket);
        }
        std::sort(sorted_active.begin(), sorted_active.end(),
            [](const auto & a, const auto & b) { return a.first < b.first; });

        if constexpr (RightMost)
        {
            /// Pointer `p` tracks the LARGEST index in `sorted_active` whose value is ≤ current grid `i`.
            /// Result for grid `i` exists iff `sorted_active[p] >= i - k + 1` (i.e. `+ k > i`).
            ssize_t p = -1;
            const ssize_t n = static_cast<ssize_t>(sorted_active.size());
            for (UInt32 i = 0; i < Base::bucket_count; ++i)
            {
                while (p + 1 < n && sorted_active[p + 1].first <= i)
                    ++p;

                if (p >= 0 && sorted_active[p].first + k > i)
                {
                    Traits::Operation::fillResultValue(
                        *sorted_active[p].second, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
                }
                else
                {
                    values[i] = 0;
                    nulls[i] = 1;
                }
            }
        }
        else
        {
            /// Pointer `p` tracks the SMALLEST index in `sorted_active` whose value is still in window
            /// (i.e. `>= i - k + 1`). Advance past expired entries each grid step.
            size_t p = 0;
            const size_t n = sorted_active.size();
            for (UInt32 i = 0; i < Base::bucket_count; ++i)
            {
                while (p < n && sorted_active[p].first + k <= i)
                    ++p;

                if (p < n && sorted_active[p].first <= i)
                {
                    Traits::Operation::fillResultValue(
                        *sorted_active[p].second, values[i], nulls[i], extra_param, Base::timestamp_scale_multiplier);
                }
                else
                {
                    values[i] = 0;
                    nulls[i] = 1;
                }
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

private:
    Float64 extra_param;
};

/// Concrete two-pointer aggregates exposed to `createAggregateFunctionTimeseries`
/// (which expects a `Traits`-only template-template parameter).
template <typename Traits>
using AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerLeft =
    AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl<Traits, /*RightMost=*/false>;

template <typename Traits>
using AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerRight =
    AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerImpl<Traits, /*RightMost=*/true>;

}
