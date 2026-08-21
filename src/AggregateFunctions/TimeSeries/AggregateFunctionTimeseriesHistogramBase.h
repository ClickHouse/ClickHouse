#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include <libdivide-config.h>
#include <libdivide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Functions/TimeSeries/TimeSeriesHistogramKernel.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// A slice of one of the aggregation state's blob arrays. Offsets are state-local and never serialized
/// (the slice is re-appended on merge/deserialize), so two states never share blobs.
struct TimeSeriesHistogramBlobRef
{
    UInt64 offset = 0;
    UInt64 size = 0;
};
static_assert(std::is_trivially_copyable_v<TimeSeriesHistogramBlobRef>);

/// The bucket of the `timeSeriesHistogramLastToGrid` state: the newest sample; scalars inline, the 5 array fields as `TimeSeriesHistogramBlobRef`
/// slices into per-state blobs (trivially copyable, `memcpy`-relocatable); superseded slices stay in the blobs until the state is destroyed.
template <typename TimestampType>
struct TimeSeriesHistogramBucket
{
    TimestampType newest_timestamp{};
    Float64 zero_threshold{};
    Float64 count{};
    Float64 sum{};
    Float64 zero_count{};
    TimeSeriesHistogramBlobRef positive_spans{};
    TimeSeriesHistogramBlobRef positive_values{};
    TimeSeriesHistogramBlobRef negative_spans{};
    TimeSeriesHistogramBlobRef negative_values{};
    TimeSeriesHistogramBlobRef custom_values{};
    UInt8 flags{};
    Int8 schema{};
    bool has_value{};
};
static_assert(std::is_trivially_copyable_v<TimeSeriesHistogramBucket<UInt32>>);
static_assert(std::is_trivially_copyable_v<TimeSeriesHistogramBucket<DateTime64>>);

/// The bucket of the rate-family `timeSeriesHistogram{Rate,Increase,Delta,InstantRate,InstantDelta}ToGrid` states: ALL samples of the bucket
/// (unlike `timeSeriesHistogramLastToGrid`) as one slice of `samples_blob` (`TimeSeriesHistogramBucket` records), grown append-only.
struct TimeSeriesHistogramSamplesBucket
{
    TimeSeriesHistogramBlobRef samples{};
};
static_assert(std::is_trivially_copyable_v<TimeSeriesHistogramSamplesBucket>);

/// The sliding window of the rate-family aggregators: every sample keyed by timestamp in ascending order;
/// duplicate timestamps resolve last-write-wins like `addSample` (upstream has no rule: the TSDB rejects those).
template <typename TimestampType>
struct TimeSeriesHistogramWindowSamples
{
    std::map<TimestampType, TimeSeriesHistogramBucket<TimestampType>> samples; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Feeds one bucket's records to the window.
    template <typename State>
    void add(const State & state, const TimeSeriesHistogramSamplesBucket & bucket)
    {
        for (size_t i = 0; i < bucket.samples.size; ++i)
        {
            const auto & record = state.samples_blob[bucket.samples.offset + i];
            samples[record.newest_timestamp] = record;
        }
    }

    /// Drops the samples that left the window (the window is left-open: samples at or before the
    /// cutoff are out).
    void removeBefore(TimestampType cut_off)
    {
        samples.erase(samples.begin(), samples.upper_bound(cut_off));
    }
};

/// Base class for aggregate functions resampling native-histogram samples to a grid: the histogram sibling of `AggregateFunctionTimeseriesBase`
/// (which hardcodes Float buckets/results); `Traits::Bucket` selects the bucket kind, the derived class implements `doInsertResultInto`.
template <class FunctionImpl, class Traits>
class AggregateFunctionTimeseriesHistogramBase :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesHistogramBase<FunctionImpl, Traits>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesHistogramBase<FunctionImpl, Traits>>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;

    using Bucket = typename Traits::Bucket;

    String getName() const override
    {
        return Traits::getName();
    }

    /// Timeseries parameters may carry DecimalField (from toDateTime64(...) casts), whose
    /// default printed form collides with String literals — so we print parameters with ::Type.
    bool shouldPrintParametersWithTypes() const override { return true; }

    explicit AggregateFunctionTimeseriesHistogramBase(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_)
        : Base(
            argument_types_,
            parameters_,
            createResultType())
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

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * /*arena*/) const override
    {
        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto payload = HistogramPayloadViews::fromTuple(typeid_cast<const ColumnTuple &>(*columns[1]));
        addSample(place, timestamp_column.getData()[row_num], payload, row_num);
    }

    /// Batch add with a per-row state: a plain per-row loop (the conditional payload copy doesn't vectorize,
    /// so there is no multitarget kernel like in `AggregateFunctionTimeseriesBase`).
    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * /*arena*/,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto payload = HistogramPayloadViews::fromTuple(typeid_cast<const ColumnTuple &>(*columns[1]));
        const TimestampType * timestamp_data = timestamp_column.getData().data();

        for (size_t i = row_begin; i < row_end; ++i)
            if (places[i] && (!flags || flags[i]))
                addSample(places[i] + place_offset, timestamp_data[i], payload, i);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = nullptr;
        if (if_argument_pos >= 0)
        {
            const auto & flags_column = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if (row_end > flags_column.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "row_end {} is greater than flags column size {}", row_end, flags_column.size());

            flags = flags_column.data();
        }

        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto payload = HistogramPayloadViews::fromTuple(typeid_cast<const ColumnTuple &>(*columns[1]));
        const TimestampType * timestamp_data = timestamp_column.getData().data();

        for (size_t i = row_begin; i < row_end; ++i)
            if (!flags || flags[i])
                addSample(place, timestamp_data[i], payload, i);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = nullptr;
        if (if_argument_pos >= 0)
        {
            const auto & flags_column = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if (row_end > flags_column.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "row_end {} is greater than flags column size {}", row_end, flags_column.size());

            flags = flags_column.data();
        }

        const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
        const auto payload = HistogramPayloadViews::fromTuple(typeid_cast<const ColumnTuple &>(*columns[1]));
        const TimestampType * timestamp_data = timestamp_column.getData().data();

        /// Exclude rows where the histogram is NULL or the -If condition is false.
        for (size_t i = row_begin; i < row_end; ++i)
            if (!null_map[i] && (!flags || flags[i]))
                addSample(place, timestamp_data[i], payload, i);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    /// Newest-sample bucket: the newer bucket wins (rhs on a tie, matching `addSample`), its slices appended to the local
    /// blobs; rate-family bucket: the record slices are concatenated (ordering/duplicates resolved in the aggregator).
    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = *data(place);
        const auto & rhs_state = *data(rhs);
        state.buckets.reserve(rhs_state.buckets.size());
        for (const auto & rhs_entry : rhs_state.buckets)
        {
            const Bucket & rhs_bucket = rhs_entry.getMapped();

            if constexpr (std::is_same_v<Bucket, TimeSeriesHistogramSamplesBucket>)
            {
                if (rhs_bucket.samples.size == 0)
                    continue;

                auto & bucket = state.buckets[rhs_entry.getKey()];
                auto & blob = state.samples_blob;
                if (bucket.samples.offset + bucket.samples.size != blob.size())
                {
                    blob.insert(blob.end(), blob.begin() + bucket.samples.offset, blob.begin() + bucket.samples.offset + bucket.samples.size);
                    bucket.samples.offset = blob.size() - bucket.samples.size;
                }
                for (size_t i = 0; i < rhs_bucket.samples.size; ++i)
                {
                    auto record = rhs_state.samples_blob[rhs_bucket.samples.offset + i];
                    record.positive_spans = copySpansBlobSlice(state.positive_spans_blob, rhs_state.positive_spans_blob, record.positive_spans);
                    record.positive_values = copyFloatsBlobSlice(state.positive_values_blob, rhs_state.positive_values_blob, record.positive_values);
                    record.negative_spans = copySpansBlobSlice(state.negative_spans_blob, rhs_state.negative_spans_blob, record.negative_spans);
                    record.negative_values = copyFloatsBlobSlice(state.negative_values_blob, rhs_state.negative_values_blob, record.negative_values);
                    record.custom_values = copyFloatsBlobSlice(state.custom_values_blob, rhs_state.custom_values_blob, record.custom_values);
                    blob.push_back(record);
                    ++bucket.samples.size;
                }
            }
            else
            {
                if (!rhs_bucket.has_value)
                    continue;

                auto & bucket = state.buckets[rhs_entry.getKey()];
                if (bucket.has_value && bucket.newest_timestamp > rhs_bucket.newest_timestamp)
                    continue;

                bucket.newest_timestamp = rhs_bucket.newest_timestamp;
                bucket.flags = rhs_bucket.flags;
                bucket.schema = rhs_bucket.schema;
                bucket.zero_threshold = rhs_bucket.zero_threshold;
                bucket.count = rhs_bucket.count;
                bucket.sum = rhs_bucket.sum;
                bucket.zero_count = rhs_bucket.zero_count;
                bucket.positive_spans = copySpansBlobSlice(state.positive_spans_blob, rhs_state.positive_spans_blob, rhs_bucket.positive_spans);
                bucket.positive_values = copyFloatsBlobSlice(state.positive_values_blob, rhs_state.positive_values_blob, rhs_bucket.positive_values);
                bucket.negative_spans = copySpansBlobSlice(state.negative_spans_blob, rhs_state.negative_spans_blob, rhs_bucket.negative_spans);
                bucket.negative_values = copyFloatsBlobSlice(state.negative_values_blob, rhs_state.negative_values_blob, rhs_bucket.negative_values);
                bucket.custom_values = copyFloatsBlobSlice(state.custom_values_blob, rhs_state.custom_values_blob, rhs_bucket.custom_values);
                bucket.has_value = true;
            }
        }
    }

    /// Per bucket: the key, the scalars, then per array field the size and the raw elements read from the
    /// state's blobs. The `TimeSeriesHistogramBlobRef` offsets are state-local and are never serialized.
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(bucket_count, buf);

        const auto & state = *data(place);
        writeBinaryLittleEndian(state.buckets.size(), buf);

        for (const auto & entry : state.buckets)
        {
            writeBinaryLittleEndian(entry.getKey(), buf);
            const Bucket & bucket = entry.getMapped();

            if constexpr (std::is_same_v<Bucket, TimeSeriesHistogramSamplesBucket>)
            {
                /// Per bucket: the key, the record count, then per record the scalars and per array
                /// field the size and the raw elements read from the state's blobs.
                writeBinaryLittleEndian(bucket.samples.size, buf);
                for (size_t i = 0; i < bucket.samples.size; ++i)
                {
                    const auto & record = state.samples_blob[bucket.samples.offset + i];
                    writeBinaryLittleEndian(record.newest_timestamp, buf);
                    writeBinaryLittleEndian(record.flags, buf);
                    writeBinaryLittleEndian(record.schema, buf);
                    writeBinaryLittleEndian(record.zero_threshold, buf);
                    writeBinaryLittleEndian(record.count, buf);
                    writeBinaryLittleEndian(record.sum, buf);
                    writeBinaryLittleEndian(record.zero_count, buf);
                    serializeSpans(state.positive_spans_blob, record.positive_spans, buf);
                    serializeFloats(state.positive_values_blob, record.positive_values, buf);
                    serializeSpans(state.negative_spans_blob, record.negative_spans, buf);
                    serializeFloats(state.negative_values_blob, record.negative_values, buf);
                    serializeFloats(state.custom_values_blob, record.custom_values, buf);
                }
            }
            else
            {
                writeBinaryLittleEndian(bucket.newest_timestamp, buf);
                writeBinaryLittleEndian(bucket.flags, buf);
                writeBinaryLittleEndian(bucket.schema, buf);
                writeBinaryLittleEndian(bucket.zero_threshold, buf);
                writeBinaryLittleEndian(bucket.count, buf);
                writeBinaryLittleEndian(bucket.sum, buf);
                writeBinaryLittleEndian(bucket.zero_count, buf);
                writeBinary(bucket.has_value, buf);
                serializeSpans(state.positive_spans_blob, bucket.positive_spans, buf);
                serializeFloats(state.positive_values_blob, bucket.positive_values, buf);
                serializeSpans(state.negative_spans_blob, bucket.negative_spans, buf);
                serializeFloats(state.negative_values_blob, bucket.negative_values, buf);
                serializeFloats(state.custom_values_blob, bucket.custom_values, buf);
            }
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

        auto & state = *data(place);
        state.buckets.reserve(buckets_size);

        for (size_t i = 0; i < buckets_size; ++i)
        {
            size_t bucket_index = 0;
            readBinaryLittleEndian(bucket_index, buf);

            if (bucket_index >= bucket_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with index {} greater than bucket count {}", bucket_index, bucket_count);

            auto & bucket = state.buckets[bucket_index];

            if constexpr (std::is_same_v<Bucket, TimeSeriesHistogramSamplesBucket>)
            {
                UInt64 record_count = 0;
                readBinaryLittleEndian(record_count, buf);

                /// No a-priori record-count cap: a bucket can hold many samples, and a corrupt
                /// count fails at EOF like the array-field counts below.
                auto & blob = state.samples_blob;
                bucket.samples = TimeSeriesHistogramBlobRef{blob.size(), record_count};
                for (UInt64 record_index = 0; record_index < record_count; ++record_index)
                {
                    TimeSeriesHistogramBucket<TimestampType> record;
                    readBinaryLittleEndian(record.newest_timestamp, buf);
                    readBinaryLittleEndian(record.flags, buf);
                    readBinaryLittleEndian(record.schema, buf);
                    readBinaryLittleEndian(record.zero_threshold, buf);
                    readBinaryLittleEndian(record.count, buf);
                    readBinaryLittleEndian(record.sum, buf);
                    readBinaryLittleEndian(record.zero_count, buf);
                    record.positive_spans = deserializeSpans(state.positive_spans_blob, buf);
                    record.positive_values = deserializeFloats(state.positive_values_blob, buf);
                    record.negative_spans = deserializeSpans(state.negative_spans_blob, buf);
                    record.negative_values = deserializeFloats(state.negative_values_blob, buf);
                    record.custom_values = deserializeFloats(state.custom_values_blob, buf);
                    record.has_value = true;

                    /// Validate that the deserialized sample falls into this bucket's timestamp range.
                    if (!bucketTimeRange(bucket_index).contains(record.newest_timestamp))
                        throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Cannot deserialize data: timestamp {} is outside its bucket's range",
                            static_cast<Int64>(record.newest_timestamp));

                    blob.push_back(record);
                }
            }
            else
            {
                readBinaryLittleEndian(bucket.newest_timestamp, buf);
                readBinaryLittleEndian(bucket.flags, buf);
                readBinaryLittleEndian(bucket.schema, buf);
                readBinaryLittleEndian(bucket.zero_threshold, buf);
                readBinaryLittleEndian(bucket.count, buf);
                readBinaryLittleEndian(bucket.sum, buf);
                readBinaryLittleEndian(bucket.zero_count, buf);
                readBinary(bucket.has_value, buf);
                bucket.positive_spans = deserializeSpans(state.positive_spans_blob, buf);
                bucket.positive_values = deserializeFloats(state.positive_values_blob, buf);
                bucket.negative_spans = deserializeSpans(state.negative_spans_blob, buf);
                bucket.negative_values = deserializeFloats(state.negative_values_blob, buf);
                bucket.custom_values = deserializeFloats(state.custom_values_blob, buf);

                /// Validate that the deserialized sample falls into this bucket's timestamp range.
                if (bucket.has_value && !bucketTimeRange(bucket_index).contains(bucket.newest_timestamp))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Cannot deserialize data: timestamp {} is outside its bucket's range",
                        static_cast<Int64>(bucket.newest_timestamp));
            }
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        derived().doInsertResultInto(place, to);
    }

protected:
    /// Keep in sync with AggregateFunctionTimeseriesBase: the grid/timestamp members and methods below are a verbatim copy
    /// (access relaxed from private to protected; the Float-specific bucketing kernel and `doInsertResultInto` are not copied).

    struct State
    {
        /// Maps bucket index to the bucket's newest sample.
        TimeSeriesBucketsMap<Bucket> buckets;
        /// Append-only blob arrays holding the array fields of the buckets' samples.
        PODArray<TimeSeriesHistogramSpan> positive_spans_blob;
        PODArray<Float64> positive_values_blob;
        PODArray<TimeSeriesHistogramSpan> negative_spans_blob;
        PODArray<Float64> negative_values_blob;
        PODArray<Float64> custom_values_blob;
        /// Append-only blob of the rate-family buckets' sample records (unused by the newest-sample
        /// bucket of `timeSeriesHistogramLastToGrid`).
        PODArray<TimeSeriesHistogramBucket<TimestampType>> samples_blob;
    };

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

    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }

    /// Appends one result row (the histogram payload referenced by `bucket`) to the subcolumns of `tuple_to`
    /// (getTimeSeriesHistogramPayloadTupleType layout), for the non-NULL grid points.
    static void appendBucketToResultColumns(const State & state, const Bucket & bucket, ColumnTuple & tuple_to)
    {
        namespace Idx = TimeSeriesHistogramPayloadTupleIndex;
        typeid_cast<ColumnUInt8 &>(tuple_to.getColumn(Idx::Flags)).getData().push_back(bucket.flags);
        typeid_cast<ColumnInt8 &>(tuple_to.getColumn(Idx::Schema)).getData().push_back(bucket.schema);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroThreshold)).getData().push_back(bucket.zero_threshold);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Count)).getData().push_back(bucket.count);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Sum)).getData().push_back(bucket.sum);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroCount)).getData().push_back(bucket.zero_count);
        appendSpansToResultColumn(state.positive_spans_blob, bucket.positive_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveSpans)));
        appendFloatsToResultColumn(state.positive_values_blob, bucket.positive_values, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveValues)));
        appendSpansToResultColumn(state.negative_spans_blob, bucket.negative_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeSpans)));
        appendFloatsToResultColumn(state.negative_values_blob, bucket.negative_values, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeValues)));
        appendFloatsToResultColumn(state.custom_values_blob, bucket.custom_values, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::CustomValues)));
    }

    /// Decodes one sample record (see `TimeSeriesHistogramSamplesBucket`) into the kernel's `TimeSeriesFloatHistogram`,
    /// validating the layout (fail-close on corrupt data). Shared by the rate-family derived classes.
    static TimeSeriesFloatHistogram histogramFromRecord(const State & state, const TimeSeriesHistogramBucket<TimestampType> & record)
    {
        TimeSeriesFloatHistogram histogram;
        histogram.counter_reset_hint = TimeSeriesFloatHistogram::counterResetHintFromFlags(record.flags);
        histogram.schema = record.schema;
        histogram.zero_threshold = record.zero_threshold;
        histogram.count = record.count;
        histogram.sum = record.sum;
        histogram.zero_count = record.zero_count;

        auto assign_slice = [](const auto & blob, TimeSeriesHistogramBlobRef ref, auto & out)
        {
            out.assign(blob.begin() + ref.offset, blob.begin() + ref.offset + ref.size);
        };
        assign_slice(state.positive_spans_blob, record.positive_spans, histogram.positive_spans);
        assign_slice(state.positive_values_blob, record.positive_values, histogram.positive_buckets);
        assign_slice(state.negative_spans_blob, record.negative_spans, histogram.negative_spans);
        assign_slice(state.negative_values_blob, record.negative_values, histogram.negative_buckets);
        assign_slice(state.custom_values_blob, record.custom_values, histogram.custom_values);

        histogram.validateDecodedLayout();
        return histogram;
    }

    /// Appends a computed rate-family histogram to the subcolumns of `tuple_to` (payload tuple layout); the counter reset
    /// hint is re-encoded into the `flags` byte, all other flag bits 0 (a synthetic histogram, not a stored sample).
    static void appendHistogramToResultColumns(const TimeSeriesFloatHistogram & result, ColumnTuple & tuple_to)
    {
        namespace Idx = TimeSeriesHistogramPayloadTupleIndex;
        const UInt8 flags = static_cast<UInt8>(result.counter_reset_hint << TimeSeriesHistogramFlags::CounterResetHintShift);
        typeid_cast<ColumnUInt8 &>(tuple_to.getColumn(Idx::Flags)).getData().push_back(flags);
        typeid_cast<ColumnInt8 &>(tuple_to.getColumn(Idx::Schema)).getData().push_back(static_cast<Int8>(result.schema));
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroThreshold)).getData().push_back(result.zero_threshold);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Count)).getData().push_back(result.count);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Sum)).getData().push_back(result.sum);
        typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroCount)).getData().push_back(result.zero_count);
        appendSpansVectorToResultColumn(result.positive_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveSpans)));
        appendFloatsVectorToResultColumn(result.positive_buckets, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveValues)));
        appendSpansVectorToResultColumn(result.negative_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeSpans)));
        appendFloatsVectorToResultColumn(result.negative_buckets, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeValues)));
        appendFloatsVectorToResultColumn(result.custom_values, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::CustomValues)));
    }

    /// Compute the grid timestamp `start_timestamp + grid_index * step` in unsigned 64-bit arithmetic: avoids signed overflow/UBSAN
    /// on adversarial boundary values, preserving the signed accumulator's bit pattern for normal inputs.
    TimestampType timestampAtIndex(size_t grid_index) const
    {
        chassert(grid_index < grid_size);
        const UInt64 start_bits = static_cast<UInt64>(toInt64(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(step);
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_index) * step_bits;
        const TimestampType grid_point = static_cast<TimestampType>(static_cast<Int64>(result_bits));
        return grid_point;
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

    /// End timestamp of bucket `bucket_index`: `first_bucket_end_time` plus the end-spacings (`even/odd_bucket_step`) of
    /// buckets 1..bucket_index; wrapping unsigned arithmetic recovers the in-range end modulo 2^64 on extreme grids.
    TimestampType ALWAYS_INLINE bucketEndTimestamp(size_t bucket_index) const
    {
        chassert(bucket_index < bucket_count);
        const UInt64 num_even_buckets = bucket_index / 2;
        const UInt64 num_odd_buckets = bucket_index - num_even_buckets;
        const UInt64 bucket_end_time = static_cast<UInt64>(toInt64(first_bucket_end_time))
            + num_odd_buckets * odd_bucket_step + num_even_buckets * even_bucket_step;
        return static_cast<TimestampType>(static_cast<Int64>(bucket_end_time));
    }

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

    /// Drops buckets that left grid point `grid_index`'s window from the front of the sliding `aggregator`; window-aligned
    /// buckets are fully in or out, so the bucket's latest timestamp decides against the cutoff `grid_timestamp - window`.
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

    /// Density threshold for `doInsertResultInto`: at or above it, range-scanning bucket indices beats collect-and-sort;
    /// kept in sync with `AggregateFunctionTimeseriesBase` (tuned by the `timeseries_to_grid_range_scan_vs_std_sort` example).
    static constexpr double BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN = 0.35;

private:
    /// `HashMap` relocates cells with `memcpy`, so it requires position-independent buckets: trivially
    /// copyable or declaring `is_position_independent`.
    static_assert(
        std::is_trivially_copyable_v<Bucket> || requires { requires Bucket::is_position_independent; },
        "Bucket must be position independent (memmove-able) to be stored in a HashMap");

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(getTimeSeriesHistogramPayloadTupleType()));
    }

    /// Upper bound on the number of grid points (the output array length), preventing huge allocations from adversarial
    /// grids; 16M is consistent with `MAX_ARRAY_SIZE` of other aggregates (`AggregateFunctionGroupArray`, etc.).
    static constexpr size_t MAX_GRID_SIZE = 0xFFFFFF;

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

    /// Number of buckets in grid point #0's window: usually `buckets_per_window`, fewer when leading buckets lie entirely
    /// below the smallest representable timestamp and are dropped (keeping every bucket's end timestamp in range).
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

    /// End-to-end spacing of even- or odd-indexed buckets: equals the bucket width, except one bucket per step with
    /// `window < step` keeps ends spaced by `step` though narrower. Static - used once, by the constructor.
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

    /// Width of bucket #0: `even_bucket_width`, shortened when bucket #0's start falls below the smallest representable
    /// timestamp (bucket #0 then covers everything from that minimum up; the shortened width fits `IntervalType`).
    static IntervalType firstBucketWidth(IntervalType window, IntervalType even_bucket_width, TimestampType first_bucket_end_time)
    {
        if (window == 0)
        {
            /// window == 0 means no buckets at all, so there is no bucket #0.
            return 0;
        }
        /// The width is always at least 1: `even_bucket_width >= 1` (see `bucketWidth`), and `clamped_width >= 1`
        /// because `bucketsPerFirstWindow` drops the buckets lying entirely below the type minimum.
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

    static constexpr size_t NO_BUCKET = -1;

    /// What `addSample` does with a sample: add it to bucket `bucket_index`, or skip it (`bucket_index == NO_BUCKET`);
    /// `time_range` is the bucket's time range. Keep in sync with `AggregateFunctionTimeseriesBase`.
    struct SampleClass
    {
        size_t bucket_index;
        BucketTimeRange time_range;
    };

    /// Classifies a sample: the bucket index to add it to (or NO_BUCKET to skip it), plus its `time_range` for run
    /// detection; with `ReturnType == size_t` the range is not computed and only the index is returned.
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

        /// A sample before bucket #0's start is out of window for every grid point; a start clamped to the
        /// type minimum rejects nothing - then every timestamp is in window.
        if (timestamp < first_bucket_start_time)
        {
            if constexpr (return_index)
                return NO_BUCKET;
            else if (bucket_count == 0)
                return {NO_BUCKET, {minTimestamp(), maxTimestamp()}};  /// A grid without buckets (`window == 0`) rejects everything.
            else
                return {NO_BUCKET, {minTimestamp(), static_cast<TimestampType>(toInt64(first_bucket_start_time) - 1)}};  /// The check passed, so no underflow
        }

        /// All the arithmetic is 64-bit for any grid parameters: a difference of two Int64 timestamps can overflow
        /// Int64, but every value computed here is non-negative and less than 2^64, so UInt64 arithmetic is exact.
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

            /// A sample out of its grid point's window is out of every window (later windows start higher); possible only when
            /// `window < step`, so the bounds fit Int64 (a zero window rejects every sample of this branch here).
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
            /// A grid without buckets (`window == 0`) accepts nothing; the only sample reaching this branch is one
            /// exactly at `start` (everything below was rejected against bucket #0's start, which equals `start` then).
            if (bucket_count == 0)
            {
                if constexpr (return_index)
                    return NO_BUCKET;
                else
                    return {NO_BUCKET, {minTimestamp(), maxTimestamp()}};
            }

            /// The sample's grid point is #0, and its bucket is one of the leading buckets (1 or 2 per whole step down from
            /// bucket #(leading_buckets - 1)); it can't go below 0 (see `bucketsPerFirstWindow`), and 0 <= offset_below_start < window.
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

    /// Returns the index of the bucket a sample at `timestamp` contributes to, or NO_BUCKET if it can't
    /// contribute to any bucket because it's too early, too late, or already out of window.
    size_t ALWAYS_INLINE bucketIndexForTimestamp(const TimestampType timestamp) const
    {
        return classifySample<size_t>(timestamp);
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

    /// Read-only views of the payload tuple's array-typed subcolumns, built once per add/addBatch call so
    /// the per-row loop pays no repeated typeid_cast or subcolumn lookups.
    struct HistogramFloatArrayView
    {
        const ColumnArray::Offsets * offsets;
        const PaddedPODArray<Float64> * values;
    };

    struct HistogramSpansView
    {
        const ColumnArray::Offsets * offsets;
        const PaddedPODArray<Int32> * span_offsets;
        const PaddedPODArray<UInt32> * span_lengths;
    };

    struct HistogramPayloadViews
    {
        const PaddedPODArray<UInt8> * flags;
        const PaddedPODArray<Int8> * schema;
        const PaddedPODArray<Float64> * zero_threshold;
        const PaddedPODArray<Float64> * count;
        const PaddedPODArray<Float64> * sum;
        const PaddedPODArray<Float64> * zero_count;
        HistogramSpansView positive_spans;
        HistogramFloatArrayView positive_values;
        HistogramSpansView negative_spans;
        HistogramFloatArrayView negative_values;
        HistogramFloatArrayView custom_values;

        static HistogramFloatArrayView floatArrayView(const IColumn & column)
        {
            const auto & array = typeid_cast<const ColumnArray &>(column);
            return {&array.getOffsets(), &typeid_cast<const ColumnFloat64 &>(array.getData()).getData()};
        }

        static HistogramSpansView spansView(const IColumn & column)
        {
            const auto & array = typeid_cast<const ColumnArray &>(column);
            const auto & tuple = typeid_cast<const ColumnTuple &>(array.getData());
            return {&array.getOffsets(),
                &typeid_cast<const ColumnInt32 &>(tuple.getColumn(0)).getData(),
                &typeid_cast<const ColumnUInt32 &>(tuple.getColumn(1)).getData()};
        }

        static HistogramPayloadViews fromTuple(const ColumnTuple & tuple)
        {
            namespace Idx = TimeSeriesHistogramPayloadTupleIndex;
            return {
                &typeid_cast<const ColumnUInt8 &>(tuple.getColumn(Idx::Flags)).getData(),
                &typeid_cast<const ColumnInt8 &>(tuple.getColumn(Idx::Schema)).getData(),
                &typeid_cast<const ColumnFloat64 &>(tuple.getColumn(Idx::ZeroThreshold)).getData(),
                &typeid_cast<const ColumnFloat64 &>(tuple.getColumn(Idx::Count)).getData(),
                &typeid_cast<const ColumnFloat64 &>(tuple.getColumn(Idx::Sum)).getData(),
                &typeid_cast<const ColumnFloat64 &>(tuple.getColumn(Idx::ZeroCount)).getData(),
                spansView(tuple.getColumn(Idx::PositiveSpans)),
                floatArrayView(tuple.getColumn(Idx::PositiveValues)),
                spansView(tuple.getColumn(Idx::NegativeSpans)),
                floatArrayView(tuple.getColumn(Idx::NegativeValues)),
                floatArrayView(tuple.getColumn(Idx::CustomValues)),
            };
        }
    };

    /// Appends the `row`-th array of the view to the blob and returns the slice it landed in.
    static TimeSeriesHistogramBlobRef copyFloatsToBlob(PODArray<Float64> & blob, const HistogramFloatArrayView & view, size_t row)
    {
        const size_t begin = row == 0 ? 0 : (*view.offsets)[row - 1];
        const size_t end = (*view.offsets)[row];
        TimeSeriesHistogramBlobRef ref{blob.size(), end - begin};
        blob.insert(view.values->begin() + begin, view.values->begin() + end);
        return ref;
    }

    static TimeSeriesHistogramBlobRef copySpansToBlob(PODArray<TimeSeriesHistogramSpan> & blob, const HistogramSpansView & view, size_t row)
    {
        const size_t begin = row == 0 ? 0 : (*view.offsets)[row - 1];
        const size_t end = (*view.offsets)[row];
        TimeSeriesHistogramBlobRef ref{blob.size(), end - begin};
        for (size_t i = begin; i < end; ++i)
            blob.push_back(TimeSeriesHistogramSpan{(*view.span_offsets)[i], (*view.span_lengths)[i]});
        return ref;
    }

    /// Appends another state's slice to the blob (on merge) and returns the slice it landed in.
    static TimeSeriesHistogramBlobRef copyFloatsBlobSlice(PODArray<Float64> & blob, const PODArray<Float64> & rhs_blob, TimeSeriesHistogramBlobRef rhs_ref)
    {
        TimeSeriesHistogramBlobRef ref{blob.size(), rhs_ref.size};
        blob.insert(rhs_blob.begin() + rhs_ref.offset, rhs_blob.begin() + rhs_ref.offset + rhs_ref.size);
        return ref;
    }

    static TimeSeriesHistogramBlobRef copySpansBlobSlice(PODArray<TimeSeriesHistogramSpan> & blob, const PODArray<TimeSeriesHistogramSpan> & rhs_blob, TimeSeriesHistogramBlobRef rhs_ref)
    {
        TimeSeriesHistogramBlobRef ref{blob.size(), rhs_ref.size};
        blob.insert(rhs_blob.begin() + rhs_ref.offset, rhs_blob.begin() + rhs_ref.offset + rhs_ref.size);
        return ref;
    }

    static void serializeFloats(const PODArray<Float64> & blob, TimeSeriesHistogramBlobRef ref, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(ref.size, buf);
        for (size_t i = 0; i < ref.size; ++i)
            writeBinaryLittleEndian(blob[ref.offset + i], buf);
    }

    static void serializeSpans(const PODArray<TimeSeriesHistogramSpan> & blob, TimeSeriesHistogramBlobRef ref, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(ref.size, buf);
        for (size_t i = 0; i < ref.size; ++i)
        {
            writeBinaryLittleEndian(blob[ref.offset + i].offset, buf);
            writeBinaryLittleEndian(blob[ref.offset + i].length, buf);
        }
    }

    static TimeSeriesHistogramBlobRef deserializeFloats(PODArray<Float64> & blob, ReadBuffer & buf)
    {
        UInt64 size = 0;
        readBinaryLittleEndian(size, buf);
        TimeSeriesHistogramBlobRef ref{blob.size(), size};
        for (UInt64 i = 0; i < size; ++i)
        {
            Float64 value = 0;
            readBinaryLittleEndian(value, buf);
            blob.push_back(value);
        }
        return ref;
    }

    static TimeSeriesHistogramBlobRef deserializeSpans(PODArray<TimeSeriesHistogramSpan> & blob, ReadBuffer & buf)
    {
        UInt64 size = 0;
        readBinaryLittleEndian(size, buf);
        TimeSeriesHistogramBlobRef ref{blob.size(), size};
        for (UInt64 i = 0; i < size; ++i)
        {
            TimeSeriesHistogramSpan span{};
            readBinaryLittleEndian(span.offset, buf);
            readBinaryLittleEndian(span.length, buf);
            blob.push_back(span);
        }
        return ref;
    }

    static void appendFloatsToResultColumn(const PODArray<Float64> & blob, TimeSeriesHistogramBlobRef ref, ColumnArray & array_to)
    {
        auto & data_to = typeid_cast<ColumnFloat64 &>(array_to.getData()).getData();
        data_to.insert(blob.begin() + ref.offset, blob.begin() + ref.offset + ref.size);
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? ref.size : offsets_to.back() + ref.size);
    }

    static void appendSpansToResultColumn(const PODArray<TimeSeriesHistogramSpan> & blob, TimeSeriesHistogramBlobRef ref, ColumnArray & array_to)
    {
        auto & tuple_to = typeid_cast<ColumnTuple &>(array_to.getData());
        auto & span_offsets_to = typeid_cast<ColumnInt32 &>(tuple_to.getColumn(0)).getData();
        auto & span_lengths_to = typeid_cast<ColumnUInt32 &>(tuple_to.getColumn(1)).getData();
        for (size_t i = 0; i < ref.size; ++i)
        {
            span_offsets_to.push_back(blob[ref.offset + i].offset);
            span_lengths_to.push_back(blob[ref.offset + i].length);
        }
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? ref.size : offsets_to.back() + ref.size);
    }

    /// The std::vector sibling of `appendFloatsToResultColumn`/`appendSpansToResultColumn`: appends a
    /// computed histogram's array field (not a blob slice) to the result column.
    static void appendFloatsVectorToResultColumn(const std::vector<Float64> & values, ColumnArray & array_to) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        auto & data_to = typeid_cast<ColumnFloat64 &>(array_to.getData()).getData();
        data_to.insert(values.begin(), values.end());
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? values.size() : offsets_to.back() + values.size());
    }

    static void appendSpansVectorToResultColumn(const std::vector<TimeSeriesHistogramSpan> & spans, ColumnArray & array_to) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        auto & tuple_to = typeid_cast<ColumnTuple &>(array_to.getData());
        auto & span_offsets_to = typeid_cast<ColumnInt32 &>(tuple_to.getColumn(0)).getData();
        auto & span_lengths_to = typeid_cast<ColumnUInt32 &>(tuple_to.getColumn(1)).getData();
        for (const auto & span : spans)
        {
            span_offsets_to.push_back(span.offset);
            span_lengths_to.push_back(span.length);
        }
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? spans.size() : offsets_to.back() + spans.size());
    }

    /// Adds one sample to its bucket: the newest-sample bucket keeps the newest (a stale sample loses, ties are last-write-wins,
    /// superseded slices stay in the blobs); the rate-family bucket appends every sample to the bucket's `samples_blob` slice.
    void ALWAYS_INLINE addSample(AggregateDataPtr __restrict place, TimestampType timestamp, const HistogramPayloadViews & payload, size_t row_num) const
    {
        const size_t bucket_index = bucketIndexForTimestamp(timestamp);
        if (bucket_index == NO_BUCKET)
            return;  /// The sample can't contribute to any bucket.

        auto & state = *data(place);
        auto & bucket = state.buckets[bucket_index];

        if constexpr (std::is_same_v<Bucket, TimeSeriesHistogramSamplesBucket>)
        {
            /// The rate-family bucket: keep every sample; the bucket's slice grows in place at the blob's
            /// end, otherwise it is copied forward (the superseded slice stays in the blob).
            auto & blob = state.samples_blob;
            if (bucket.samples.offset + bucket.samples.size != blob.size())
            {
                blob.insert(blob.end(), blob.begin() + bucket.samples.offset, blob.begin() + bucket.samples.offset + bucket.samples.size);
                bucket.samples.offset = blob.size() - bucket.samples.size;
            }
            TimeSeriesHistogramBucket<TimestampType> record;
            record.newest_timestamp = timestamp;
            record.flags = (*payload.flags)[row_num];
            record.schema = (*payload.schema)[row_num];
            record.zero_threshold = (*payload.zero_threshold)[row_num];
            record.count = (*payload.count)[row_num];
            record.sum = (*payload.sum)[row_num];
            record.zero_count = (*payload.zero_count)[row_num];
            record.positive_spans = copySpansToBlob(state.positive_spans_blob, payload.positive_spans, row_num);
            record.positive_values = copyFloatsToBlob(state.positive_values_blob, payload.positive_values, row_num);
            record.negative_spans = copySpansToBlob(state.negative_spans_blob, payload.negative_spans, row_num);
            record.negative_values = copyFloatsToBlob(state.negative_values_blob, payload.negative_values, row_num);
            record.custom_values = copyFloatsToBlob(state.custom_values_blob, payload.custom_values, row_num);
            record.has_value = true;
            blob.push_back(record);
            ++bucket.samples.size;
            return;
        }
        else
        {
            if (bucket.has_value && timestamp < bucket.newest_timestamp)
                return;  /// A stale sample loses; the bucket keeps its newest sample.

            bucket.newest_timestamp = timestamp;
            bucket.flags = (*payload.flags)[row_num];
            bucket.schema = (*payload.schema)[row_num];
            bucket.zero_threshold = (*payload.zero_threshold)[row_num];
            bucket.count = (*payload.count)[row_num];
            bucket.sum = (*payload.sum)[row_num];
            bucket.zero_count = (*payload.zero_count)[row_num];
            bucket.positive_spans = copySpansToBlob(state.positive_spans_blob, payload.positive_spans, row_num);
            bucket.positive_values = copyFloatsToBlob(state.positive_values_blob, payload.positive_values, row_num);
            bucket.negative_spans = copySpansToBlob(state.negative_spans_blob, payload.negative_spans, row_num);
            bucket.negative_values = copyFloatsToBlob(state.negative_values_blob, payload.negative_values, row_num);
            bucket.custom_values = copyFloatsToBlob(state.custom_values_blob, payload.custom_values, row_num);
            bucket.has_value = true;
        }
    }

    const FunctionImpl & derived() const
    {
        return static_cast<const FunctionImpl &>(*this);
    }
};

}
