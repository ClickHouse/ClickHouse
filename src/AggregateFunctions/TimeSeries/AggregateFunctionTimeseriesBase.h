#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <type_traits>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/UnorderedMapWithMemoryTracking.h>


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

    String getName() const override
    {
        return Traits::getName();
    }

    using Bucket = typename Traits::Bucket;

    struct State
    {
        /// Maps bucket index to the set of all timestamps and values
        UnorderedMapWithMemoryTracking<size_t, Bucket> buckets;
    };

    explicit AggregateFunctionTimeseriesBase(const DataTypes & argument_types_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_)
        : Base(
            argument_types_,
            {
                /// Normalize all parameters to decimals with the same scale as the scale of timestamp argument
                DecimalField<Decimal64>(start_timestamp_, timestamp_scale_),
                DecimalField<Decimal64>(end_timestamp_, timestamp_scale_),
                DecimalField<Decimal64>(step_, timestamp_scale_),
                DecimalField<Decimal64>(window_, timestamp_scale_)
            },
            createResultType())
        , bucket_count(bucketCount(start_timestamp_, end_timestamp_, step_))
        , start_timestamp(start_timestamp_)
        , end_timestamp(static_cast<TimestampType>(start_timestamp_ + (bucket_count - 1) * step_))  /// Align end timestamp down by step
        , step(step_)
        , window(window_)
        , timestamp_scale_multiplier(static_cast<TimestampType>(DecimalUtils::scaleMultiplier<Int64>(timestamp_scale_)))
    {
        if (window < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window should be non-negative");
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ValueType>>()));
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

    /// Upper bound on the number of buckets that can be allocated for a single grid.
    /// This prevents absurdly large grids (e.g. from adversarial input that passes extreme
    /// timestamps and a tiny step) from allocating huge amounts of memory or triggering
    /// undefined behaviour in downstream arithmetic. 16M is consistent with the
    /// `MAX_ARRAY_SIZE` used by other aggregate functions (`AggregateFunctionGroupArray`,
    /// `AggregateFunctionIntervalLengthSum`, etc.).
    static constexpr size_t MAX_BUCKET_COUNT = 0xFFFFFF;

    static size_t bucketCount(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (end_timestamp < start_timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

        if (end_timestamp == start_timestamp)
            return 1;

        if (step <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

        /// Compute the bucket count using unsigned 64-bit arithmetic to avoid signed overflow
        /// when `start_timestamp` is very negative (e.g. `DateTime64` near `INT64_MIN`
        /// produced by an adversarial fuzzer-generated query). Since we already verified
        /// `end_timestamp >= start_timestamp`, the unsigned difference is the correct
        /// mathematical value for any representable input.
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 end_bits = static_cast<UInt64>(static_cast<Int64>(end_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));

        const UInt64 diff = end_bits - start_bits;
        const UInt64 quotient = diff / step_bits;

        /// Check the cap on `quotient` rather than on `quotient + 1`. With
        /// `start = INT64_MIN`, `end = INT64_MAX`, `step = 1`, `diff` is `UINT64_MAX`,
        /// `quotient` is `UINT64_MAX`, and `quotient + 1` wraps to `0` — bypassing the
        /// cap and returning `bucket_count = 0`, which later fires `chassert(index <
        /// bucket_count)` inside `bucketIndexForTimestamp`. Since `MAX_BUCKET_COUNT`
        /// is well below `UINT64_MAX`, checking `quotient >= MAX_BUCKET_COUNT` is
        /// equivalent to the original `count > MAX_BUCKET_COUNT` in the safe range,
        /// but remains correct at the `UInt64` overflow boundary.
        if (quotient >= MAX_BUCKET_COUNT)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Number of buckets in the timeseries grid exceeds maximum ({}). "
                "Consider narrowing the [start, end] range or increasing the step.",
                MAX_BUCKET_COUNT);

        return static_cast<size_t>(quotient + 1);
    }

    size_t bucketIndexForTimestamp(const TimestampType timestamp) const
    {
        chassert(timestamp <= end_timestamp);
        if (timestamp <= start_timestamp)
            return 0;

        /// Use unsigned arithmetic to avoid signed overflow when `start_timestamp` is very
        /// negative. Both operands are first converted to `Int64` (the native type of every
        /// supported `TimestampType`), then reinterpreted as `UInt64`. Since the early-return
        /// above guarantees `timestamp > start_timestamp`, the unsigned subtraction produces
        /// the correct non-negative delta in all cases.
        const UInt64 ts_bits = static_cast<UInt64>(static_cast<Int64>(timestamp));
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));

        const UInt64 diff = ts_bits - start_bits;
        /// Overflow-safe ceil-division. The classic `(diff + step - 1) / step` formula can
        /// overflow modulo `2^64` when `diff` is close to `UINT64_MAX` (reachable for extreme
        /// inputs such as `start_timestamp` near `INT64_MIN` and a large `step`). The
        /// mathematically-equivalent form `diff / step + (diff % step != 0)` never exceeds
        /// `diff` itself and therefore cannot overflow `UInt64`.
        const size_t index = static_cast<size_t>(diff / step_bits + (diff % step_bits != 0));
        chassert(index < bucket_count);
        return index;
    }

    /// Compute the grid timestamp for a given bucket index, i.e. `start_timestamp + index * step`.
    /// Uses unsigned 64-bit arithmetic internally to avoid signed overflow on extreme inputs
    /// (`start_timestamp` near `INT64_MIN` together with a `step` near `INT64_MAX`). The final
    /// cast back to `TimestampType` preserves the same bit pattern that the signed accumulator
    /// `current_timestamp += step` would produce for normal inputs, but does not trigger UBSAN
    /// on the adversarial boundary values generated by the AST fuzzer.
    TimestampType timestampAtIndex(size_t index) const
    {
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(index) * step_bits;
        return static_cast<TimestampType>(static_cast<Int64>(result_bits));
    }

    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }

    void create(AggregateDataPtr __restrict place) const override  /// NOLINT
    {
        new (place) State{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place)->~State();
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value) const
    {
        if (timestamp + window + step < start_timestamp || timestamp > end_timestamp)
            return;

        const size_t index = bucketIndexForTimestamp(timestamp);
        auto & bucket = data(place)->buckets[index];
        bucket.add(timestamp, value);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (Traits::array_arguments)
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
        if (Traits::array_arguments)
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

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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
            FunctionImpl::serializeBucket(bucket.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        size_t size;
        readBinaryLittleEndian(size, buf);

        if (size != bucket_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot deserialize data with different bucket count");

        size_t buckets_size;
        readBinaryLittleEndian(buckets_size, buf);

        if (buckets_size > bucket_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more buckets than expected");

        data(place)->buckets.reserve(buckets_size);

        for (size_t i = 0; i < buckets_size; ++i)
        {
            size_t index;
            readBinaryLittleEndian(index, buf);

            if (index >= bucket_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with index {} greater than bucket count {}", index, bucket_count);

            auto & bucket = data(place)->buckets[index];

            derived().deserializeBucket(bucket, buf, index);
        }
    }

    /// Validates that timestamp is in the range [0, end_timestamp] and that bucket_index is correct for the timestamp.
    /// We don't check that timestamp is >= start_timestamp because 0th bucket might contain older timestamps for handling staleness.
    /// This method is used in deserialization to check that the data is consistent.
    void checkTimestampInRange(const TimestampType timestamp, const size_t bucket_index) const
    {
        if (timestamp > end_timestamp)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with timestamp {} greater than end timestamp {}",
                static_cast<Int64>(timestamp), static_cast<Int64>(end_timestamp));

        const size_t expected_bucket_index = bucketIndexForTimestamp(timestamp);

        if (bucket_index != expected_bucket_index)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different bucket index for timestamp {}: expected {}, got {}",
                static_cast<Int64>(timestamp), expected_bucket_index, bucket_index);
    }

    const FunctionImpl & derived() const
    {
        return static_cast<const FunctionImpl &>(*this);
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
        data_to.reserve(data_to.size() + batch_size * bucket_count);
        nulls_to.reserve(nulls_to.size() + batch_size * bucket_count);

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
    static constexpr UInt16 FORMAT_VERSION = FunctionImpl::FORMAT_VERSION;

    const size_t bucket_count{};            /// Number of buckets in the grid calculated from start_timestamp, end_timestamp and step
    const TimestampType start_timestamp{};  /// First timestamp in the grid
    const TimestampType end_timestamp{};    /// Last timestamp in the grid. NOTE: It is aligned down by step relative to start_timestamp
    const IntervalType step{};              /// Grid step (IntervalType represent time difference between timestamps)
    const IntervalType window{};            /// Window size used by derived functions (e.g. for rate and delta calculations)
    const TimestampType timestamp_scale_multiplier{};   /// When timestamps are in DateTime64 (which is Decimal with some scale)
                                                        /// this multiplier is used for calculation rate per second (i.e. it is 1000 for
                                                        /// milliseconds or 1e6 for microseconds)
};

}
