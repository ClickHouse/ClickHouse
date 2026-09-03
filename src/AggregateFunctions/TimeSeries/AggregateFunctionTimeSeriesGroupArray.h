#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/Arena.h>
#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/sort.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Aggregate function sorting pairs (timestamp, values) by timestamp.
/// If there are pairs with the same timestamp then the function keeps only a pair with the biggest value,
/// where a NaN value loses to any other value (see `timeseriesMaxValueForDuplicateTimestamp`).
/// Samples can be passed in three ways: as two scalar arguments (timestamp, value),
/// as two arrays (Array(timestamp), Array(value)), or as a single array of pairs Array(Tuple(timestamp, value)).
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeSeriesGroupArray final :
    public IAggregateFunctionHelper<AggregateFunctionTimeSeriesGroupArray<TimestampType, ValueType>>
{
public:
    using Base = IAggregateFunctionHelper<AggregateFunctionTimeSeriesGroupArray<TimestampType, ValueType>>;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;
    using ColVecResultType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override
    {
        return "timeSeriesGroupArray";
    }

    struct Element
    {
        TimestampType timestamp;
        ValueType value;
    };

    /// Small states are kept in the aggregation arena; bigger ones move to the general allocator, which,
    /// unlike the arena, reclaims the previous buffer while the array grows.
    using ElementsAllocator = MixedAlignedArenaAllocator<alignof(Element), 4096>;
    using Elements = PODArray<Element, 32, ElementsAllocator>;

    /// Stores all samples.
    struct Data
    {
        /// The samples, sorted by timestamp and deduplicated whenever `sorted` is true.
        Elements elements;
        /// Cleared by an out-of-order `add`; while set, timestamps in `elements` are strictly increasing.
        bool sorted = true;

        void add(TimestampType timestamp, ValueType value, Arena * arena)
        {
            /// Samples usually arrive in timestamp order, hence `[[unlikely]]`.
            if (!elements.empty() && timestamp <= elements.back().timestamp) [[unlikely]]
            {
                Element & last = elements.back();
                if (timestamp == last.timestamp)
                {
                    last.value = timeseriesMaxValueForDuplicateTimestamp(last.value, value);
                    return;
                }
                sorted = false;
            }
            elements.push_back(Element{.timestamp = timestamp, .value = value}, arena);
        }

        void merge(const Data & rhs, Arena * arena)
        {
            if (rhs.elements.empty())
                return;

            if (elements.empty())
            {
                elements.assign(rhs.elements.begin(), rhs.elements.end(), arena);
                sorted = rhs.sorted;
                sort(arena);
                return;
            }

            sort(arena);

            /// A rare unsorted argument is sorted into a copy: `rhs` belongs to another state and is kept intact.
            const Elements * rhs_elements = &rhs.elements;
            Elements sorted_rhs_elements;
            if (!rhs.sorted) [[unlikely]]
            {
                sorted_rhs_elements.assign(rhs.elements.begin(), rhs.elements.end(), arena);
                sorted_rhs_elements.resize_exact(
                    sortElements(sorted_rhs_elements.data(), sorted_rhs_elements.size()), arena);
                rhs_elements = &sorted_rhs_elements;
            }

            /// Partial states often cover disjoint time ranges - then the merge is a plain append or prepend.
            if (elements.back().timestamp < rhs_elements->front().timestamp)
            {
                elements.insert(rhs_elements->begin(), rhs_elements->end(), arena);
                return;
            }

            if (rhs_elements->back().timestamp < elements.front().timestamp)
            {
                Elements prepended;
                prepended.reserve_exact(rhs_elements->size() + elements.size(), arena);
                prepended.insert_assume_reserved(rhs_elements->begin(), rhs_elements->end());
                prepended.insert_assume_reserved(elements.begin(), elements.end());
                elements.swap(prepended, arena);
                return;
            }

            Elements merged;
            merged.resize_exact(elements.size() + rhs_elements->size(), arena);
            std::merge(
                elements.begin(), elements.end(), rhs_elements->begin(), rhs_elements->end(), merged.begin(), lessByTimestamp);
            merged.resize_exact(deduplicateSorted(merged.data(), merged.size()), arena);
            elements.swap(merged, arena);
        }

        /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
        void sort(Arena * arena)
        {
            if (sorted)
                return;
            elements.resize_exact(sortElements(elements.data(), elements.size()), arena);
            sorted = true;
        }

        static bool lessByTimestamp(const Element & lhs, const Element & rhs)
        {
            return lhs.timestamp < rhs.timestamp;
        }

        /// Collapses each equal-timestamp run of a sorted range into one element and returns how many elements
        /// are left at the beginning of the range.
        static size_t deduplicateSorted(Element * elements, size_t size)
        {
            if (size == 0)
                return 0;

            size_t last_unique = 0;
            for (size_t i = 1; i < size; ++i)
            {
                if (elements[i].timestamp == elements[last_unique].timestamp)
                    elements[last_unique].value
                        = timeseriesMaxValueForDuplicateTimestamp(elements[last_unique].value, elements[i].value);
                else
                    elements[++last_unique] = elements[i];
            }
            return last_unique + 1;
        }

        static size_t sortElements(Element * elements, size_t size)
        {
            ::sort(elements, elements + size, lessByTimestamp);
            return deduplicateSorted(elements, size);
        }
    };

    explicit AggregateFunctionTimeSeriesGroupArray(const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , array_of_pairs_argument(argument_types_.size() == 1)
        , array_arguments(!array_of_pairs_argument && (argument_types_[1]->getTypeId() == TypeIndex::Array))
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        /// With the single argument form the result type is the same as the type of that argument.
        if (argument_types_.size() == 1)
            return argument_types_[0];

        const bool arrays_passed = (argument_types_[1]->getTypeId() == TypeIndex::Array);
        const auto & timestamp_type = arrays_passed ? typeid_cast<const DataTypeArray *>(argument_types_[0].get())->getNestedType() : argument_types_[0];
        const auto & value_type = arrays_passed ? typeid_cast<const DataTypeArray *>(argument_types_[1].get())->getNestedType() : argument_types_[1];
        return std::make_shared<DataTypeArray>(make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}));
    }

    bool allocatesMemoryInArena() const override { return true; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    void create(AggregateDataPtr __restrict place) const override   /// NOLINT(readability-non-const-parameter)
    {
        new (place) Data{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    static Data & data(AggregateDataPtr __restrict place)   /// NOLINT(readability-non-const-parameter)
    {
        return *reinterpret_cast<Data *>(place);
    }

    static const Data & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const Data *>(place);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value, Arena * arena) const
    {
        Data & data = this->data(place);
        data.add(timestamp, value, arena);
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE reserveAdd(AggregateDataPtr __restrict place, size_t num_elements_to_add, Arena * arena) const
    {
        Data & data = this->data(place);
        data.elements.reserve(data.elements.size() + num_elements_to_add, arena);
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
            add(place, timestamp_column.getData()[row_num], value_column.getData()[row_num], arena);
        }
    }

    void addMany(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, size_t start, size_t end, Arena * arena) const
    {
        reserveAdd(place, end - start, arena);
        for (size_t i = start; i < end; ++i)
            add(place, timestamp_ptr[i], value_ptr[i], arena);
    }

    void addManyNotNull(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict null_map, size_t start, size_t end, Arena * arena) const
    {
        reserveAdd(place, end - start, arena);
        for (size_t i = start; i < end; ++i)
            if (!null_map[i])
                add(place, timestamp_ptr[i], value_ptr[i], arena);
    }

    void addManyConditional(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict condition_map, size_t start, size_t end, Arena * arena) const
    {
        reserveAdd(place, end - start, arena);
        for (size_t i = start; i < end; ++i)
            if (condition_map[i])
                add(place, timestamp_ptr[i], value_ptr[i], arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
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

        addBatchSinglePlaceWithFlags<true>(row_begin, row_end, place, columns, arena, include_flags_data);
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
        Arena * arena,
        const UInt8 * flags_data) const
    {
        if (!array_of_pairs_argument && !array_arguments)
        {
            /// Each row holds a single sample.
            const TimestampType * timestamp_data = typeid_cast<const ColVecType &>(*columns[0]).getData().data();
            const ValueType * value_data = typeid_cast<const ColVecResultType &>(*columns[1]).getData().data();

            if (!flags_data)
                addMany(place, timestamp_data, value_data, row_begin, row_end, arena);
            else if constexpr (flag_value_to_include)
                addManyConditional(place, timestamp_data, value_data, flags_data, row_begin, row_end, arena);
            else
                addManyNotNull(place, timestamp_data, value_data, flags_data, row_begin, row_end, arena);

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

        /// Reserve memory for all the samples at once if no rows are skipped.
        if (!flags_data && row_end > row_begin)
            reserveAdd(place, timestamp_offsets[row_end - 1] - previous_timestamp_offset, arena);

        for (size_t i = row_begin; i < row_end; ++i)
        {
            /// A flag is per row, and each row holds a whole series
            if (!flags_data || flags_data[i] == flag_value_to_include)
            {
                const size_t timestamp_array_size = timestamp_offsets[i] - previous_timestamp_offset;
                const size_t value_array_size = value_offsets[i] - previous_value_offset;

                /// Check that timestamp and value arrays have the same size for the selected rows
                if (timestamp_array_size != value_array_size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                        i, timestamp_array_size, value_array_size);

                addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, timestamp_array_size, arena);
            }

            previous_timestamp_offset = timestamp_offsets[i];
            previous_value_offset = value_offsets[i];
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
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

        addBatchSinglePlaceWithFlags<false>(row_begin, row_end, place, columns, arena, exclude_flags_data);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & data = this->data(place);

        /// A rare unsorted state is serialized from a sorted copy, so the state is not mutated behind `const`.
        /// The copy cannot live in the arena because `serialize` gets no arena.
        if (!data.sorted) [[unlikely]]
        {
            PODArray<Element> sorted_elements;
            sorted_elements.assign(data.elements.begin(), data.elements.end());
            sorted_elements.resize_exact(Data::sortElements(sorted_elements.data(), sorted_elements.size()));
            writeElements(sorted_elements, buf);
            return;
        }

        writeElements(data.elements, buf);
    }

    template <typename Container>
    static void writeElements(const Container & elements, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(elements.size(), buf);

        for (const Element & element : elements)
            writeBinaryLittleEndian(element.timestamp, buf);

        for (const Element & element : elements)
            writeBinaryLittleEndian(element.value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different format version, expected {}, got {}",
                FORMAT_VERSION, format_version);

        Data & data = this->data(place);

        /// Deserialize replaces any previous contents.
        data.elements.clear();
        data.sorted = true;

        size_t size = 0;
        readBinaryLittleEndian(size, buf);

        /// The number of elements is read from the state and cannot be trusted, so only a bounded amount is
        /// reserved upfront and the array grows while the timestamps are read. That way a corrupted size fails
        /// with an end-of-buffer error instead of allocating memory for the claimed number of elements.
        data.elements.reserve(std::min(size, MAX_ELEMENTS_TO_RESERVE), arena);

        for (size_t i = 0; i < size; ++i)
        {
            TimestampType timestamp{};
            readBinaryLittleEndian(timestamp, buf);

            /// Peers running older versions write the samples in the order they were added, so the order is
            /// checked here instead of assumed.
            if (i > 0 && !(data.elements.back().timestamp < timestamp))
                data.sorted = false;

            data.elements.push_back(Element{.timestamp = timestamp, .value = ValueType{}}, arena);
        }

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].value, buf);

        data.sort(arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnArray & array_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = array_to.getOffsets();

        ColumnTuple & tuple = typeid_cast<ColumnTuple &>(array_to.getData());

        if (tuple.tupleSize() != 2)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected tuple size 2, got {}",
                tuple.tupleSize());

        ColVecType & timestamps_to = typeid_cast<ColVecType &>(tuple.getColumn(0));
        ColVecResultType & values_to = typeid_cast<ColVecResultType &>(tuple.getColumn(1));

        Data & data = this->data(place);

        data.sort(arena);

        for (const Element & element : data.elements)
        {
            timestamps_to.insert(element.timestamp);
            values_to.insert(element.value);
        }

        offsets_to.push_back(offsets_to.back() + data.elements.size());
    }

private:
    /// Whether samples are passed as a single argument of type Array(Tuple(timestamp, value)).
    const bool array_of_pairs_argument;

    /// Whether timestamp/value arguments are arrays (one row holds a whole series) or scalars.
    const bool array_arguments;

    static constexpr UInt16 FORMAT_VERSION = 1;

    /// How many elements `deserialize` reserves before reading the data. Bigger states grow while they are read.
    static constexpr size_t MAX_ELEMENTS_TO_RESERVE = 4096;
};

}
