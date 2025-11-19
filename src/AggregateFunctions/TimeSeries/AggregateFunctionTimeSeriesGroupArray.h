#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/Arena.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Aggregate function sorting pairs (timestamp, values) by timestamp.
/// If there are pairs with the same timestamp then the function keeps only a pair with the biggest value.
template <typename TimestampType, typename ValueType, bool array_arguments>
class AggregateFunctionTimeSeriesGroupArray final :
    public IAggregateFunctionHelper<AggregateFunctionTimeSeriesGroupArray<TimestampType, ValueType, array_arguments>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeSeriesGroupArray<TimestampType, ValueType, array_arguments>>;

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

    /// Stores all samples.
    struct Data
    {
        Element * elements = nullptr;
        size_t size = 0;
        size_t allocated_size = 0;

        void reserve(size_t new_size, Arena * arena)
        {
            if (new_size > allocated_size)
            {
                auto old_size = allocated_size;
                allocated_size = std::max(2 * allocated_size, new_size);
                elements = reinterpret_cast<Element *>(arena->alignedRealloc(
                    reinterpret_cast<char *>(elements), old_size * sizeof(Element), allocated_size * sizeof(Element),
                    alignof(Element)));
            }
        }

        void add(TimestampType timestamp, ValueType value, Arena * arena)
        {
            reserve(size + 1, arena);
            elements[size++] = Element{.timestamp = timestamp, .value = value};
        }

        void merge(const Data & rhs, Arena * arena)
        {
            reserve(size + rhs.size, arena);
            if (rhs.size)
                memcpy(elements + size, rhs.elements, rhs.size * sizeof(Element));
            size += rhs.size;
        }

        void sortAndRemoveDuplicates()
        {
            auto less_by_timestamp = [](const Element & left, const Element & right) { return left.timestamp < right.timestamp; };

            if (!std::is_sorted(elements, elements + size, less_by_timestamp))
            {
                std::sort(elements, elements + size, less_by_timestamp);
            }

            bool need_deduplication = false;

            if (size > 0)
            {
                for (size_t i = size - 1; i > 0; --i)
                {
                    if (elements[i].timestamp == elements[i - 1].timestamp)
                    {
                        /// If there are multiple values with the same timestamp, then we move the biggest value
                        /// to the first position in each group of values with the same timestamp.
                        /// We do that because std::unique() which is called below will remove all except the first element
                        /// in each group of values with the same timestamp.
                        elements[i - 1].value = std::max(elements[i - 1].value, elements[i].value);
                        need_deduplication = true;
                    }
                }
            }

            if (need_deduplication)
            {
                auto equal_by_timestamp = [](const Element & left, const Element & right) { return left.timestamp == right.timestamp; };
                const Element * new_end = std::unique(elements, elements + size, equal_by_timestamp);
                size = new_end - elements;
            }
        }
    };

    explicit AggregateFunctionTimeSeriesGroupArray(const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType(argument_types_))
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types_[0].get())->getNestedType() : argument_types_[0];
        const auto & value_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types_[1].get())->getNestedType() : argument_types_[1];
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
        data.reserve(data.size + num_elements_to_add, arena);
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
                        addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, timestamp_array_size, arena);
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

                addMany(place, timestamp_data, value_data, data_row_begin, data_row_end, arena);
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
                    addManyConditional(place, timestamp_data, value_data, flags_data, row_begin, row_end, arena);
                else
                    addManyNotNull(place, timestamp_data, value_data, flags_data, row_begin, row_end, arena);
            }
            else
            {
                addMany(place, timestamp_data, value_data, row_begin, row_end, arena);
            }
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

    void addBatchSparse(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const auto & column_sparse = typeid_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        const auto & offsets = column_sparse.getOffsetsData();

        size_t from = std::lower_bound(offsets.begin(), offsets.end(), row_begin) - offsets.begin();
        size_t to = std::lower_bound(offsets.begin(), offsets.end(), row_end) - offsets.begin();

        for (size_t i = from; i < to; ++i)
            add(places[offsets[i]] + place_offset, &values, i + 1, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & data = this->data(place);

        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(data.size, buf);

        for (size_t i = 0; i < data.size; ++i)
            writeBinaryLittleEndian(data.elements[i].timestamp, buf);

        for (size_t i = 0; i < data.size; ++i)
            writeBinaryLittleEndian(data.elements[i].value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt16 format_version;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different format version, expected {}, got {}",
                FORMAT_VERSION, format_version);

        Data & data = this->data(place);
        size_t size;
        readBinaryLittleEndian(size, buf);

        data.reserve(size, arena);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].timestamp, buf);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].value, buf);

        data.size = size;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
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

        data.sortAndRemoveDuplicates();

        for (size_t i = 0; i != data.size; ++i)
        {
            const auto & element = data.elements[i];
            timestamps_to.insert(element.timestamp);
            values_to.insert(element.value);
        }

        offsets_to.push_back(offsets_to.back() + data.size);
    }

private:
    static constexpr UInt16 FORMAT_VERSION = 1;
};

}
