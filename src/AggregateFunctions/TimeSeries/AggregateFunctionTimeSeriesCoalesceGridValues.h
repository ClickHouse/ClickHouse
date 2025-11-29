#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
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

enum class AggregateFunctionTimeSeriesCoalesceGridValuesMode
{
    NULL_IF_CONFLICT,
    THROW_IF_CONFLICT,
};

/// Combines non-NULL values in arrays from different rows into a single array.

template <typename ValueType>
class AggregateFunctionTimeSeriesCoalesceGridValues final :
    public IAggregateFunctionHelper<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>
{
public:
    using Base = IAggregateFunctionHelper<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>;
    using Mode = AggregateFunctionTimeSeriesCoalesceGridValuesMode;

    using ColVecType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override { return "timeSeriesCoalesceGridValues"; }

    struct Element
    {
        /// Number of non-null values for a specific position.
        size_t num_values = 0;

        /// The last non-null value we've met so far.
        /// `last_value` does matter only if `num_values == 1`
        ValueType last_value = {};
    };

    /// Stores number of non-null values for each position and the last such value.
    struct Data
    {
        Element * elements = nullptr;
        size_t size = 0;

        void resize(size_t new_size, Arena * arena)
        {
            if (new_size == size)
                return;
            if (new_size)
            {
                elements = reinterpret_cast<Element *>(arena->alignedRealloc(
                    reinterpret_cast<char *>(elements), size * sizeof(Element), new_size * sizeof(Element), alignof(Element)));
                if (new_size > size)
                {
                    for (size_t i = size; i != new_size; ++i)
                        elements[i].num_values = 0;
                }
            }
            else
            {
                elements = nullptr;
            }
            size = new_size;
        }

        void add(size_t count, const ValueType * values, const UInt8 * null_map, Arena * arena)
        {
            if (count > size)
                resize(count, arena);
            for (size_t i = 0; i != size; ++i)
            {
                if (!null_map[i])
                {
                    auto & element = elements[i];
                    ++element.num_values;
                    element.last_value = values[i];
                }
            }
        }

        void merge(const Data & rhs, Arena * arena)
        {
            if (rhs.size > size)
                resize(rhs.size, arena);
            for (size_t i = 0; i != rhs.size; ++i)
            {
                const auto & rhs_element = rhs.elements[i];
                if (rhs_element.num_values)
                {
                    auto & element = elements[i];
                    element.num_values += rhs_element.num_values;
                    element.last_value = rhs_element.last_value;
                }
            }
        }
    };

    explicit AggregateFunctionTimeSeriesCoalesceGridValues(const DataTypes & argument_types_, Mode mode_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , mode(mode_)
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        const auto & values_type = argument_types_[0];
        return values_type;
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

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    static Data & data(AggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<Data *>(place);
    }

    static const Data & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const Data *>(place);
    }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, size_t count, const ValueType * values, const UInt8 * null_map, Arena * arena) const
    {
        Data & data = this->data(place);
        data.add(count, values, null_map, arena);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & column_array = typeid_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets = column_array.getOffsets();
        const auto & nullable_column = typeid_cast<const ColumnNullable &>(column_array.getData());
        const auto & values = typeid_cast<const ColVecType &>(nullable_column.getNestedColumn()).getData();
        const auto & null_map = nullable_column.getNullMapData();
        size_t previous_offset = offsets[row_num - 1];
        add(place, offsets[row_num] - previous_offset, values.data() + previous_offset, null_map.data() + previous_offset, arena);
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
        const auto & column_array = typeid_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets = column_array.getOffsets();
        const auto & nullable_column = typeid_cast<const ColumnNullable &>(column_array.getData());
        const auto & values = typeid_cast<const ColVecType &>(nullable_column.getNestedColumn()).getData();
        const auto & null_map = nullable_column.getNullMapData();

        for (size_t i = row_begin; i != row_end; ++i)
        {
            if (!flags_data || (flags_data[i] == flag_value_to_include))
            {
                size_t previous_offset = offsets[i - 1];
                add(place, offsets[i] - previous_offset, values.data() + previous_offset, null_map.data() + previous_offset, arena);
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
        ssize_t if_argument_pos) const override
    {
        const UInt8 * exclude_flags_data = null_map;    /// By default exclude using null_map
        std::unique_ptr<UInt8[]> combined_exclude_flags;

        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one.
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
            writeBinaryLittleEndian(data.elements[i].num_values, buf);

        for (size_t i = 0; i < data.size; ++i)
        {
            ValueType last_value = (data.elements[i].num_values == 1) ? data.elements[i].last_value : ValueType{};
            writeBinaryLittleEndian(last_value, buf);
        }
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
        data.resize(size, arena);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].num_values, buf);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].last_value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = typeid_cast<ColumnArray &>(to);
        auto & offsets = column_array.getOffsets();
        auto & nullable_column = typeid_cast<ColumnNullable &>(column_array.getData());
        auto & values = typeid_cast<ColVecType &>(nullable_column.getNestedColumn()).getData();
        auto & null_map = nullable_column.getNullMapData();

        const Data & data = this->data(place);

        for (size_t i = 0; i != data.size; ++i)
        {
            const auto & element = data.elements[i];
            if (element.num_values == 1)
            {
                values.push_back(element.last_value);
                null_map.push_back(0);
            }
            else if ((element.num_values == 0) || (mode == Mode::NULL_IF_CONFLICT))
            {
                values.push_back(ValueType{});
                null_map.push_back(1);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Vector cannot contain metrics with the same labelset");
            }
        }

        offsets.push_back(values.size());
    }

private:
    static constexpr UInt16 FORMAT_VERSION = 1;
    Mode mode;
};

}
