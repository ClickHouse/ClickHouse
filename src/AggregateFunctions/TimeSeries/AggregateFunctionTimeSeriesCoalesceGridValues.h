#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/Arena.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

enum class AggregateFunctionTimeSeriesCoalesceGridValuesMode
{
    /// The function returns the first non-null value it meets at each position.
    kAny,

    /// The function returns `NaN` if there are multiple non-null values at the same position
    /// (even if they're equal).
    kNaN,

    /// The function throws an exception (`Found duplicate series`) if there are multiple non-null values at the same position
    /// (even if they're equal).
    kThrow,
};

/// Combines non-NULL values in arrays from different rows into a single array.
template <typename ValueType>
class AggregateFunctionTimeSeriesCoalesceGridValues final
    : public IAggregateFunctionHelper<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>
{
public:
    using Base = IAggregateFunctionHelper<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>;
    using Mode = AggregateFunctionTimeSeriesCoalesceGridValuesMode;

    using ColVecType = ColumnVector<ValueType>;
    using Group = UInt64;

    String getName() const override { return "timeSeriesCoalesceGridValues"; }

    /// Information about each position in source arrays.
    struct Element
    {
        ValueType value;
        bool is_null;
        Group group;
    };

    struct Data
    {
        Element * elements;
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
                    {
                        auto & element = elements[i];
                        element.value = {};
                        element.is_null = true;
                        element.group = 0;
                    }
                }
            }
            else
            {
                elements = nullptr;
            }
            size = new_size;
        }

        void add(size_t count,
                 const ValueType * values,
                 const UInt8 * null_map,
                 Group group,
                 const AggregateFunctionTimeSeriesCoalesceGridValues<ValueType> & function,
                 Arena * arena)
        {
            if (count > size)
                resize(count, arena);
            for (size_t i = 0; i != size; ++i)
            {
                if (null_map && null_map[i])
                    continue;
                auto & element = elements[i];
                if (element.is_null)
                {
                    element.value = values[i];
                    element.is_null = false;
                    element.group = group;
                }
                else if (function.mode == Mode::kNaN)
                {
                    element.value = std::numeric_limits<ValueType>::quiet_NaN();
                }
                else if (function.mode == Mode::kThrow)
                {
                    function.throwFoundDuplicateSeries(element.group, group);
                }
            }
        }

        void merge(const Data & rhs,
                   const AggregateFunctionTimeSeriesCoalesceGridValues<ValueType> & function,
                   Arena * arena)
        {
            if (rhs.size > size)
                resize(rhs.size, arena);
            for (size_t i = 0; i != rhs.size; ++i)
            {
                auto & element = elements[i];
                const auto & rhs_element = rhs.elements[i];
                if (rhs_element.is_null)
                    continue;
                if (element.is_null)
                {
                    element = rhs_element;
                }
                else if (function.mode == Mode::kNaN)
                {
                    element.value = std::numeric_limits<ValueType>::quiet_NaN();
                }
                else if (function.mode == Mode::kThrow)
                {
                    function.throwFoundDuplicateSeries(element.group, rhs_element.group);
                }
            }
        }
    };

    explicit AggregateFunctionTimeSeriesCoalesceGridValues(ContextPtr context, const DataTypes & argument_types_, Mode mode_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector())
        , mode(mode_)
        , has_group_argument(argument_types_.size() == 2)
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        return argument_types_[0];
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

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, size_t count, const ValueType * values, const UInt8 * null_map, Group group, Arena * arena) const
    {
        Data & data = this->data(place);
        data.add(count, values, null_map, group, *this, arena);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & column_array = typeid_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets = column_array.getOffsets();
        size_t previous_offset = offsets[row_num - 1];
        Group group = has_group_argument ? columns[1]->get64(row_num) : 0;
        const ValueType * values = nullptr;
        const UInt8 * null_map = nullptr;
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(&column_array.getData()))
        {
            null_map = nullable_column->getNullMapData().data() + previous_offset;
            values = typeid_cast<const ColVecType &>(nullable_column->getNestedColumn()).getData().data() + previous_offset;
        }
        else
        {
            values = typeid_cast<const ColVecType &>(column_array.getData()).getData().data() + previous_offset;
        }
        add(place, offsets[row_num] - previous_offset, values, null_map, group, arena);
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
        const ValueType * values = nullptr;
        const UInt8 * null_map = nullptr;
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(&column_array.getData()))
        {
            null_map = nullable_column->getNullMapData().data();
            values = typeid_cast<const ColVecType &>(nullable_column->getNestedColumn()).getData().data();
        }
        else
        {
            values = typeid_cast<const ColVecType &>(column_array.getData()).getData().data();
        }

        for (size_t i = row_begin; i != row_end; ++i)
        {
            if (!flags_data || (flags_data[i] == flag_value_to_include))
            {
                size_t previous_offset = offsets[i - 1];
                Group group = has_group_argument ? columns[1]->get64(i) : 0;
                add(place, offsets[i] - previous_offset, values + previous_offset, null_map ? (null_map + previous_offset) : nullptr, group, arena);
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
        data(place).merge(data(rhs), *this, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & data = this->data(place);

        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(data.size, buf);

        for (size_t i = 0; i < data.size; ++i)
            writeBinaryLittleEndian(data.elements[i].value, buf);

        for (size_t i = 0; i < data.size; ++i)
            writeBinaryLittleEndian(data.elements[i].is_null, buf);

        for (size_t i = 0; i < data.size; ++i)
            writeBinaryLittleEndian(data.elements[i].group, buf);
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
            readBinaryLittleEndian(data.elements[i].value, buf);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].is_null, buf);

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(data.elements[i].group, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = typeid_cast<ColumnArray &>(to);
        auto & offsets = column_array.getOffsets();

        ColVecType * values = nullptr;
        NullMap * null_map = nullptr;
        if (auto * nullable_column = typeid_cast<ColumnNullable *>(&column_array.getData()))
        {
            values = typeid_cast<ColVecType *>(&nullable_column->getNestedColumn());
            null_map = &nullable_column->getNullMapData();
        }
        else
        {
            values = typeid_cast<ColVecType *>(&column_array.getData());
        }

        const Data & data = this->data(place);

        for (size_t i = 0; i != data.size; ++i)
        {
            const auto & element = data.elements[i];
            values->insertValue(element.value);
            chassert(null_map || !element.is_null);
            if (null_map)
                null_map->push_back(element.is_null);
        }

        offsets.push_back(values->size());
    }

    [[noreturn]] void throwFoundDuplicateSeries(Group first_group, Group second_group) const
    {
        String extra_info;
        if (has_group_argument)
        {
            auto first_tags = tags_collector->getTagsByGroup(first_group);
            auto second_tags = tags_collector->getTagsByGroup(second_group);
            extra_info = fmt::format(": {} and {}", ContextTimeSeriesTagsCollector::toString(first_tags), ContextTimeSeriesTagsCollector::toString(second_tags));
        }

        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Instant vector cannot contain metrics with the same groups of tags: found duplicate series {}",
                        extra_info);
    }

private:
    static constexpr UInt16 FORMAT_VERSION = 1;
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
    Mode mode;
    bool has_group_argument;
};

}
