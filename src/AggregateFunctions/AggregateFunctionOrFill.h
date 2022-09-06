#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
}

/**
  * -OrDefault and -OrNull combinators for aggregate functions.
  * If there are no input values, return NULL or a default value, accordingly.
  * Use a single additional byte of data after the nested function data:
  * 0 means there was no input, 1 means there was some.
  */
template <bool UseNull>
class AggregateFunctionOrFill final : public IAggregateFunctionHelper<AggregateFunctionOrFill<UseNull>>
{
private:
    AggregateFunctionPtr nested_function;

    size_t size_of_data;
    DataTypePtr inner_type;
    bool inner_nullable;

public:
    AggregateFunctionOrFill(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionOrFill>{arguments, params}
        , nested_function{nested_function_}
        , size_of_data {nested_function->sizeOfData()}
        , inner_type {nested_function->getReturnType()}
        , inner_nullable {inner_type->isNullable()}
    {
        // nothing
    }

    String getName() const override
    {
        if constexpr (UseNull)
            return nested_function->getName() + "OrNull";
        else
            return nested_function->getName() + "OrDefault";
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return size_of_data + sizeof(char);
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
        place[size_of_data] = 0;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(place);
    }

    void add(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena) const override
    {
        nested_function->add(place, columns, row_num, arena);
        place[size_of_data] = 1;
    }

    void addBatch( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i] && places[i])
                    add(places[i] + place_offset, columns, i, arena);
            }
        }
        else
        {
            nested_function->addBatch(row_begin, row_end, places, place_offset, columns, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
                if (places[i])
                    (places[i] + place_offset)[size_of_data] = 1;
        }
    }

    void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            nested_function->addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                {
                    place[size_of_data] = 1;
                    break;
                }
            }
        }
        else
        {
            if (row_end != row_begin)
            {
                nested_function->addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
                place[size_of_data] = 1;
            }
        }
    }

    void addBatchSinglePlaceNotNull( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i] && !null_map[i])
                {
                    place[size_of_data] = 1;
                    break;
                }
            }
        }
        else
        {
            if (row_end != row_begin)
            {
                nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    if (!null_map[i])
                    {
                        place[size_of_data] = 1;
                        break;
                    }
                }
            }
        }
    }

    void merge(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
        place[size_of_data] |= rhs[size_of_data];
    }

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        nested_function->mergeBatch(row_begin, row_end, places, place_offset, rhs, arena);
        for (size_t i = row_begin; i < row_end; ++i)
            (places[i] + place_offset)[size_of_data] |= rhs[i][size_of_data];
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);

        writeChar(place[size_of_data], buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);

        readChar(place[size_of_data], buf);
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (UseNull)
        {
            // -OrNull

            if (inner_nullable)
                return inner_type;

            return std::make_shared<DataTypeNullable>(inner_type);
        }
        else
        {
            // -OrDefault

            return inner_type;
        }
    }

    void insertResultInto(
        AggregateDataPtr __restrict place,
        IColumn & to,
        Arena * arena) const override
    {
        if (place[size_of_data])
        {
            if constexpr (UseNull)
            {
                // -OrNull

                if (inner_nullable)
                    nested_function->insertResultInto(place, to, arena);
                else
                {
                    ColumnNullable & col = typeid_cast<ColumnNullable &>(to);

                    col.getNullMapColumn().insertDefault();
                    nested_function->insertResultInto(place, col.getNestedColumn(), arena);
                }
            }
            else
            {
                // -OrDefault

                nested_function->insertResultInto(place, to, arena);
            }
        }
        else
            to.insertDefault();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
