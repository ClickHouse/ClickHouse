#pragma once

#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}


struct AggregateFunctionForEachData
{
    size_t dynamic_array_size = 0;
    char * array_of_aggregate_datas = nullptr;
};

/** Adaptor for aggregate functions.
  * Adding -ForEach suffix to aggregate function
  *  will convert that aggregate function to a function, accepting arrays,
  *  and applies aggregation for each corresponding elements of arrays independently,
  *  returning arrays of aggregated values on corresponding positions.
  *
  * Example: sumForEach of:
  *  [1, 2],
  *  [3, 4, 5],
  *  [6, 7]
  * will return:
  *  [10, 13, 5]
  *
  * TODO Allow variable number of arguments.
  */
class AggregateFunctionForEach final : public IAggregateFunctionDataHelper<AggregateFunctionForEachData, AggregateFunctionForEach>
{
private:
    AggregateFunctionPtr nested_func;
    size_t nested_size_of_data = 0;
    size_t num_arguments;

    AggregateFunctionForEachData & ensureAggregateData(AggregateDataPtr place, size_t new_size, Arena & arena) const
    {
        AggregateFunctionForEachData & state = data(place);

        /// Ensure we have aggreate states for new_size elements, allocate from arena if needed

        size_t old_size = state.dynamic_array_size;
        if (old_size < new_size)
        {
            state.array_of_aggregate_datas = arena.realloc(
                state.array_of_aggregate_datas,
                old_size * nested_size_of_data,
                new_size * nested_size_of_data);

            size_t i = old_size;
            char * nested_state = state.array_of_aggregate_datas + i * nested_size_of_data;

            try
            {
                for (; i < new_size; ++i)
                {
                    nested_func->create(nested_state);
                    nested_state += nested_size_of_data;
                }
            }
            catch (...)
            {
                size_t cleanup_size = i;
                nested_state = state.array_of_aggregate_datas + i * nested_size_of_data;

                for (i = 0; i < cleanup_size; ++i)
                {
                    nested_func->destroy(nested_state);
                    nested_state += nested_size_of_data;
                }

                throw;
            }

            state.dynamic_array_size = new_size;
        }

        return state;
    }

public:
    AggregateFunctionForEach(AggregateFunctionPtr nested_, const DataTypes & arguments)
        : nested_func(nested_), num_arguments(arguments.size())
    {
        nested_size_of_data = nested_func->sizeOfData();

        if (arguments.empty())
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!typeid_cast<const DataTypeArray *>(type.get()))
                throw Exception("All arguments for aggregate function " + getName() + " must be arrays", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "ForEach";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(nested_func->getReturnType());
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        AggregateFunctionForEachData & state = data(place);

        char * nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i)
        {
            nested_func->destroy(nested_state);
            nested_state += nested_size_of_data;
        }
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const IColumn * nested[num_arguments];

        for (size_t i = 0; i < num_arguments; ++i)
            nested[i] = &static_cast<const ColumnArray &>(*columns[i]).getData();

        const ColumnArray & first_array_column = static_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets = first_array_column.getOffsets();

        size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];

        /// Sanity check. NOTE We can implement specialization for a case with single argument, if the check will hurt performance.
        for (size_t i = 1; i < num_arguments; ++i)
        {
            const ColumnArray & ith_column = static_cast<const ColumnArray &>(*columns[i]);
            const IColumn::Offsets & ith_offsets = ith_column.getOffsets();

            if (ith_offsets[row_num] != end || (row_num != 0 && ith_offsets[row_num - 1] != begin))
                throw Exception("Arrays passed to " + getName() + " aggregate function have different sizes", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

        AggregateFunctionForEachData & state = ensureAggregateData(place, end - begin, *arena);

        char * nested_state = state.array_of_aggregate_datas;
        for (size_t i = begin; i < end; ++i)
        {
            nested_func->add(nested_state, nested, i, arena);
            nested_state += nested_size_of_data;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        const AggregateFunctionForEachData & rhs_state = data(rhs);
        AggregateFunctionForEachData & state = ensureAggregateData(place, rhs_state.dynamic_array_size, *arena);

        const char * rhs_nested_state = rhs_state.array_of_aggregate_datas;
        char * nested_state = state.array_of_aggregate_datas;

        for (size_t i = 0; i < state.dynamic_array_size && i < rhs_state.dynamic_array_size; ++i)
        {
            nested_func->merge(nested_state, rhs_nested_state, arena);

            rhs_nested_state += nested_size_of_data;
            nested_state += nested_size_of_data;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const AggregateFunctionForEachData & state = data(place);
        writeBinary(state.dynamic_array_size, buf);

        const char * nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i)
        {
            nested_func->serialize(nested_state, buf);
            nested_state += nested_size_of_data;
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        AggregateFunctionForEachData & state = data(place);

        size_t new_size = 0;
        readBinary(new_size, buf);

        ensureAggregateData(place, new_size, *arena);

        char * nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < new_size; ++i)
        {
            nested_func->deserialize(nested_state, buf, arena);
            nested_state += nested_size_of_data;
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const AggregateFunctionForEachData & state = data(place);

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & elems_to = arr_to.getData();

        const char * nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i)
        {
            nested_func->insertResultInto(nested_state, elems_to);
            nested_state += nested_size_of_data;
        }

        offsets_to.push_back(offsets_to.empty() ? state.dynamic_array_size : offsets_to.back() + state.dynamic_array_size);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


}
