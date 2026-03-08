#pragma once

#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/Arena.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypeTuple.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Adaptor for aggregate functions.
  * Adding -Tuple suffix to aggregate function
  *  will convert that aggregate function to a function, accepting Tuples,
  *  and applies aggregation for each element of the Tuple independently,
  *  returning a Tuple of aggregated values.
  *
  * Example: avgTuple of:
  *  (1, 2.0, 3.0),
  *  (3, 4.0, 5.0),
  *  (6, 7.0, 8.0)
  * will return:
  *  (avg(1,3,6), avg(2.0,4.0,7.0), avg(3.0,5.0,8.0))
  *
  * Since each tuple element may have a different type, we create
  * a separate nested aggregate function instance per element.
  */
class AggregateFunctionTuple final : public IAggregateFunctionHelper<AggregateFunctionTuple>
{
private:
    /// One nested aggregate function per tuple element (may be different instantiations).
    VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions;
    /// Precomputed byte offsets of each nested state within the aggregation data block.
    VectorWithMemoryTracking<size_t> state_offsets;
    size_t total_state_size = 0;
    size_t max_state_align = 1;
    size_t num_elements;
    String nested_func_name;

    static DataTypePtr createResultType(
        const String & base_name,
        const DataTypes & arguments,
        const Array & params)
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {}Tuple requires exactly one Tuple argument", base_name);

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of aggregate function {}Tuple must be Tuple", base_name);

        const auto & elem_types = tuple_type->getElements();
        if (elem_types.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple must not be empty for aggregate function {}Tuple", base_name);

        auto & factory = AggregateFunctionFactory::instance();
        DataTypes result_types;
        result_types.reserve(elem_types.size());

        for (const auto & elem_type : elem_types)
        {
            AggregateFunctionProperties props;
            DataTypes nested_arg_types = {elem_type};
            auto action = NullsAction::EMPTY;
            auto func = factory.get(base_name, action, nested_arg_types, params, props);
            result_types.push_back(func->getResultType());
        }

        if (tuple_type->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(result_types, tuple_type->getElementNames());
        return std::make_shared<DataTypeTuple>(result_types);
    }

public:
    AggregateFunctionTuple(
        const AggregateFunctionPtr & representative_nested_func,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionTuple>(
            arguments, params,
            createResultType(representative_nested_func->getName(), arguments, params))
        , nested_func_name(representative_nested_func->getName())
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {}Tuple requires exactly one Tuple argument", nested_func_name);

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of aggregate function {}Tuple must be Tuple", nested_func_name);

        const auto & elem_types = tuple_type->getElements();
        if (elem_types.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple must not be empty for aggregate function {}Tuple", nested_func_name);

        num_elements = elem_types.size();

        auto & factory = AggregateFunctionFactory::instance();
        nested_functions.resize(num_elements);
        state_offsets.resize(num_elements);

        size_t offset = 0;
        for (size_t i = 0; i < num_elements; ++i)
        {
            AggregateFunctionProperties props;
            DataTypes nested_arg_types = {elem_types[i]};
            auto action = NullsAction::EMPTY;
            nested_functions[i] = factory.get(nested_func_name, action, nested_arg_types, params, props);

            size_t align = nested_functions[i]->alignOfData();
            max_state_align = std::max(max_state_align, align);
            offset = (offset + align - 1) / align * align;
            state_offsets[i] = offset;
            offset += nested_functions[i]->sizeOfData();
        }
        total_state_size = offset;
    }

    String getName() const override
    {
        return nested_func_name + "Tuple";
    }

    bool isVersioned() const override
    {
        for (const auto & func : nested_functions)
            if (func->isVersioned())
                return true;
        return false;
    }

    size_t getDefaultVersion() const override
    {
        size_t version = 0;
        for (const auto & func : nested_functions)
            version = std::max(version, func->getDefaultVersion());
        return version;
    }

    size_t sizeOfData() const override { return total_state_size; }
    size_t alignOfData() const override { return max_state_align; }

    void create(AggregateDataPtr __restrict place) const override
    {
        size_t i = 0;
        try
        {
            for (; i < num_elements; ++i)
                nested_functions[i]->create(place + state_offsets[i]);
        }
        catch (...)
        {
            for (size_t j = 0; j < i; ++j)
                nested_functions[j]->destroy(place + state_offsets[j]);
            throw;
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < num_elements; ++i)
            nested_functions[i]->destroy(place + state_offsets[i]);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < num_elements; ++i)
            nested_functions[i]->destroyUpToState(place + state_offsets[i]);
    }

    bool hasTrivialDestructor() const override
    {
        for (const auto & func : nested_functions)
            if (!func->hasTrivialDestructor())
                return false;
        return true;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns[0]);
        for (size_t i = 0; i < num_elements; ++i)
        {
            const IColumn * nested_col = &tuple_column.getColumn(i);
            nested_functions[i]->add(place + state_offsets[i], &nested_col, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (size_t i = 0; i < num_elements; ++i)
            nested_functions[i]->merge(place + state_offsets[i], rhs + state_offsets[i], arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        for (size_t i = 0; i < num_elements; ++i)
            nested_functions[i]->serialize(place + state_offsets[i], buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        for (size_t i = 0; i < num_elements; ++i)
            nested_functions[i]->deserialize(place + state_offsets[i], buf, std::nullopt, arena);
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & tuple_to = assert_cast<ColumnTuple &>(to);
        for (size_t i = 0; i < num_elements; ++i)
        {
            if constexpr (merge)
                nested_functions[i]->insertMergeResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
            else
                nested_functions[i]->insertResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        for (const auto & func : nested_functions)
            if (func->allocatesMemoryInArena())
                return true;
        return false;
    }

    bool isState() const override
    {
        for (const auto & func : nested_functions)
            if (func->isState())
                return true;
        return false;
    }
};

}
