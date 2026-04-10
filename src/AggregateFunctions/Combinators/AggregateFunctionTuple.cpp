
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionTuple.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Arena.h>
#include <Common/memory.h>
#include <Common/typeid_cast.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

std::pair<VectorWithMemoryTracking<AggregateFunctionPtr>, DataTypePtr> AggregateFunctionTuple::initNested(
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
    VectorWithMemoryTracking<AggregateFunctionPtr> functions;
    functions.resize(elem_types.size());
    DataTypes result_types;
    result_types.reserve(elem_types.size());

    for (size_t i = 0; i < elem_types.size(); ++i)
    {
        AggregateFunctionProperties props;
        DataTypes nested_arg_types = {elem_types[i]};
        auto action = NullsAction::EMPTY;
        functions[i] = factory.get(base_name, action, nested_arg_types, params, props);
        result_types.push_back(functions[i]->getResultType());
    }

    DataTypePtr result_type;
    if (tuple_type->hasExplicitNames())
        result_type = std::make_shared<DataTypeTuple>(result_types, tuple_type->getElementNames());
    else
        result_type = std::make_shared<DataTypeTuple>(result_types);

    return {std::move(functions), std::move(result_type)};
}

AggregateFunctionTuple::AggregateFunctionTuple(
    const AggregateFunctionPtr & representative_nested_func,
    const DataTypes & arguments,
    const Array & params)
    : AggregateFunctionTuple(representative_nested_func->getName(), arguments, params,
        initNested(representative_nested_func->getName(), arguments, params))
{
}

AggregateFunctionTuple::AggregateFunctionTuple(
    const String & func_name,
    const DataTypes & arguments,
    const Array & params,
    std::pair<VectorWithMemoryTracking<AggregateFunctionPtr>, DataTypePtr> && nested_and_type)
    : IAggregateFunctionHelper<AggregateFunctionTuple>(arguments, params, nested_and_type.second)
    , nested_functions(std::move(nested_and_type.first))
    , nested_func_name(func_name)
{
    num_elements = nested_functions.size();
    state_offsets.resize(num_elements);

    size_t offset = 0;
    for (size_t i = 0; i < num_elements; ++i)
    {
        size_t align = nested_functions[i]->alignOfData();
        max_state_align = std::max(max_state_align, align);
        offset = ::Memory::alignUp(offset, align);
        state_offsets[i] = offset;
        offset += nested_functions[i]->sizeOfData();
    }
    total_state_size = ::Memory::alignUp(offset, max_state_align);
}

bool AggregateFunctionTuple::isVersioned() const
{
    for (const auto & func : nested_functions)
        if (func->isVersioned())
            return true;
    return false;
}

size_t AggregateFunctionTuple::getDefaultVersion() const
{
    size_t version = 0;
    for (const auto & func : nested_functions)
        version = std::max(version, func->getDefaultVersion());
    return version;
}

size_t AggregateFunctionTuple::getVersionFromRevision(size_t revision) const
{
    size_t version = 0;
    for (const auto & func : nested_functions)
        version = std::max(version, func->getVersionFromRevision(revision));
    return version;
}

void AggregateFunctionTuple::create(AggregateDataPtr __restrict place) const
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

void AggregateFunctionTuple::destroy(AggregateDataPtr __restrict place) const noexcept
{
    for (size_t i = 0; i < num_elements; ++i)
        nested_functions[i]->destroy(place + state_offsets[i]);
}

void AggregateFunctionTuple::destroyUpToState(AggregateDataPtr __restrict place) const noexcept
{
    for (size_t i = 0; i < num_elements; ++i)
        nested_functions[i]->destroyUpToState(place + state_offsets[i]);
}

bool AggregateFunctionTuple::hasTrivialDestructor() const
{
    for (const auto & func : nested_functions)
        if (!func->hasTrivialDestructor())
            return false;
    return true;
}

void AggregateFunctionTuple::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
{
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns[0]);
    for (size_t i = 0; i < num_elements; ++i)
    {
        const IColumn * nested_col = &tuple_column.getColumn(i);
        nested_functions[i]->add(place + state_offsets[i], &nested_col, row_num, arena);
    }
}

void AggregateFunctionTuple::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const
{
    for (size_t i = 0; i < num_elements; ++i)
        nested_functions[i]->merge(place + state_offsets[i], rhs + state_offsets[i], arena);
}

void AggregateFunctionTuple::serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const
{
    for (size_t i = 0; i < num_elements; ++i)
        nested_functions[i]->serialize(place + state_offsets[i], buf, version);
}

void AggregateFunctionTuple::deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const
{
    for (size_t i = 0; i < num_elements; ++i)
        nested_functions[i]->deserialize(place + state_offsets[i], buf, version, arena);
}

void AggregateFunctionTuple::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
{
    insertResultIntoImpl<false>(place, to, arena);
}

void AggregateFunctionTuple::insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
{
    insertResultIntoImpl<true>(place, to, arena);
}

bool AggregateFunctionTuple::allocatesMemoryInArena() const
{
    for (const auto & func : nested_functions)
        if (func->allocatesMemoryInArena())
            return true;
    return false;
}

bool AggregateFunctionTuple::isState() const
{
    for (const auto & func : nested_functions)
        if (func->isState())
            return true;
    return false;
}

namespace
{

class AggregateFunctionCombinatorTuple final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Tuple"; }

    bool transformsArgumentTypes() const override { return true; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly one Tuple argument", getName());

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix. Must be Tuple.",
                arguments[0]->getName(), getName());

        const auto & elem_types = tuple_type->getElements();
        if (elem_types.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple must not be empty for aggregate function with {} suffix", getName());

        /// Return first element type as a placeholder so that the factory can resolve the nested function name.
        /// The actual per-element functions will be created inside AggregateFunctionTuple.
        return DataTypes({elem_types[0]});
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionTuple>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorTuple(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTuple>());
}

}
