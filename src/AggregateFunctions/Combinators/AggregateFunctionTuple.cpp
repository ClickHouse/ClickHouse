
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionTuple.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
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
    const AggregateFunctionPtr & representative_nested_func,
    const DataTypes & arguments,
    const Array & params)
{
    const String & base_name = representative_nested_func->getName();

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

    /// When every tuple element is only-null (e.g. `Tuple(Nullable(Nothing))`), the factory has
    /// collapsed the representative function to `AggregateFunctionNothing`, whose name is a
    /// placeholder such as `nothingNull` rather than the original aggregate name. Re-resolving
    /// each element by that placeholder name would feed the original parameters to the `nothing*`
    /// creators, which reject parameters, turning otherwise valid parametric aggregates over
    /// `NULL` into an exception (e.g. `groupArrayMovingAvgTuple(2)(tuple(NULL))`). Since all
    /// elements are only-null and therefore resolve identically, reuse the already-created
    /// representative function for every element instead of re-resolving by name.
    bool all_only_null = true;
    for (const auto & type : elem_types)
    {
        if (!type->onlyNull())
        {
            all_only_null = false;
            break;
        }
    }

    for (size_t i = 0; i < elem_types.size(); ++i)
    {
        if (all_only_null)
        {
            functions[i] = representative_nested_func;
        }
        else
        {
            AggregateFunctionProperties props;
            DataTypes nested_arg_types = {elem_types[i]};
            auto action = NullsAction::EMPTY;
            functions[i] = factory.get(base_name, action, nested_arg_types, params, props);
        }
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
        initNested(representative_nested_func, arguments, params))
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
    /// Per-row fallback path: materialize sparse children defensively so callers that bypass
    /// our batch overrides (e.g. direct add() calls in tests or future code paths) stay correct.
    /// The hot paths (addBatch/addBatchSinglePlace) materialize once up front and use
    /// addRowFromMaterialized to avoid paying this cost per row.
    ColumnPtr materialized = recursiveRemoveSparse(columns[0]->getPtr());
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*materialized);
    addRowFromMaterialized(place, tuple_column, row_num, arena);
}

void AggregateFunctionTuple::addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const
{
    ColumnPtr materialized = recursiveRemoveSparse(columns[0]->getPtr());
    const IColumn * full_columns[1] = {materialized.get()};
    IAggregateFunctionHelper<AggregateFunctionTuple>::addManyDefaults(place, full_columns, length, arena);
}

void AggregateFunctionTuple::addBatch( /// NOLINT
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr * places,
    size_t place_offset,
    const IColumn ** columns,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    /// MergeTree may store individual Tuple elements as ColumnSparse while the outer ColumnTuple is dense.
    /// Nested aggregate functions cast their column to its concrete type, so we materialize once per batch.
    /// Note: we call addRowFromMaterialized directly in the row loop instead of delegating to
    /// IAggregateFunctionHelper::addBatch, because that would route back through our add() override
    /// and rerun recursiveRemoveSparse on every row.
    ColumnPtr materialized = recursiveRemoveSparse(columns[0]->getPtr());
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*materialized);

    if (if_argument_pos >= 0)
    {
        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (flags[i] && places[i])
                addRowFromMaterialized(places[i] + place_offset, tuple_column, i, arena);
        }
    }
    else
    {
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (places[i])
                addRowFromMaterialized(places[i] + place_offset, tuple_column, i, arena);
        }
    }
}

void AggregateFunctionTuple::addBatchSinglePlace( /// NOLINT
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr __restrict place,
    const IColumn ** columns,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    ColumnPtr materialized = recursiveRemoveSparse(columns[0]->getPtr());
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*materialized);

    if (if_argument_pos >= 0)
    {
        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (flags[i])
                addRowFromMaterialized(place, tuple_column, i, arena);
        }
    }
    else
    {
        for (size_t i = row_begin; i < row_end; ++i)
            addRowFromMaterialized(place, tuple_column, i, arena);
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

bool AggregateFunctionTuple::haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const AggregateFunctionTuple *>(&rhs);
    if (!rhs_tuple)
        return false;
    if (num_elements != rhs_tuple->num_elements)
        return false;
    for (size_t i = 0; i < num_elements; ++i)
        if (!nested_functions[i]->haveSameStateRepresentation(*rhs_tuple->nested_functions[i]))
            return false;
    return true;
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

        /// Return a representative element type as a placeholder so that the factory can resolve the nested function name.
        /// Prefer the first non-only-null element: if the first element happens to be `Nullable(Nothing)`, the recursive
        /// `get()` would wrap with the `Null` combinator and collapse to `AggregateFunctionNothing*`, which would then
        /// be used as the base function name and force every per-element nested function to also collapse to Nothing.
        /// The actual per-element functions are still created inside AggregateFunctionTuple based on real elem_types.
        for (const auto & type : elem_types)
            if (!type->onlyNull())
                return DataTypes({type});

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
