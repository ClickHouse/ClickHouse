
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionTuple.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/memory.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr AggregateFunctionTuple::deriveResultType(
    const VectorWithMemoryTracking<AggregateFunctionPtr> & nested_functions,
    const DataTypes & arguments)
{
    /// Every construction path goes through the combinator's `transformArgumentsForMultipleNestedFunctions`,
    /// which has already validated that the arguments are non-empty `Tuple`s of equal size. The first
    /// `Tuple` argument provides the element names of the result.
    const auto & tuple_type = assert_cast<const DataTypeTuple &>(*arguments[0]);

    DataTypes result_types;
    result_types.reserve(nested_functions.size());
    for (const auto & nested_function : nested_functions)
        result_types.push_back(nested_function->getResultType());

    if (tuple_type.hasExplicitNames())
        return std::make_shared<DataTypeTuple>(result_types, tuple_type.getElementNames());
    return std::make_shared<DataTypeTuple>(result_types);
}

AggregateFunctionTuple::AggregateFunctionTuple(
    const String & nested_name,
    VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions_,
    const DataTypes & arguments,
    const Array & params)
    : IAggregateFunctionHelper<AggregateFunctionTuple>(arguments, params, deriveResultType(nested_functions_, arguments))
    , nested_functions(std::move(nested_functions_))
    , nested_func_name(nested_name)
{
    state_offsets.resize(nested_functions.size());

    size_t offset = 0;
    for (size_t i = 0; i < nested_functions.size(); ++i)
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

/// All nested functions share the same base aggregate function, so they agree on one versioning
/// scheme and a single version number serves the whole tuple state. Placeholder functions for
/// only-null elements ignore the version entirely.
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
        for (; i < nested_functions.size(); ++i)
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
    for (size_t i = 0; i < nested_functions.size(); ++i)
        nested_functions[i]->destroy(place + state_offsets[i]);
}

void AggregateFunctionTuple::destroyUpToState(AggregateDataPtr __restrict place) const noexcept
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
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
    /// An element column may be sparse when the caller has not materialized the block (the
    /// aggregation engine does it for the batch paths), while nested aggregate functions require
    /// dense columns.
    if (argument_types.size() == 1)
    {
        /// Single-tuple fast path: read a sparse element through its dense values column at the
        /// translated index; default rows map to index 0, where the shared default value lives.
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns[0]);

        for (size_t i = 0; i < nested_functions.size(); ++i)
        {
            const IColumn * nested_col = &tuple_column.getColumn(i);
            size_t nested_row = row_num;
            if (const auto * sparse = typeid_cast<const ColumnSparse *>(nested_col))
            {
                nested_col = &sparse->getValuesColumn();
                nested_row = sparse->getValueIndex(row_num);
            }
            nested_functions[i]->add(place + state_offsets[i], &nested_col, nested_row, arena);
        }
        return;
    }

    /// Multiple zipped tuples: the nested function takes one row index for all of its argument
    /// columns, and sparse columns would translate the same row to different values indices. When
    /// any of the i-th element columns is sparse, single-row cuts bring all of them to a common
    /// row 0 (a sparse element is cut from its dense values column at the translated index); an
    /// element with no sparse columns is passed through unchanged.
    size_t num_tuples = argument_types.size();
    Columns holders(num_tuples);
    ColumnRawPtrs nested_columns(num_tuples);
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        bool has_sparse = false;
        for (size_t k = 0; k < num_tuples; ++k)
        {
            nested_columns[k] = &assert_cast<const ColumnTuple &>(*columns[k]).getColumn(i);
            has_sparse |= typeid_cast<const ColumnSparse *>(nested_columns[k]) != nullptr;
        }

        size_t nested_row = row_num;
        if (has_sparse)
        {
            for (size_t k = 0; k < num_tuples; ++k)
            {
                if (const auto * sparse = typeid_cast<const ColumnSparse *>(nested_columns[k]))
                    holders[k] = sparse->getValuesColumn().cut(sparse->getValueIndex(row_num), 1);
                else
                    holders[k] = nested_columns[k]->cut(row_num, 1);
                nested_columns[k] = holders[k].get();
            }
            nested_row = 0;
        }
        nested_functions[i]->add(place + state_offsets[i], nested_columns.data(), nested_row, arena);
    }
}

template <bool has_null_map, typename GetPlace>
void AggregateFunctionTuple::addBatchImpl(
    size_t row_begin,
    size_t row_end,
    const IColumn ** columns,
    const UInt8 * null_map,
    ssize_t if_argument_pos,
    Arena * arena,
    GetPlace && get_place) const
{
    /// Batch callers may pass tuples whose element columns are sparse (e.g. window aggregation over
    /// data read from `MergeTree`), while nested aggregate functions require dense columns.
    size_t num_tuples = argument_types.size();

    /// Single-tuple fast path: for a sparse element the nested function receives the dense values
    /// column, and the row index is translated with a `ColumnSparse::Iterator` advanced
    /// monotonically. Expanding a sparse element would cost a full-column copy per call, and
    /// sliding window frames call this once per row; rows within one call are processed in
    /// increasing order, so the translation is amortized constant time.
    struct ElementAccess
    {
        const IColumn * column = nullptr;
        std::optional<ColumnSparse::Iterator> sparse_iterator;
    };
    VectorWithMemoryTracking<ElementAccess> elements;

    /// Multiple zipped tuples: the nested function takes one row index for all of its argument
    /// columns, and sparse columns would translate the same row to different values indices. When
    /// any of the i-th element columns is sparse, all of them are cut to the batch range
    /// [row_begin, row_end) and converted to full columns, and `row_bases[i]` rebases the row
    /// indices to the cut; an element with no sparse columns is passed through unchanged with
    /// `row_bases[i]` = 0. `zipped_columns` is a row-major matrix of the element columns:
    /// `zipped_columns[i * num_tuples + k]` is element `i` of the `k`-th Tuple argument.
    Columns holders;
    ColumnRawPtrs zipped_columns;
    VectorWithMemoryTracking<size_t> row_bases;

    if (num_tuples == 1)
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns[0]);
        elements.resize(nested_functions.size());
        for (size_t i = 0; i < nested_functions.size(); ++i)
        {
            const IColumn * element = &tuple_column.getColumn(i);
            if (const auto * sparse = typeid_cast<const ColumnSparse *>(element))
            {
                elements[i].column = &sparse->getValuesColumn();
                elements[i].sparse_iterator.emplace(sparse->getIterator(row_begin));
            }
            else
                elements[i].column = element;
        }
    }
    else
    {
        holders.resize(nested_functions.size() * num_tuples);
        zipped_columns.resize(nested_functions.size() * num_tuples);
        row_bases.resize(nested_functions.size());
        for (size_t i = 0; i < nested_functions.size(); ++i)
        {
            bool has_sparse = false;
            for (size_t k = 0; k < num_tuples; ++k)
            {
                zipped_columns[i * num_tuples + k] = &assert_cast<const ColumnTuple &>(*columns[k]).getColumn(i);
                has_sparse |= typeid_cast<const ColumnSparse *>(zipped_columns[i * num_tuples + k]) != nullptr;
            }

            row_bases[i] = 0;
            if (has_sparse)
            {
                for (size_t k = 0; k < num_tuples; ++k)
                {
                    auto & holder = holders[i * num_tuples + k];
                    holder = zipped_columns[i * num_tuples + k]->cut(row_begin, row_end - row_begin)->convertToFullColumnIfSparse();
                    zipped_columns[i * num_tuples + k] = holder.get();
                }
                row_bases[i] = row_begin;
            }
        }
    }

    auto add_row = [&](AggregateDataPtr place, size_t row)
    {
        if (num_tuples == 1)
        {
            for (size_t i = 0; i < nested_functions.size(); ++i)
            {
                auto & element = elements[i];
                size_t nested_row = row;
                if (element.sparse_iterator)
                {
                    element.sparse_iterator->advanceToRow(row);
                    nested_row = element.sparse_iterator->getValueIndex();
                }
                nested_functions[i]->add(place + state_offsets[i], &element.column, nested_row, arena);
            }
        }
        else
        {
            for (size_t i = 0; i < nested_functions.size(); ++i)
                nested_functions[i]->add(place + state_offsets[i], &zipped_columns[i * num_tuples], row - row_bases[i], arena);
        }
    };

    if (if_argument_pos >= 0)
    {
        const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (!flags[i])
                continue;
            if constexpr (has_null_map)
            {
                if (null_map[i])
                    continue;
            }
            if (AggregateDataPtr place = get_place(i))
                add_row(place, i);
        }
    }
    else
    {
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if constexpr (has_null_map)
            {
                if (null_map[i])
                    continue;
            }
            if (AggregateDataPtr place = get_place(i))
                add_row(place, i);
        }
    }
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
    addBatchImpl<false>(row_begin, row_end, columns, nullptr, if_argument_pos, arena,
        [&](size_t i) { return places[i] ? places[i] + place_offset : nullptr; });
}

void AggregateFunctionTuple::addBatchSinglePlace( /// NOLINT
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr __restrict place,
    const IColumn ** columns,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    addBatchImpl<false>(row_begin, row_end, columns, nullptr, if_argument_pos, arena,
        [&](size_t) { return place; });
}

void AggregateFunctionTuple::addBatchSinglePlaceNotNull( /// NOLINT
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr __restrict place,
    const IColumn ** columns,
    const UInt8 * null_map,
    Arena * arena,
    ssize_t if_argument_pos) const
{
    /// Reached from `AggregateFunctionNullUnary` for `Nullable(Tuple(...))` inputs: rows whose tuple
    /// is NULL are skipped via the null map.
    addBatchImpl<true>(row_begin, row_end, columns, null_map, if_argument_pos, arena,
        [&](size_t) { return place; });
}

void AggregateFunctionTuple::mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
        nested_functions[i]->merge(place + state_offsets[i], rhs + state_offsets[i], arena);
}

bool AggregateFunctionTuple::isParallelizeMergePrepareNeeded() const
{
    for (const auto & func : nested_functions)
        if (func->isParallelizeMergePrepareNeeded())
            return true;
    return false;
}

bool AggregateFunctionTuple::isAbleToParallelizeMerge() const
{
    for (const auto & func : nested_functions)
        if (func->isAbleToParallelizeMerge())
            return true;
    return false;
}

bool AggregateFunctionTuple::canOptimizeEqualKeysRanges() const
{
    for (const auto & func : nested_functions)
        if (!func->canOptimizeEqualKeysRanges())
            return false;
    return true;
}

void AggregateFunctionTuple::parallelizeMergePrepare(
    AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const
{
    AggregateDataPtrs nested_places(places.size());
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        if (!nested_functions[i]->isParallelizeMergePrepareNeeded())
            continue;
        for (size_t p = 0; p < places.size(); ++p)
            nested_places[p] = places[p] + state_offsets[i];
        nested_functions[i]->parallelizeMergePrepare(nested_places, thread_pool, is_cancelled);
    }
}

void AggregateFunctionTuple::mergeImpl(
    AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        const auto & func = nested_functions[i];
        if (func->isAbleToParallelizeMerge())
            func->merge(place + state_offsets[i], rhs + state_offsets[i], thread_pool, is_cancelled, arena);
        else
            func->merge(place + state_offsets[i], rhs + state_offsets[i], arena);
    }
}

void AggregateFunctionTuple::parallelizeMergeMulti(
    AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const
{
    AggregateDataPtrs nested_places(places.size());
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        const auto & func = nested_functions[i];
        for (size_t p = 0; p < places.size(); ++p)
            nested_places[p] = places[p] + state_offsets[i];

        if (func->isAbleToParallelizeMerge())
        {
            func->parallelizeMergeMulti(nested_places, thread_pool, is_cancelled, arena);
        }
        else
        {
            for (size_t p = 1; p < nested_places.size(); ++p)
            {
                if (is_cancelled.load(std::memory_order_seq_cst))
                    return;
                func->merge(nested_places[0], nested_places[p], arena);
            }
        }
    }
}

void AggregateFunctionTuple::mergeBatch(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr * places,
    size_t place_offset,
    const AggregateDataPtr * rhs,
    ThreadPool & thread_pool,
    std::atomic<bool> & is_cancelled,
    Arena * arena) const
{
    AggregateDataPtrs nested_rhs(row_end);
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        for (size_t r = row_begin; r < row_end; ++r)
            nested_rhs[r] = rhs[r] + state_offsets[i];
        nested_functions[i]->mergeBatch(
            row_begin, row_end, places, place_offset + state_offsets[i], nested_rhs.data(), thread_pool, is_cancelled, arena);
    }
}

void AggregateFunctionTuple::mergeAndDestroyBatch(
    AggregateDataPtr * dst_places,
    AggregateDataPtr * rhs_places,
    size_t size,
    size_t offset,
    ThreadPool & thread_pool,
    std::atomic<bool> & is_cancelled,
    Arena * arena) const
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
        nested_functions[i]->mergeAndDestroyBatch(
            dst_places, rhs_places, size, offset + state_offsets[i], thread_pool, is_cancelled, arena);
}

void AggregateFunctionTuple::serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
        nested_functions[i]->serialize(place + state_offsets[i], buf, version);
}

void AggregateFunctionTuple::deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const
{
    for (size_t i = 0; i < nested_functions.size(); ++i)
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

/// The tuple result contains nested aggregation states if any element produces one, so the whole
/// result requires state lifetime handling as soon as a single nested function is state-producing.
bool AggregateFunctionTuple::isState() const
{
    for (const auto & func : nested_functions)
        if (func->isState())
            return true;
    return false;
}

/// Both `haveSameStateRepresentationImpl` and `getNormalizedStateType` define state compatibility as
/// the element-wise composition of the same-named concept of the nested functions, plus equal arity.
/// They must agree so that equality of normalized types implies `haveSameStateRepresentation`; every
/// nested function guarantees that implication for itself, and the composition preserves it.
bool AggregateFunctionTuple::haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const AggregateFunctionTuple *>(&rhs);
    if (!rhs_tuple)
        return false;
    if (nested_functions.size() != rhs_tuple->nested_functions.size())
        return false;
    for (size_t i = 0; i < nested_functions.size(); ++i)
        if (!nested_functions[i]->haveSameStateRepresentation(*rhs_tuple->nested_functions[i]))
            return false;
    return true;
}

DataTypePtr AggregateFunctionTuple::getNormalizedStateType() const
{
    /// Unlike `-Array` / `-If`, the `-Tuple` state is a concatenation of per-element nested states,
    /// so we cannot delegate to a single nested function. Two tuple states are interchangeable
    /// exactly when their nested states are interchangeable pairwise, and the normalized type
    /// encodes that: its argument types are the nested normalized state types (compared recursively
    /// by `DataTypeAggregateFunction::equals`), and its function is a `-Tuple` wrapper rebuilt
    /// around the normalized nested functions, so that its name uses the canonical nested spelling.
    /// This unifies spellings with identical states (`quantileExactTuple` and `quantilesExactTuple`
    /// both normalize to a `quantilesExact`-based name), states that do not depend on the argument
    /// types (`countTuple` normalizes to `count` elements regardless of the tuple element types),
    /// and placeholder elements for only-null types. The parameters are dropped: every parameter
    /// that affects a state representation is already part of the corresponding nested normalized
    /// state type.
    DataTypes nested_normalized_state_types;
    nested_normalized_state_types.reserve(nested_functions.size());
    VectorWithMemoryTracking<AggregateFunctionPtr> normalized_nested_functions;
    normalized_nested_functions.reserve(nested_functions.size());
    for (const auto & nested_function : nested_functions)
    {
        auto normalized_state_type = nested_function->getNormalizedStateType();
        normalized_nested_functions.push_back(assert_cast<const DataTypeAggregateFunction &>(*normalized_state_type).getFunction());
        nested_normalized_state_types.push_back(std::move(normalized_state_type));
    }

    String normalized_nested_name = normalized_nested_functions.front()->getName();
    auto normalized_function = std::make_shared<AggregateFunctionTuple>(
        normalized_nested_name, std::move(normalized_nested_functions), argument_types, Array{});
    return std::make_shared<DataTypeAggregateFunction>(std::move(normalized_function), nested_normalized_state_types, Array{});
}

AggregateFunctionStateVariant AggregateFunctionTuple::getStateVariant() const
{
    /// All elements are resolved together under one requested variant, so any element reporting
    /// Window means the whole state uses the window representation; only elements without a window
    /// implementation (including `Nothing` placeholders) report Aggregation unconditionally.
    for (const auto & func : nested_functions)
        if (func->getStateVariant() == AggregateFunctionStateVariant::Window)
            return AggregateFunctionStateVariant::Window;
    return AggregateFunctionStateVariant::Aggregation;
}

bool AggregateFunctionTuple::canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const AggregateFunctionTuple *>(&rhs);
    if (!rhs_tuple)
        return false;

    if (nested_functions.size() != rhs_tuple->nested_functions.size())
        return false;

    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        const auto & lhs_nested = nested_functions[i];
        const auto & rhs_nested = rhs_tuple->nested_functions[i];
        if (!lhs_nested->haveSameStateRepresentation(*rhs_nested) && !lhs_nested->canMergeStateFromDifferentVariant(*rhs_nested))
            return false;
    }
    return true;
}

void AggregateFunctionTuple::mergeStateFromDifferentVariant(
    AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const
{
    chassert(canMergeStateFromDifferentVariant(rhs));

    const auto & rhs_tuple = assert_cast<const AggregateFunctionTuple &>(rhs);
    for (size_t i = 0; i < nested_functions.size(); ++i)
    {
        const auto & lhs_nested = nested_functions[i];
        const auto & rhs_nested = rhs_tuple.nested_functions[i];
        AggregateDataPtr lhs_element = place + state_offsets[i];
        ConstAggregateDataPtr rhs_element = rhs_place + rhs_tuple.state_offsets[i];
        if (lhs_nested->getStateVariant() == rhs_nested->getStateVariant())
            lhs_nested->merge(lhs_element, rhs_element, arena);
        else
            lhs_nested->mergeStateFromDifferentVariant(lhs_element, *rhs_nested, rhs_element, arena);
    }
}

namespace
{

class AggregateFunctionCombinatorTuple final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Tuple"; }

    bool transformsArgumentTypes() const override { return true; }

    bool transformsMultipleNestedFunctions() const override { return true; }

    VectorWithMemoryTracking<DataTypes> transformArgumentsForMultipleNestedFunctions(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires at least one Tuple argument", getName());

        VectorWithMemoryTracking<const DataTypeTuple *> tuple_types;
        tuple_types.reserve(arguments.size());
        for (const auto & argument : arguments)
        {
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(argument.get());
            if (!tuple_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument for aggregate function with {} suffix. Must be Tuple.",
                    argument->getName(), getName());
            tuple_types.push_back(tuple_type);
        }

        size_t num_elements = tuple_types.front()->getElements().size();
        if (num_elements == 0)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple must not be empty for aggregate function with {} suffix", getName());

        for (const auto * tuple_type : tuple_types)
            if (tuple_type->getElements().size() != num_elements)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "All Tuple arguments of aggregate function with {} suffix must have the same number of elements, got {} and {}",
                    getName(), num_elements, tuple_type->getElements().size());

        /// One nested function per element position; each receives the corresponding element from
        /// every Tuple argument, in argument order.
        VectorWithMemoryTracking<DataTypes> nested_arguments_list;
        nested_arguments_list.reserve(num_elements);
        for (size_t i = 0; i < num_elements; ++i)
        {
            DataTypes nested_arguments;
            nested_arguments.reserve(tuple_types.size());
            for (const auto * tuple_type : tuple_types)
                nested_arguments.push_back(tuple_type->getElements()[i]);
            nested_arguments_list.push_back(std::move(nested_arguments));
        }
        return nested_arguments_list;
    }

    AggregateFunctionPtr transformAggregateFunctionFromMultipleNestedFunctions(
        const String & nested_name,
        VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionTuple>(nested_name, std::move(nested_functions), arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorTuple(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorTuple(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTuple>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumTuple`), it makes the function take one `Tuple` argument per argument of the underlying function (all tuples of the same size) and aggregate each element position independently, producing a tuple of per-element results. Elements are paired by position: `corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))`.",
        .syntax = "<aggregate_function>Tuple(tuple1[, tuple2, ...])",
        .examples = {{"`sum` aggregate function",
            "SELECT sumTuple(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.5)) AS t UNION ALL SELECT tuple(toInt64(3), toFloat64(4.5)) UNION ALL SELECT tuple(toInt64(5), toFloat64(6.5)))",
            "(9,13.5)"},
            {"`corr` aggregate function with two tuples",
            "SELECT corrTuple((a1, a2), (b1, b2)) FROM (SELECT toFloat64(number) AS a1, toFloat64(number * 2) AS a2, toFloat64(100 - number) AS b1, toFloat64(number * 3) AS b2 FROM numbers(10))",
            "(-1,1)"}},
        .introduced_in = {26, 7},
        .related = {"Array", "ForEach", "Map"}});
}

}
