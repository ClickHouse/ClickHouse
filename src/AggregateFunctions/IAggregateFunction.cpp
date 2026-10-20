
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

DataTypePtr IAggregateFunction::getStateType() const
{
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters);
}

DataTypePtr IAggregateFunction::getStateTypeWithVersionOf(const IAggregateFunction & nested) const
{
    std::optional<size_t> version;
    /// The state type has to be held in a named variable: `getStateType` returns a fresh
    /// `DataTypePtr` by value, and a temporary would be destroyed at the end of the full
    /// expression, leaving `nested_state` dangling.
    const DataTypePtr nested_state_type = nested.getStateType();
    if (const auto * nested_state = typeid_cast<const DataTypeAggregateFunction *>(nested_state_type.get()))
        version = nested_state->getVersionIfExplicit();
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), argument_types, parameters, version);
}

DataTypePtr IAggregateFunction::getNormalizedStateType() const
{
    DataTypes normalized_argument_types;
    normalized_argument_types.reserve(argument_types.size());
    for (const auto & arg : argument_types)
        normalized_argument_types.emplace_back(arg->getNormalizedType());
    return std::make_shared<DataTypeAggregateFunction>(shared_from_this(), normalized_argument_types, parameters);
}

String IAggregateFunction::getDescription() const
{
    String description;

    description += getName();

    description += '(';

    for (const auto & parameter : parameters)
    {
        description += parameter.dump();
        description += ", ";
    }

    if (!parameters.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    description += '(';

    for (const auto & argument_type : argument_types)
    {
        description += argument_type->getName();
        description += ", ";
    }

    if (!argument_types.empty())
    {
        description.pop_back();
        description.pop_back();
    }

    description += ')';

    return description;
}

bool IAggregateFunction::haveEqualArgumentTypes(const IAggregateFunction & rhs) const
{
    return std::equal(
        argument_types.begin(),
        argument_types.end(),
        rhs.argument_types.begin(),
        rhs.argument_types.end(),
        [](const auto & t1, const auto & t2) { return t1->equals(*t2); });
}

DataTypePtr IAggregateFunction::getReturnTypeToPredict() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prediction is not supported for {}", getName());
}

bool IAggregateFunction::haveSameStateRepresentation(const IAggregateFunction & rhs) const
{
    const auto & lhs_base = getBaseAggregateFunctionWithSameStateRepresentation();
    const auto & rhs_base = rhs.getBaseAggregateFunctionWithSameStateRepresentation();

    return lhs_base.haveSameStateRepresentationImpl(rhs_base);
}

bool IAggregateFunction::haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const
{
    return getStateType()->equals(*rhs.getStateType());
}

bool IAggregateFunction::haveSameDefinition(const IAggregateFunction & rhs) const
{
    return assert_cast<const DataTypeAggregateFunction &>(*getStateType()).equalsIgnoringVariant(*rhs.getStateType());
}

void IAggregateFunction::parallelizeMergePrepare(
    AggregateDataPtrs & /*places*/, ThreadPool & /*thread_pool*/, std::atomic<bool> & /*is_cancelled*/) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "parallelizeMergePrepare() with thread pool parameter isn't implemented for {} ", getName());
}

char * IAggregateFunction::serializeToMemory(ConstAggregateDataPtr __restrict, char *, std::optional<size_t>) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeToMemory is not implemented for {}, it reported no serialized size bound", getName());
}

void IAggregateFunction::mergeStateFromDifferentVariant(
    AggregateDataPtr __restrict /*place*/, const IAggregateFunction & rhs, ConstAggregateDataPtr /*rhs_place*/, Arena * /*arena*/) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "mergeStateFromDifferentVariant() is not implemented for aggregate function '{}' ({} state variant). "
        "Cannot merge state produced by '{}' ({} state variant)",
        getName(),
        toString(getStateVariant()),
        rhs.getName(),
        toString(rhs.getStateVariant()));
}

void IAggregateFunction::mergeImpl(
    AggregateDataPtr __restrict /*place*/,
    ConstAggregateDataPtr /*rhs*/,
    ThreadPool & /*thread_pool*/,
    std::atomic<bool> & /*is_cancelled*/,
    Arena * /*arena*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "merge() with thread pool parameter isn't implemented for {} ", getName());
}

void IAggregateFunction::parallelizeMergeMulti(
    AggregateDataPtrs & places,
    ThreadPool & thread_pool,
    std::atomic<bool> & is_cancelled,
    Arena * arena) const
{
    /// Default: fall back to pairwise parallel merge.
    for (size_t i = 1; i < places.size(); ++i)
    {
        if (is_cancelled.load(std::memory_order_seq_cst))
            return;
        merge(places[0], places[i], thread_pool, is_cancelled, arena);
    }
}

void IAggregateFunction::mergeAndDestroyBatchOrDefer(
    AggregateDataPtr * dst_places,
    AggregateDataPtr * rhs_places,
    size_t size,
    size_t offset,
    PaddedPODArray<AggregateDataPtr> & deferred_dst,
    PaddedPODArray<AggregateDataPtr> & deferred_src,
    Arena * arena) const
{
    /// The caller has already detached the source states from their hash table, so a pair that is neither
    /// merged nor recorded in `deferred_*` would leak. Reserve the slots for the deferred pairs before
    /// anything is merged, so that recording a pair cannot throw. The destinations of a batch are distinct
    /// and a merge changes only its own destination, so the decisions below do not change in the loop.
    /// If something throws, the source states that are neither destroyed nor recorded yet are destroyed here.
    size_t i = 0;
    try
    {
        size_t num_large = 0;
        for (size_t j = 0; j < size; ++j)
            num_large += isLargeMergePair(dst_places[j] + offset, rhs_places[j] + offset);
        if (num_large)
        {
            deferred_dst.reserve(deferred_dst.size() + num_large);
            deferred_src.reserve(deferred_src.size() + num_large);
        }

        for (; i < size; ++i)
        {
            chassert(dst_places[i] + offset != rhs_places[i] + offset,
                     "IAggregateFunction::mergeAndDestroyBatchOrDefer called with the same source and destination state");
            if (isLargeMergePair(dst_places[i] + offset, rhs_places[i] + offset))
            {
                chassert(deferred_dst.size() < deferred_dst.capacity() && deferred_src.size() < deferred_src.capacity());
                deferred_dst.push_back(dst_places[i] + offset);
                deferred_src.push_back(rhs_places[i] + offset);
                continue;
            }

            mergeImpl(dst_places[i] + offset, rhs_places[i] + offset, arena);
            destroy(rhs_places[i] + offset);
        }
    }
    catch (...)
    {
        for (; i < size; ++i)
            destroy(rhs_places[i] + offset);
        throw;
    }
}

void IAggregateFunction::mergeBatchOrDefer(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr * places,
    size_t place_offset,
    const AggregateDataPtr * rhs,
    PaddedPODArray<AggregateDataPtr> & deferred_dst,
    PaddedPODArray<AggregateDataPtr> & deferred_src,
    Arena * arena) const
{
    for (size_t i = row_begin; i < row_end; ++i)
    {
        if (!places[i])
            continue;

        chassert(places[i] + place_offset != rhs[i],
                 "IAggregateFunction::mergeBatchOrDefer called with the same source and destination state");
        if (isLargeMergePair(places[i] + place_offset, rhs[i]))
        {
            deferred_dst.push_back(places[i] + place_offset);
            deferred_src.push_back(rhs[i]);
            continue;
        }

        mergeImpl(places[i] + place_offset, rhs[i], arena);
    }
}

void IAggregateFunction::insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
{
    if (isState())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Function {} is marked as State but method insertMergeResultInto is not implemented", getName());

    insertResultInto(place, to, arena);
}

void IAggregateFunction::addBatchSparseSinglePlace(
    size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena) const
{
    const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
    const auto * values = &column_sparse.getValuesColumn();
    const auto & offsets = column_sparse.getOffsetsData();

    auto from = std::lower_bound(offsets.begin(), offsets.end(), row_begin) - offsets.begin() + 1;
    auto to = std::lower_bound(offsets.begin(), offsets.end(), row_end) - offsets.begin() + 1;

    size_t num_defaults = (row_end - row_begin) - (to - from);
    if (from < to)
        addBatchSinglePlace(from, to, place, &values, arena, -1);
    if (num_defaults > 0)
        addManyDefaults(place, &values, num_defaults, arena);
}

void IAggregateFunction::predictValues(
    ConstAggregateDataPtr __restrict /* place */,
    IColumn & /*to*/,
    const ColumnsWithTypeAndName & /*arguments*/,
    size_t /*offset*/,
    size_t /*limit*/,
    ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method predictValues is not supported for {}", getName());
}

void IAggregateFunction::compileCreate(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

void IAggregateFunction::compileAdd(
    llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/, const ValuesWithType & /*arguments*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

void IAggregateFunction::compileMerge(
    llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_dst_ptr*/, llvm::Value * /*aggregate_data_src_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

llvm::Value * IAggregateFunction::compileGetResult(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

}
