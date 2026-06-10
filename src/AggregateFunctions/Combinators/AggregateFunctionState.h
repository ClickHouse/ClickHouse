#pragma once

#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/assert_cast.h>


namespace DB
{
struct Settings;


/** Not an aggregate function, but an adapter of aggregate functions,
  * Aggregate functions with the `State` suffix differ from the corresponding ones in that their states are not finalized.
  * Return type - DataTypeAggregateFunction.
  */

class AggregateFunctionState final : public IAggregateFunctionHelper<AggregateFunctionState>
{
private:
    AggregateFunctionPtr nested_func;

public:
    AggregateFunctionState(AggregateFunctionPtr nested_, const DataTypes & arguments_, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionState>(arguments_, params_, nested_->getStateType())
        , nested_func(nested_)
    {}

    String getName() const override
    {
        return nested_func->getName() + "State";
    }

    const IAggregateFunction & getBaseAggregateFunctionWithSameStateRepresentation() const override
    {
        return nested_func->getBaseAggregateFunctionWithSameStateRepresentation();
    }

    DataTypePtr getStateType() const override
    {
        return nested_func->getStateType();
    }

    DataTypePtr getNormalizedStateType() const override
    {
        return nested_func->getNormalizedStateType();
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (!this->haveSameDefinition(rhs))
            return false;

        chassert(rhs.getNestedFunction() != nullptr);

        return nested_func->canMergeStateFromDifferentVariant(*rhs.getNestedFunction());
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override
    {
        chassert(rhs.getNestedFunction() != nullptr);

        nested_func->mergeStateFromDifferentVariant(place, *rhs.getNestedFunction(), rhs_place, arena);
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    size_t getVersionFromRevision(size_t revision) const override { return nested_func->getVersionFromRevision(revision); }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict) const noexcept override {}

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        nested_func->add(place, columns, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_func->isAbleToParallelizeMerge(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_func->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_func->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_func->merge(place, rhs, thread_pool, is_cancelled, arena);
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_func->parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnAggregateFunction &>(to).getData().push_back(place);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnAggregateFunction &>(to).insertFrom(place);
    }

    /// Aggregate function or aggregate function state.
    bool isState() const override { return true; }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }

    /// `State` forwards `getOwnNullAdapter` to the nested function (see below),
    /// so it may claim the payload-preserving property of the nested function.
    bool preservesNullablePayloadForIf() const override { return nested_func->preservesNullablePayloadForIf(); }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & /*nested_function*/,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & properties) const override
    {
        /// If the inner aggregate function provides its own null adapter and preserves
        /// nullable payload (e.g. `groupFormat`), forward through `State` so that combinator
        /// stacks like `groupFormatStateIf` reach `AggregateFunctionIfRespectNulls` instead of
        /// falling back to `AggregateFunctionIfNull*`, which would drop payload `NULL` rows.
        /// The inner adapter is rebuilt with the original nullable argument types, and we
        /// re-wrap it through the `State` combinator so the serialized state matches those types.
        if (nested_func->preservesNullablePayloadForIf())
        {
            if (auto inner_adapter = nested_func->getOwnNullAdapter(nested_func, arguments, params, properties))
            {
                if (auto combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("State"))
                    return combinator->transformAggregateFunction(inner_adapter, properties, arguments, params);
            }
        }

        /// Otherwise keep the default behaviour: the `Null` combinator handles `State`
        /// functions by applying itself to the nested function instead (see `tryTransformStateFunction`).
        return nullptr;
    }
};

}
