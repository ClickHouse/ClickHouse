#pragma once

#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace FailPoints
{
extern const char aggregate_function_state_transfer_throw[];
}


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
        auto & column = assert_cast<ColumnAggregateFunction &>(to);

        /// Only once the column holds an aliased state, which is the partial transfer to undo.
        if (unlikely(!column.empty()))
        {
            fiu_do_on(FailPoints::aggregate_function_state_transfer_throw,
            {
                throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Injected failure in AggregateFunctionState::insertResultInto");
            });
        }

        column.getData().push_back(place);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnAggregateFunction &>(to).insertFrom(place);
    }

    void rollbackInsertResult(ConstAggregateDataPtr __restrict, IColumn & to) const noexcept override
    {
        /// insertResultInto aliased a state that `place` still owns, so the row must go without the
        /// destroy that ColumnAggregateFunction::popBack performs.
        assert_cast<ColumnAggregateFunction &>(to).popBackWithoutDestroy(1);
    }

    /// Aggregate function or aggregate function state.
    bool isState() const override { return true; }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
