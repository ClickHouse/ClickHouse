#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

/// Merges per-lane `sumForEach` states for the native PromQL range-sum path.
///
/// The input contains one row per partial output group:
/// `group UInt64, values <sumForEach result type>`. States are retained only
/// for groups observed by this final merge and are emitted in group-key order.
class PromQLPartialGroupMergeTransform final : public IAccumulatingTransform
{
public:
    PromQLPartialGroupMergeTransform(SharedHeader input_header_, AggregateFunctionPtr sum_function_, size_t max_output_groups_);

    ~PromQLPartialGroupMergeTransform() override;

    String getName() const override { return "PromQLPartialGroupMerge"; }

    static SharedHeader transformHeader(const AggregateFunctionPtr & sum_function);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregateDataPtr getOrCreateGroupState(UInt64 group);
    void destroyStates() noexcept;

    AggregateFunctionPtr sum_function;
    const size_t max_output_groups;

    size_t group_position = 0;
    size_t values_position = 0;

    Arena group_arena;
    HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>> group_states;
    bool generated = false;
};

}
