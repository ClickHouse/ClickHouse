#include <Interpreters/materializePlan.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

QueryPlan materializePlan(std::string_view serialized_bytes, const ContextPtr & context)
{
    /// Reconstruct plan skeleton from binary bytes.
    /// Leaf nodes are `ReadFromTableStep` (storage-agnostic
    /// placeholders).
    ReadBufferFromMemory in(serialized_bytes.data(), serialized_bytes.size());
    auto plan_and_sets = QueryPlan::deserializeForQueryPlanCache(in, context);

    /// Build `PreparedSet` objects for IN (...) subquery
    /// expressions embedded in the plan.
    auto plan = QueryPlan::makeSets(std::move(plan_and_sets), context);

    /// Replace `ReadFromTableStep` placeholders with
    /// storage-specific read steps. For local MergeTree tables
    /// this calls `storage->read` with
    /// `QueryProcessingStage::FetchColumns`, which performs
    /// part selection and creates `ReadFromMergeTree` against
    /// the current data snapshot.
    plan.resolveStorages(context);

    return plan;
}

}

