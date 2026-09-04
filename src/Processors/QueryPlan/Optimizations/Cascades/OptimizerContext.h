#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepDigestCounters.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <optional>

namespace DB
{

/// Memo-size metrics for one optimizer run, the observability half of memo-wide group
/// deduplication. Owned by `OptimizerContext`, logged at the end of the pass; not synchronized -
/// the optimizer runs single-threaded.
struct MemoCounters
{
    /// Groups the memo created, including every fail-closed fresh group.
    UInt64 groups_created = 0;
    /// Interned expressions that joined an existing group instead of creating one.
    UInt64 groups_reused = 0;
    /// Insertions that proved two DIFFERENT groups logically equal (plan section 9). Counted and
    /// logged only; merging groups is a later stage.
    UInt64 duplicate_group_detections = 0;
};

/// Per-query context the optimizer runs in: the cluster size, the cost model configuration
/// and the query settings the rules honor. Field defaults match the settings' defaults. Set once
/// before optimization starts; only the sort settings are captured later, while the memo is
/// built from the query plan.
struct OptimizerContext
{
    size_t cluster_node_count = 1;
    CostConfig cost_config;
    /// All tasks run in one process reading the same local storage, so a replicated read is
    /// consistent even on non-shared storage.
    bool distributed_plan_execute_locally = false;
    bool distributed_aggregation_memory_efficient = true;
    bool distributed_plan_force_shuffle_aggregation = false;
    bool exact_rows_before_limit = false;
    /// Deduplicate groups on logical expression identity (`Memo::internExpression`).
    bool cascades_memo_deduplication = false;
    /// Sort settings taken from the query (size limits, spill thresholds), used when
    /// SortingEnforcer builds a new sort so it matches the rest of the query's pipeline.
    std::optional<SortingStep::Settings> sort_settings;
    /// Cost of the step-digest machinery for this run; see `CurrentStepDigestCounters`.
    StepDigestCounters step_digest_counters;
    /// What the memo did with that identity for this run.
    MemoCounters memo_counters;
};

}
