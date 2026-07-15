#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Logger.h>

namespace DB
{

/// Per-query environment the optimizer runs in: the cluster size, the cost model configuration
/// and the query settings the rules honor. Field defaults match the settings' defaults. Set once
/// before optimization starts; only the sort settings are captured later, while the memo is
/// built from the query plan.
struct OptimizationEnvironment
{
    size_t cluster_node_count = 1;
    CostConfig cost_config;
    /// All tasks run in one process reading the same local storage, so a replicated read is
    /// consistent even on non-shared storage.
    bool distributed_plan_execute_locally = false;
    bool distributed_aggregation_memory_efficient = true;
    bool distributed_plan_force_shuffle_aggregation = false;
    bool exact_rows_before_limit = false;
    /// Sort settings taken from the query (size limits, spill thresholds), used when
    /// SortingEnforcer builds a new sort so it matches the rest of the query's pipeline.
    std::optional<SortingStep::Settings> sort_settings;
};

class Memo
{
public:
    explicit Memo(LoggerPtr log_)
        : log(log_)
    {}

    GroupId addGroup(GroupExpressionPtr group_expression);

    GroupPtr getGroup(GroupId group_id);
    GroupConstPtr getGroup(GroupId group_id) const;

    size_t getGroupCount() const { return groups_by_id.size(); }

    const OptimizationEnvironment & getEnvironment() const { return environment; }
    void setEnvironment(OptimizationEnvironment environment_) { environment = std::move(environment_); }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    OptimizationEnvironment environment;
};

}
