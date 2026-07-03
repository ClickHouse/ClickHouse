#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Logger.h>

namespace DB
{

class Memo
{
public:
    explicit Memo(LoggerPtr log_)
        : log(log_)
    {}

    GroupId addGroup(GroupExpressionPtr group_expression);

    GroupPtr getGroup(GroupId group_id);
    GroupConstPtr getGroup(GroupId group_id) const;

    size_t getClusterNodeCount() const { return cluster_node_count; }
    void setClusterNodeCount(size_t count) { cluster_node_count = count; }

    size_t getGroupCount() const { return groups_by_id.size(); }

    const CostConfig & getCostConfig() const { return cost_config; }
    void setCostConfig(CostConfig config) { cost_config = config; }

    /// Sort settings from the query's own SortingStep (they carry the query's size limits and
    /// spill thresholds), used when SortingEnforcer builds a new sort. All sorts of one query
    /// share these settings, so keeping the first is enough.
    const std::optional<SortingStep::Settings> & getSortSettings() const { return sort_settings; }
    void setSortSettings(const SortingStep::Settings & settings)
    {
        if (!sort_settings)
            sort_settings = settings;
    }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    size_t cluster_node_count = 1;
    CostConfig cost_config;
    std::optional<SortingStep::Settings> sort_settings;
};

}
