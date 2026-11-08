#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/JoinGraphStep.h>
#include <Common/Logger.h>

namespace DB
{

/// A join graph imported as one group per set of its relations, whose remaining joins the `JoinEnumeration` rule adds
/// to the memo once, see `JoinGraphStep::OrderBuilder::buildRemainingSets`.
struct JoinGraphExpansion
{
    std::shared_ptr<JoinGraphStep::OrderBuilder> order_builder;
    /// The group of every set imported so far; bit `i` of a set is the graph's input `i`.
    std::unordered_map<UInt64, GroupId> set_groups;
    /// The properties that the joins request from each input of the graph.
    std::unordered_map<UInt64, ExpressionProperties> input_pending_props;
    bool expanded = false;
};
using JoinGraphExpansionPtr = std::shared_ptr<JoinGraphExpansion>;

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

    const OptimizerContext & getContext() const { return context; }
    void setContext(OptimizerContext context_) { context = std::move(context_); }
    /// Rows of the largest leaf with an estimate, once the leaves are derived; the cost model prices
    /// an exchange of an unknown result on it.
    std::optional<Float64> getLargestKnownLeafRows() const { return largest_known_leaf_rows; }
    void setLargestKnownLeafRows(Float64 rows) { largest_known_leaf_rows = rows; }

    /// The expansion of the join graph whose group of all relations is `group_id`, if any.
    void addJoinGraphExpansion(GroupId group_id, JoinGraphExpansionPtr expansion) { join_graph_expansions[group_id] = std::move(expansion); }
    JoinGraphExpansionPtr getJoinGraphExpansion(GroupId group_id) const
    {
        auto it = join_graph_expansions.find(group_id);
        return it == join_graph_expansions.end() ? nullptr : it->second;
    }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    OptimizerContext context;
    std::optional<Float64> largest_known_leaf_rows;
    std::unordered_map<GroupId, JoinGraphExpansionPtr> join_graph_expansions;
};

}
