#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
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

    size_t getGroupCount() const { return groups_by_id.size(); }

    const OptimizerContext & getContext() const { return context; }
    void setContext(OptimizerContext context_) { context = std::move(context_); }
    /// Rows of the largest leaf with an estimate, once the leaves are derived; the cost model prices
    /// an exchange of an unknown result on it.
    std::optional<Float64> getLargestKnownLeafRows() const { return largest_known_leaf_rows; }
    void setLargestKnownLeafRows(Float64 rows) { largest_known_leaf_rows = rows; }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    OptimizerContext context;
    std::optional<Float64> largest_known_leaf_rows;
};

}
