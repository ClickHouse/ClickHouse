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
    [[nodiscard]] GroupConstPtr getGroup(GroupId group_id) const;

    [[nodiscard]] size_t getGroupCount() const { return groups_by_id.size(); }

    [[nodiscard]] const OptimizerContext & getContext() const { return context; }
    void setContext(OptimizerContext context_) { context = std::move(context_); }

    void dump(WriteBuffer & out) const;
    [[nodiscard]] String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    OptimizerContext context;
};

}
