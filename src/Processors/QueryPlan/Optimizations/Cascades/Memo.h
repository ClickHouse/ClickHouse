#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
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

    void dump(WriteBuffer & out);
    String dump();

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
};

}
