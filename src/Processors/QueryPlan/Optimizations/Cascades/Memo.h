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

    size_t getClusterNodeCount() const { return cluster_node_count; }
    void setClusterNodeCount(size_t count) { cluster_node_count = count; }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    size_t cluster_node_count = 1;
};

}
