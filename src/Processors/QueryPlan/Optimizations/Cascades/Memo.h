#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
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

    /// The table reads that one key range function divides, and how.
    struct KeyRangeLayoutFunctionKey
    {
        VectorWithMemoryTracking<GroupId> read_groups;
        size_t key_size = 0;
        size_t node_count = 0;

        auto operator<=>(const KeyRangeLayoutFunctionKey & other) const = default;
    };

    /// The functions that `getKeyRangeLayoutFunction` made. The rules that divide the same reads get the same
    /// function, and the borders are computed one time. A null value: the reads have nothing to divide.
    MapWithMemoryTracking<KeyRangeLayoutFunctionKey, StorageLayoutFunctionPtr> key_range_layout_functions;

private:
    LoggerPtr log;
    VectorWithMemoryTracking<GroupPtr> groups_by_id;
    OptimizerContext context;
};

}
