#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>

namespace DB
{

class Memo
{
public:
    explicit Memo(LoggerPtr log_)
        : log(log_)
    {}

    /// Creates a new group holding `group_expression`. Unconditional: the caller has already
    /// decided that no existing group can hold it.
    GroupId addGroup(GroupExpressionPtr group_expression);

    /// The group-creation entry point for ingestion and for rules. Returns the existing group when
    /// an equivalent logical expression is already in the memo, after inserting the new expression
    /// into that group as an alternative (dropped only when a fully-equal one is already there);
    /// creates a new group otherwise. Requires a pure logical expression (`strategy == nullptr`,
    /// no `enforced_property`): an enforcer computes its input's relation, so it would fold into
    /// its own child group.
    ///
    /// Merging only happens with `cascades_memo_deduplication` on and for a step instance that has
    /// a logical digest; otherwise a fresh group is created exactly as `addGroup` does, which is
    /// the fail-closed direction - a missed merge costs search effort, nothing else.
    ///
    /// The caller must have finalized the expression's inputs first: the index stores the fingerprint
    /// computed at insertion and never recomputes it, so an input group id that changes afterwards
    /// would strand the entry.
    GroupId internExpression(GroupExpressionPtr group_expression);

    /// Inserts a logical alternative into an existing group and runs the duplicate-group detection
    /// probe on it. Returns false when the group already held a fully-equal expression.
    bool addLogicalExpressionToGroup(GroupId group_id, GroupExpressionPtr group_expression);

    GroupPtr getGroup(GroupId group_id);
    GroupConstPtr getGroup(GroupId group_id) const;

    size_t getGroupCount() const { return groups_by_id.size(); }

    /// Is `target` reachable from the inputs of `expression`, following logical input links
    /// transitively? The cycle check of group deduplication: an expression may never join a group
    /// its own subtree already consumes. Physical expressions are not walked - a same-group
    /// enforcer links its group to itself by design, and every other physical expression repeats
    /// the input groups of its logical origin.
    bool isGroupReachableFromInputs(const GroupExpression & group_expression, GroupId target) const;

    /// Groups not reachable from `root_group_id`, i.e. groups no extracted plan can ever use.
    /// Zero for a healthy run; a non-zero count means some caller created a group and then dropped
    /// the only expression that would have consumed it.
    size_t countGroupsUnreachableFrom(GroupId root_group_id) const;

    OptimizerContext & getContext() { return context; }
    const OptimizerContext & getContext() const { return context; }
    void setContext(OptimizerContext context_) { context = std::move(context_); }

    void dump(WriteBuffer & out) const;
    String dump() const;

private:
    /// One interned logical expression. `insertion_time_fingerprint` is the key it was filed
    /// under, kept so an entry is never looked up or removed by a recomputed fingerprint: lazily
    /// populated analysis state can change a step's digest over its lifetime. A `ReadFromMergeTree`
    /// is the one step whose *logical* digest is stable across that - its mutable analysis members
    /// are excluded from it - but the rule governs the index regardless of the step type.
    struct LogicalExpressionIndexEntry
    {
        UInt128 insertion_time_fingerprint;
        GroupId group_id;
        GroupExpression * expression;   /// owned by the group
    };

    /// Finds the group of an interned expression logically equal to `group_expression`, or
    /// `INVALID_GROUP_ID`. Confirms every fingerprint candidate field by field over the two live
    /// steps: a fingerprint collision must never merge two groups.
    GroupId findInternedGroup(UInt128 fingerprint, const GroupExpression & group_expression) const;

    /// Counts and logs the case that proves two groups equal (plan section 9): the freshly
    /// inserted expression of `group_id` is logically equal to an interned expression of another
    /// group. No merging - that is a later stage.
    void detectDuplicateGroup(const GroupExpression & group_expression, GroupId group_id);

    /// May the expression take part in group identity at all?
    bool participatesInLogicalIndex(const GroupExpression & group_expression) const;

    LoggerPtr log;
    std::vector<GroupPtr> groups_by_id;
    std::unordered_map<UInt128, std::vector<LogicalExpressionIndexEntry>, UInt128Hash> logical_expression_index;
    OptimizerContext context;
};

}
