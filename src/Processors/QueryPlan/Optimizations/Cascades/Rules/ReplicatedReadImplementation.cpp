#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Replicated read: every node reads the full table, pinned to the coordinator's single-bucket mark
/// set so all nodes read the identical snapshot. Satisfies `{node_count=N, is_replicated=true}` without
/// a `BroadcastExchange`, eliminating network transfer for dimension tables in broadcast joins.
class ReplicatedReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ReplicatedRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool ReplicatedReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    if (!read_step
        || required_properties.distribution.node_count <= 1
        || !required_properties.distribution.is_replicated)
        return false;

    /// Correct only where every worker reads the same data: shared storage, or local execution
    /// (one process). Otherwise a BroadcastExchange satisfies the replicated requirement.
    return read_step->getMergeTreeData().isSharedStorage() || memo.getContext().distributed_plan_execute_locally;
}

std::vector<GroupExpressionPtr> ReplicatedReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    LOG_TEST(getLogger("ReplicatedRead"), "Creating replicated read for '{}' at {} nodes",
        read_step->getStepDescription(), node_count);

    /// Pin the coordinator's full mark set as a single bucket so every node reads the same snapshot.
    /// A read that cannot be pinned (an unsupported feature, or `FINAL`, which the single-bucket path
    /// refuses) gets no replicated implementation and the requirement falls back to a BroadcastExchange.
    size_t actual_buckets = 0;
    auto replicated_read_expression = tryMakeBucketedReadVariant(
        expression, node_count, /*target_buckets=*/1,
        strategySingleton<ReplicatedReadStrategy>(), "ReplicatedRead", /*is_replicated=*/true, actual_buckets);
    if (!replicated_read_expression)
    {
        LOG_TEST(getLogger("ReplicatedRead"), "No replicated read for '{}': its marks cannot be pinned into one bucket",
            read_step->getStepDescription());
        return {};
    }

    return addPhysicalToMemo(replicated_read_expression, required_properties, memo);
}

OptimizationRulePtr createReplicatedReadImplementation() { return std::make_shared<ReplicatedReadImplementation>(); }

}
