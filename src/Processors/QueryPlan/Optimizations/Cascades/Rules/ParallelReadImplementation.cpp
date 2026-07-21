#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Splits a table read across N nodes - each node reads 1/N of the data.
/// Satisfies `{node_count=N, is_replicated=false}`.
class ParallelReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ParallelRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool ParallelReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    if (!read_step
        || required_properties.distribution.node_count <= 1
        || required_properties.distribution.is_replicated)
        return false;

    /// `FINAL` reads can also be split: buckets follow primary-key-range layers, so a dedup group
    /// never spans buckets; reads that cannot be split safely are refused at apply time.
    return true;
}

/// Clones the read with its marks pinned into `target_buckets` coordinator-computed buckets and
/// wraps it into a physical expression with the given strategy and output distribution. Returns
/// nullptr when the read does not split into exactly the requested count (an unsupported
/// feature, nothing to read, an unsplittable FINAL); `actual_buckets` reports the count for the
/// caller's log line.
static GroupExpressionPtr makeBucketedReadVariant(
    const GroupExpressionPtr & expression,
    size_t node_count,
    size_t target_buckets,
    ImplementationStrategyPtr strategy,
    const char * description_prefix,
    bool is_replicated,
    size_t & actual_buckets)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto bucketed_read_step_ptr = cloneStepAs(*read_step);
    auto * bucketed_read_step = bucketed_read_step_ptr.get();

    actual_buckets = bucketed_read_step->setupDistributedReadBuckets(target_buckets, ReadFromMergeTree::max_distributed_read_buckets);
    if (actual_buckets != target_buckets)
        return nullptr;
    bucketed_read_step->setStepDescription(fmt::format("{} {}", description_prefix, read_step->getStepDescription()), 200);

    GroupExpressionPtr bucketed_read_expression = std::make_shared<GroupExpression>(*expression);
    bucketed_read_expression->plan_step = std::move(bucketed_read_step_ptr);
    bucketed_read_expression->strategy = std::move(strategy);

    ExpressionProperties output_properties;
    output_properties.distribution.node_count = node_count;
    output_properties.distribution.is_replicated = is_replicated;
    bucketed_read_expression->properties = output_properties;
    return bucketed_read_expression;
}

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    /// Produce a distributed read that splits work uniformly across all nodes: the coordinator
    /// computes each bucket's authoritative marks and the fan-out ships them to the workers in
    /// the `read_bucket` task parameters. DefaultImplementation handles the single-node read.
    size_t actual_buckets = 0;
    auto parallel_read_expression = makeBucketedReadVariant(
        expression, node_count, /*target_buckets=*/node_count,
        std::make_shared<ParallelReadStrategy>(), "ParallelRead", /*is_replicated=*/false, actual_buckets);
    if (!parallel_read_expression)
    {
        LOG_TEST(getLogger("ParallelRead"), "No parallel read for '{}': the read splits into {} buckets instead of {}",
            read_step->getStepDescription(), actual_buckets, node_count);
        return {};
    }

    std::vector<GroupExpressionPtr> result;
    addPhysicalToMemo(parallel_read_expression, required_properties, memo, result);
    return result;
}

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
    return read_step->getMergeTreeData().isSharedStorage() || memo.getEnvironment().distributed_plan_execute_locally;
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
    auto replicated_read_expression = makeBucketedReadVariant(
        expression, node_count, /*target_buckets=*/1,
        std::make_shared<ReplicatedReadStrategy>(), "ReplicatedRead", /*is_replicated=*/true, actual_buckets);
    if (!replicated_read_expression)
    {
        LOG_TEST(getLogger("ReplicatedRead"), "No replicated read for '{}': its marks cannot be pinned into one bucket",
            read_step->getStepDescription());
        return {};
    }

    std::vector<GroupExpressionPtr> result;
    addPhysicalToMemo(replicated_read_expression, required_properties, memo, result);
    return result;
}

/// Unsorted single-node read: fallback for `ReadFromMergeTree` at {1 node}.
/// `ReadFromMergeTree` is excluded from `DefaultImplementation` so that specialized
/// read rules (`ParallelRead`, `ReplicatedRead`) handle it.
class LocalReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "LocalRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr;
    }
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);
        /// No distribution propagation: output stays at default {1 node}.
        std::vector<GroupExpressionPtr> result;
        addPhysicalToMemo(implementation_expression, required_properties, memo, result);
        return result;
    }
};

OptimizationRulePtr createLocalReadImplementation();
OptimizationRulePtr createLocalReadImplementation() { return std::make_shared<LocalReadImplementation>(); }
OptimizationRulePtr createParallelReadImplementation();
OptimizationRulePtr createParallelReadImplementation() { return std::make_shared<ParallelReadImplementation>(); }
OptimizationRulePtr createReplicatedReadImplementation();
OptimizationRulePtr createReplicatedReadImplementation() { return std::make_shared<ReplicatedReadImplementation>(); }

}
