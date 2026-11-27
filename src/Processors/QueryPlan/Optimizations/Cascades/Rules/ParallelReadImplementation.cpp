#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

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

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    /// Produce a distributed read that splits work uniformly across all nodes: the coordinator
    /// computes each bucket's authoritative marks and the fan-out ships them to the workers in
    /// the per-read bucket task parameters. `LocalReadImplementation` handles the single-node read.
    size_t actual_buckets = 0;
    auto parallel_read_expression = tryMakeBucketedReadVariant(
        expression, node_count, /*target_buckets=*/node_count,
        strategySingleton<ParallelReadStrategy>(), "ParallelRead", /*is_replicated=*/false, actual_buckets);
    if (!parallel_read_expression)
    {
        LOG_TEST(getLogger("ParallelRead"), "No parallel read for '{}': the read splits into {} buckets instead of {}",
            read_step->getStepDescription(), actual_buckets, node_count);
        return {};
    }

    return addPhysicalToMemo(parallel_read_expression, required_properties, memo);
}

OptimizationRulePtr createParallelReadImplementation() { return std::make_shared<ParallelReadImplementation>(); }

}
