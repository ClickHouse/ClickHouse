#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
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

/// Splits a table read across N nodes — each node reads 1/N of the data.
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
    return typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr &&
        required_properties.distribution.node_count > 1 &&
        !required_properties.distribution.is_replicated;
}

std::vector<GroupExpressionPtr> ParallelReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    /// Produce a distributed read that splits work uniformly across all nodes.
    /// DefaultImplementation handles the single-node (local) read.
    auto parallel_read_step_ptr = read_step->clone();
    auto * parallel_read_step = typeid_cast<ReadFromMergeTree *>(parallel_read_step_ptr.get());
    if (!parallel_read_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ParallelReadImplementation: clone() of ReadFromMergeTree returned unexpected step type for expression '{}'",
            expression->getDescription());

    parallel_read_step->setDistributedRead(node_count);
    parallel_read_step->setStepDescription(fmt::format("ParallelRead {}", read_step->getStepDescription()), 200);

    GroupExpressionPtr parallel_read_expression = std::make_shared<GroupExpression>(*expression);
    parallel_read_expression->plan_step = std::move(parallel_read_step_ptr);
    parallel_read_expression->strategy = std::make_shared<ParallelReadStrategy>();

    ExpressionProperties parallel_properties;
    parallel_properties.distribution.node_count = node_count;
    parallel_read_expression->properties = parallel_properties;

    parallel_read_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(parallel_read_expression);

    return {parallel_read_expression};
}

/// Replicated read on shared storage: every node reads the full table directly from
/// object storage (S3).  No `setDistributedRead` — each node reads all data.
/// Satisfies `{node_count=N, is_replicated=true}` without a `BroadcastExchange`,
/// eliminating network transfer for dimension tables in broadcast joins.
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

bool ReplicatedReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep()) != nullptr &&
        required_properties.distribution.node_count > 1 &&
        required_properties.distribution.is_replicated;
}

std::vector<GroupExpressionPtr> ReplicatedReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const size_t node_count = required_properties.distribution.node_count;

    LOG_TEST(getLogger("ReplicatedRead"), "Creating replicated read for '{}' at {} nodes",
        read_step->getStepDescription(), node_count);

    /// Clone the read step without calling setDistributedRead — each node reads the full table.
    auto replicated_read_step_ptr = read_step->clone();
    auto * replicated_read_step = typeid_cast<ReadFromMergeTree *>(replicated_read_step_ptr.get());
    if (!replicated_read_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReplicatedReadImplementation: clone() of ReadFromMergeTree returned unexpected step type for expression '{}'",
            expression->getDescription());

    replicated_read_step->setStepDescription(fmt::format("ReplicatedRead {}", read_step->getStepDescription()), 200);

    GroupExpressionPtr replicated_read_expression = std::make_shared<GroupExpression>(*expression);
    replicated_read_expression->plan_step = std::move(replicated_read_step_ptr);
    replicated_read_expression->strategy = std::make_shared<ReplicatedReadStrategy>();

    ExpressionProperties replicated_properties;
    replicated_properties.distribution.node_count = node_count;
    replicated_properties.distribution.is_replicated = true;
    replicated_read_expression->properties = replicated_properties;

    replicated_read_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(replicated_read_expression);

    return {replicated_read_expression};
}

/// Match required sorting against MergeTree's sorting key prefix.
/// Returns {prefix_size, direction} or {0, 0} if no match.
/// Simplified: only direct column name matches (no monotonic functions or DAG traversal).
static std::pair<size_t, int> matchSortingToKey(
    const SortDescription & required_sorting,
    const KeyDescription & sorting_key)
{
    if (required_sorting.empty() || sorting_key.column_names.empty())
        return {0, 0};

    int direction = 0;
    size_t prefix_size = 0;

    for (size_t i = 0; i < required_sorting.size() && i < sorting_key.column_names.size(); ++i)
    {
        if (required_sorting[i].column_name != sorting_key.column_names[i])
            break;
        if (required_sorting[i].collator)
            break;

        int key_reverse = (!sorting_key.reverse_flags.empty() && sorting_key.reverse_flags[i]) ? -1 : 1;
        int col_direction = required_sorting[i].direction * key_reverse;

        if (direction == 0)
            direction = col_direction;
        else if (direction != col_direction)
            break;

        ++prefix_size;
    }
    return {prefix_size, direction};
}

/// Reads data in primary key order when the required sorting matches a PK prefix.
/// Eliminates the explicit `SortingStep` by providing pre-sorted output.
class SortedReadImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "SortedRead"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool SortedReadImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    if (required_properties.sorting.empty())
        return false;

    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    if (!read_step)
        return false;

    auto [prefix_size, direction] = matchSortingToKey(
        required_properties.sorting, read_step->getStorageMetadata()->getSortingKey());
    return prefix_size > 0 && direction != 0;
}

std::vector<GroupExpressionPtr> SortedReadImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const auto & sorting_key = read_step->getStorageMetadata()->getSortingKey();

    auto [prefix_size, direction] = matchSortingToKey(required_properties.sorting, sorting_key);

    std::vector<GroupExpressionPtr> result;

    /// Helper: create a sorted read variant at a given node count.
    auto create_sorted_variant = [&](size_t node_count, bool is_parallel)
    {
        auto sorted_step_ptr = read_step->clone();
        auto * sorted_step = typeid_cast<ReadFromMergeTree *>(sorted_step_ptr.get());
        if (!sorted_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "SortedReadImplementation: clone of ReadFromMergeTree returned unexpected step type");

        sorted_step->requestReadingInOrder(prefix_size, direction, required_properties.sort_limit);
        if (is_parallel)
            sorted_step->setDistributedRead(node_count);

        sorted_step->setStepDescription(
            fmt::format("{} {}",
                is_parallel ? "SortedParallelRead" : "SortedRead",
                read_step->getStepDescription()), 200);

        GroupExpressionPtr sorted_expr = std::make_shared<GroupExpression>(*expression);
        sorted_expr->plan_step = std::move(sorted_step_ptr);
        sorted_expr->strategy = std::make_shared<SortedReadStrategy>();

        ExpressionProperties props;
        props.distribution.node_count = node_count;
        props.sorting = required_properties.sorting;
        props.sort_limit = required_properties.sort_limit;
        sorted_expr->properties = props;

        sorted_expr->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(sorted_expr);
        result.push_back(sorted_expr);
    };

    /// Local sorted read at {1 node}.
    if (required_properties.distribution.node_count <= 1)
        create_sorted_variant(1, false);

    /// Sorted parallel read at {N nodes} — each node reads its partition in sort order.
    if (required_properties.distribution.node_count > 1 && !required_properties.distribution.is_replicated)
        create_sorted_variant(required_properties.distribution.node_count, true);

    return result;
}

/// Unsorted single-node read: fallback for `ReadFromMergeTree` at {1 node}.
/// `ReadFromMergeTree` is excluded from `DefaultImplementation` so that specialized
/// read rules (`ParallelRead`, `ReplicatedRead`, `SortedRead`) handle it.
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
        implementation_expression->setApplied(*this, required_properties);
        /// No distribution propagation: output stays at default {1 node}.
        memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
        return {implementation_expression};
    }
};

OptimizationRulePtr createLocalReadImplementation() { return std::make_shared<LocalReadImplementation>(); }
OptimizationRulePtr createParallelReadImplementation() { return std::make_shared<ParallelReadImplementation>(); }
OptimizationRulePtr createReplicatedReadImplementation() { return std::make_shared<ReplicatedReadImplementation>(); }
OptimizationRulePtr createSortedReadImplementation() { return std::make_shared<SortedReadImplementation>(); }

}
