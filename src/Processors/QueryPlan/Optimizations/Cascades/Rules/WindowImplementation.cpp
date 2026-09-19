#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <fmt/format.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{
    bool keyTypeBreaksHashSharding(const IDataType & type);
}

/// Implements a window on a single node (always available) and, for a window with
/// `PARTITION BY`, on each node: the input requirement asks for rows with equal partition
/// keys on one node and for the window's sort order on each node; the enforcers add the
/// keyed shuffle and the per-node sort, so each node computes its partitions on its own.
class WindowImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "Window"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return typeid_cast<const WindowStep *>(expression->getQueryPlanStep()) != nullptr;
    }

    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// The partition keys as distribution columns, or empty when the window cannot run per
/// node: without `PARTITION BY` all rows form one partition, and only a single node keeps
/// them together (empty distribution columns mean "any distribution"); a partition key
/// that is not a column of the input header cannot be hashed by name; and a key whose
/// hash disagrees with `compareAt` (floats, `JSON`, `Dynamic`: `-0.` and `0.` compare as
/// one partition but hash differently) would split one logical partition across nodes
/// and produce wrong window values.
static DistributionColumns partitionKeyColumns(const WindowStep & window_step)
{
    const auto & partition_by = window_step.getWindowDescription().partition_by;
    if (partition_by.empty())
        return {};

    const auto & input_header = window_step.getInputHeaders().at(0);
    DistributionColumns columns;
    for (const auto & partition_column : partition_by)
    {
        if (!input_header->has(partition_column.column_name))
            return {};
        if (QueryPlanOptimizations::keyTypeBreaksHashSharding(*input_header->getByName(partition_column.column_name).type))
            return {};
        columns.push_back(NameSet{partition_column.column_name});
    }
    return columns;
}

std::vector<GroupExpressionPtr> WindowImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * window_step = typeid_cast<const WindowStep *>(expression->getQueryPlanStep());
    if (!window_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "WindowImplementation::applyImpl called for non-WindowStep expression '{}'",
            expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "WindowImplementation::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    std::vector<GroupExpressionPtr> result;

    /// Single-node implementation - always available, and the only one when the window
    /// cannot be distributed.
    {
        auto single_node = std::make_shared<GroupExpression>(*expression);
        single_node->strategy = strategySingleton<WindowStrategy>();
        single_node->properties = ExpressionProperties{};    /// node_count=1 (default)
        /// `WindowTransform` emits rows in input order and within their stream, so the
        /// sorting and the stream layout required from the input also hold for the output.
        /// With `streams_fan_out` the output is split into fresh streams, so neither is
        /// promised.
        if (!window_step->hasStreamsFanOut())
        {
            single_node->properties.sorting = single_node->inputs[0].required_properties.sorting;
            single_node->properties.stream_layout = single_node->inputs[0].required_properties.stream_layout;
            single_node->properties.stream_disjoint_columns = single_node->inputs[0].required_properties.stream_disjoint_columns;
        }

        addPhysicalToMemo(single_node, required_properties, memo, result);
    }

    const DistributionColumns partition_columns = partitionKeyColumns(*window_step);
    if (partition_columns.empty())
        return result;

    for (size_t candidate_node_count : getCandidateNodeCounts(memo.getContext().cluster_node_count))
    {
        auto distributed = std::make_shared<GroupExpression>(*expression);
        /// The fan-out re-parallelizes one node's pipeline after the last window; here the
        /// parallelism comes from the nodes, and the fan-out would only destroy the
        /// output order.
        auto new_step = std::make_unique<WindowStep>(
            window_step->getInputHeaders().at(0),
            window_step->getWindowDescription(),
            window_step->getWindowFunctions(),
            /*streams_fan_out_=*/false);
        new_step->setStepDescription(fmt::format("Partitioned {}", window_step->getStepDescription()), 200);
        distributed->plan_step = std::move(new_step);
        distributed->strategy = strategySingleton<WindowStrategy>();

        ExpressionProperties input_required;
        input_required.distribution.node_count = candidate_node_count;
        input_required.distribution.columns = partition_columns;
        input_required.sorting = window_step->getWindowDescription().full_sort_description;
        /// Streams disjoint on the partition keys let each node run the window on several
        /// streams at once: every partition lands whole in one stream.
        input_required.setDisjointStreams(partition_columns);
        distributed->inputs[0].required_properties = input_required;

        distributed->properties = ExpressionProperties{};
        /// The window moves no rows and keeps them within their stream, so the output keeps
        /// the input distribution and stream layout; without the fan-out it also keeps the
        /// input order.
        distributed->properties.distribution = input_required.distribution;
        distributed->properties.sorting = input_required.sorting;
        distributed->properties.stream_layout = input_required.stream_layout;
        distributed->properties.stream_disjoint_columns = input_required.stream_disjoint_columns;

        addPhysicalToMemo(distributed, required_properties, memo, result);
    }

    return result;
}

OptimizationRulePtr createWindowImplementation() { return std::make_shared<WindowImplementation>(); }

}
