#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Core/SortDescription.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Trace a column name back through ALIAS chains to the original INPUT name.
/// Returns empty for FUNCTION nodes (computed columns).
static String traceColumnToInput(const ActionsDAG & dag, const String & output_name)
{
    const ActionsDAG::Node * node = dag.tryFindInOutputs(output_name);
    if (!node)
        return {};
    while (node->type == ActionsDAG::ActionType::ALIAS)
    {
        chassert(node->children.size() == 1);
        node = node->children.front();
    }
    if (node->type == ActionsDAG::ActionType::INPUT)
        return node->result_name;
    return {};
}

/// Translate distribution column names through an ActionsDAG to input names.
/// Returns false if any column set becomes empty (all computed — reject passthrough).
static bool translateDistributionColumns(const ActionsDAG & dag, std::vector<NameSet> & columns)
{
    for (auto & column_set : columns)
    {
        NameSet translated;
        for (const auto & name : column_set)
        {
            String input_name = traceColumnToInput(dag, name);
            if (!input_name.empty())
            {
                translated.insert(input_name);
            }
            else if (!dag.tryFindInOutputs(name))
            {
                /// Not in DAG outputs — may be a passthrough input column. Keep it.
                translated.insert(name);
            }
            /// else: computed by this step (FUNCTION) — not present in input. Drop it.
        }
        if (translated.empty())
            return false;
        column_set = std::move(translated);
    }
    return true;
}

/// Translate sort column names through an ActionsDAG to input names.
/// Returns false if any column is computed (FUNCTION node).
static bool translateSortDescription(const ActionsDAG & dag, SortDescription & sort_desc)
{
    for (auto & col_desc : sort_desc)
    {
        String input_name = traceColumnToInput(dag, col_desc.column_name);
        if (!input_name.empty())
        {
            col_desc.column_name = input_name;
        }
        else if (!dag.tryFindInOutputs(col_desc.column_name))
        {
            /// Not in DAG outputs — may be a passthrough input column. Keep it.
        }
        else
        {
            /// Computed by this step (FUNCTION) — can't translate to input.
            return false;
        }
    }
    return true;
}

/// Get the `ActionsDAG` from an `ExpressionStep` or `FilterStep`, or nullptr.
static const ActionsDAG * tryGetActionsDAG(const IQueryPlanStep * step)
{
    if (const auto * expr_step = typeid_cast<const ExpressionStep *>(step))
        return &expr_step->getExpression();
    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        return &filter_step->getExpression();
    return nullptr;
}

/// Stateless per-row steps that can run on any data partition independently.
static bool isDistributionPassthrough(const IQueryPlanStep * step)
{
    return typeid_cast<const ExpressionStep *>(step) != nullptr
        || typeid_cast<const FilterStep *>(step) != nullptr
        || typeid_cast<const BuildRuntimeFilterStep *>(step) != nullptr;
}


/// Implementation rule for stateless per-row steps. Propagates distribution to the
/// input and creates speculative multi-node variants at each candidate node count.
/// Also creates sorted passthrough variants that delegate sorting to the child group,
/// enabling `SortedRead` to eliminate explicit Sort steps for PK-aligned ORDER BY.
class DistributionPassthrough : public IOptimizationRule
{
public:
    String getName() const override { return "DistributionPassthrough"; }

    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return isDistributionPassthrough(expression->getQueryPlanStep());
    }

    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        std::vector<GroupExpressionPtr> result;

        /// Implementation at the parent's required distribution.
        if (auto implementation_expression = createAtDistribution(expression, required_properties, required_properties.distribution))
        {
            memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
            result.push_back(implementation_expression);
        }

        /// Speculative implementations at each candidate node count.
        auto candidates = getCandidateNodeCounts(memo.getClusterNodeCount());
        for (size_t candidate : candidates)
        {
            if (candidate == required_properties.distribution.node_count)
                continue;

            DistributionDescription dist;
            dist.node_count = candidate;

            if (auto implementation_expression = createAtDistribution(expression, required_properties, dist))
            {
                memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
                result.push_back(implementation_expression);
            }
        }

        /// Sorted passthrough: delegate sorting to the child (column names translated
        /// through the DAG).  Competes with unsorted variant + `SortingEnforcer`.
        /// When child has `SortedRead` matching PK, eliminates Sort entirely.
        if (!required_properties.sorting.empty())
        {
            const ActionsDAG * dag = tryGetActionsDAG(expression->getQueryPlanStep());
            SortDescription input_sorting = required_properties.sorting;
            bool can_translate = !dag || translateSortDescription(*dag, input_sorting);

            if (can_translate)
            {
                auto create_sorted_variant = [&](const DistributionDescription & dist)
                {
                    auto sorted_impl = std::make_shared<GroupExpression>(*expression);
                    sorted_impl->setApplied(*this, required_properties);

                    chassert(sorted_impl->inputs.size() == 1);
                    auto & sorted_input_props = sorted_impl->inputs[0].required_properties;

                    sorted_input_props.distribution = dist;
                    sorted_input_props.sorting = input_sorting;
                    sorted_input_props.sort_limit = required_properties.sort_limit;

                    sorted_impl->properties.distribution = dist;
                    sorted_impl->properties.sorting = required_properties.sorting;
                    sorted_impl->properties.sort_limit = required_properties.sort_limit;

                    memo.getGroup(expression->group_id)->addPhysicalExpression(sorted_impl);
                    result.push_back(sorted_impl);
                };

                for (size_t candidate : candidates)
                {
                    DistributionDescription dist;
                    dist.node_count = candidate;
                    create_sorted_variant(dist);
                }

                create_sorted_variant({});
            }
        }

        return result;
    }

private:
    GroupExpressionPtr createAtDistribution(
        const GroupExpressionPtr & expression,
        const ExpressionProperties & required_properties,
        const DistributionDescription & distribution) const
    {
        auto implementation_expression = std::make_shared<GroupExpression>(*expression);
        implementation_expression->setApplied(*this, required_properties);

        chassert(implementation_expression->inputs.size() == 1);
        auto & input_props = implementation_expression->inputs[0].required_properties;

        /// Construction-time sorting conflicts with multi-node distribution.
        if (!input_props.sorting.empty() && distribution.node_count > 1)
            return nullptr;

        if (input_props.sorting.empty())
        {
            input_props.distribution = distribution;

            /// Translate distribution column names through the step's DAG.
            if (!input_props.distribution.columns.empty())
            {
                const ActionsDAG * dag = tryGetActionsDAG(implementation_expression->plan_step.get());
                if (dag && !translateDistributionColumns(*dag, input_props.distribution.columns))
                    return nullptr;
            }

            implementation_expression->properties.distribution = distribution;
        }

        return implementation_expression;
    }
};


OptimizationRulePtr createDistributionPassthrough() { return std::make_shared<DistributionPassthrough>(); }

}
