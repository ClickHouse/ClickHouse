#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Interpreters/ActionsDAG.h>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB::QueryPlanOptimizations
{

/// A merged step's description names the two steps it absorbed. Steps that never had one of their own
/// would render as `( + )`, which tells a reader less than the empty description it replaced, so keep it
/// empty in that case.
static String mergeStepDescriptions(std::string_view parent, std::string_view child)
{
    if (parent.empty() && child.empty())
        return {};
    return fmt::format("({} + {})", parent, child);
}

static void removeFromOutputs(ActionsDAG & dag, const ActionsDAG::Node & node)
{
    auto & outputs = dag.getOutputs();
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (&node == outputs[i])
        {
            outputs.erase(outputs.begin() + i);
            return;
        }
    }
}

size_t tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return false;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * parent_expr = typeid_cast<ExpressionStep *>(parent.get());
    auto * parent_filter = typeid_cast<FilterStep *>(parent.get());
    auto * child_expr = typeid_cast<ExpressionStep *>(child.get());

    if (parent_expr && child_expr)
    {
        auto & child_actions = child_expr->getExpression();
        auto & parent_actions = parent_expr->getExpression();

        /// We cannot combine actions with arrayJoin and stateful function because we not always can reorder them.
        /// Example: select rowNumberInBlock() from (select arrayJoin([1, 2]))
        /// Such a query will return two zeroes if we combine actions together.
        /// A function that is merely non-deterministic within the query (`rand64`, `generateUUIDv4`,
        /// ...) has the same problem for the same reason: merged into the child DAG it is computed
        /// once per source row and the value is then replicated across the rows the `arrayJoin`
        /// expands, instead of being drawn once per output row.
        if (child_actions.hasArrayJoin()
            && (parent_actions.hasStatefulFunctions() || dagContainsNonDeterministicFunction(parent_actions)))
            return 0;

        auto merged = ActionsDAG::merge(std::move(child_actions), std::move(parent_actions));

        auto expr = std::make_unique<ExpressionStep>(child_expr->getInputHeaders().front(), std::move(merged));
        expr->setStepDescription(mergeStepDescriptions(parent_expr->getStepDescription(), child_expr->getStepDescription()), settings.max_step_description_length);

        parent_node->step = std::move(expr);
        parent_node->children.swap(child_node->children);
        return 1;
    }
    if (parent_filter && child_expr)
    {
        auto & child_actions = child_expr->getExpression();
        auto & parent_actions = parent_filter->getExpression();

        /// Same as for the expression step above: a stateful or non-deterministic filter must not be
        /// computed before the `arrayJoin` replicates the rows it applies to.
        if (child_actions.hasArrayJoin()
            && (parent_actions.hasStatefulFunctions() || dagContainsNonDeterministicFunction(parent_actions)))
            return 0;

        auto merged = ActionsDAG::merge(std::move(child_actions), std::move(parent_actions));
        /// merge can drag materialize wrappers from a UNION child into the filter (#78166); folding through
        /// them is left to the `FilterStep` constructor below, which does it after `deduplicateSubtrees` -
        /// a fold before dedup could have its masked-secret constant replaced by an equal plain one
        merged.deduplicateSubtrees();

        auto filter = std::make_unique<FilterStep>(
            child_expr->getInputHeaders().front(),
            std::move(merged),
            parent_filter->getFilterColumnName(),
            parent_filter->removesFilterColumn());
        filter->setStepDescription(mergeStepDescriptions(parent_filter->getStepDescription(), child_expr->getStepDescription()), settings.max_step_description_length);
        if (parent_filter->isRowLevelFilterAfterRead())
            filter->markAsRowLevelFilterAfterRead();

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    return 0;
}

/// Is the filter of the node over a row-level filter applied after the read (`FilterStep::isRowLevelFilterAfterRead`),
/// or over a read whose row-level filter is applied after it (`SelectQueryInfo::row_level_filter_after_read`)?
static bool isOverReadWithRowLevelFilterAfterIt(const QueryPlan::Node & node)
{
    const auto * current = &node;
    while (true)
    {
        const auto * filter = typeid_cast<const FilterStep *>(current->step.get());
        if (filter && filter->isRowLevelFilterAfterRead())
            return true;

        if (current->children.size() != 1 || !(filter || typeid_cast<const ExpressionStep *>(current->step.get())))
            break;

        current = current->children.front();
    }

    const auto * source = dynamic_cast<const SourceStepWithFilter *>(current->step.get());
    return source && source->getQueryInfo().row_level_filter_after_read;
}

/// Can every result of the DAG besides its filter be computed on any row? See `canBeMovedToTheFrontOfAndChain`.
static bool computesOnlyMovableResults(const ActionsDAG & dag, const ActionsDAG::Node & filter_node)
{
    return std::ranges::all_of(dag.getOutputs(),
        [&](const ActionsDAG::Node * output) { return output == &filter_node || canBeMovedToTheFrontOfAndChain(output); });
}

size_t tryMergeFilters(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return false;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * parent_filter = typeid_cast<FilterStep *>(parent.get());
    auto * child_filter = typeid_cast<FilterStep *>(child.get());

    if (parent_filter && child_filter)
    {
        auto & child_actions = child_filter->getExpression();
        auto & parent_actions = parent_filter->getExpression();

        if (child_actions.hasArrayJoin())
            return 0;

        /// A filter with a stateful function is not split into its AND chain, so the upper filter would see the rows the lower one rejects.
        if (child_actions.hasStatefulFunctions() || parent_actions.hasStatefulFunctions())
            return 0;

        const auto & child_filter_node = child_actions.findInOutputs(child_filter->getFilterColumnName());

        /// The merged filter is not always split into its AND chain (`query_plan_merge_filters`), and never at a column atom, so over
        /// a read whose row-level filter is applied after it, merge only what can be evaluated on the rows that filter rejects.
        if (isOverReadWithRowLevelFilterAfterIt(*child_node))
        {
            const auto & upper_filter_node = parent_actions.findInOutputs(parent_filter->getFilterColumnName());
            if (!canAndChainAtomsBeMovedToTheFront(&upper_filter_node)
                || !computesOnlyMovableResults(parent_actions, upper_filter_node)
                || (hasTrivialAndChainAtom(&child_filter_node) && !canAndChainAtomsBeMovedToTheFront(&child_filter_node)))
                return 0;
        }
        if (child_filter->removesFilterColumn())
            removeFromOutputs(child_actions, child_filter_node);

        child_actions.mergeInplace(std::move(parent_actions));

        const auto & parent_filter_node = child_actions.findInOutputs(parent_filter->getFilterColumnName());
        if (parent_filter->removesFilterColumn())
            removeFromOutputs(child_actions, parent_filter_node);

        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto & condition = child_actions.addFunction(func_builder_and, {&child_filter_node, &parent_filter_node}, {});
        auto & outputs = child_actions.getOutputs();
        outputs.insert(outputs.begin(), &condition);
        /// condition name may be changed by deduplicateSubtrees
        auto condition_name = condition.result_name;

        child_actions.deduplicateSubtrees();

        auto filter = std::make_unique<FilterStep>(child_filter->getInputHeaders().front(),
                                                   std::move(child_actions),
                                                   condition_name,
                                                   true);
        filter->setStepDescription(mergeStepDescriptions(parent_filter->getStepDescription(), child_filter->getStepDescription()), settings.max_step_description_length);
        if (child_filter->isRowLevelFilterAfterRead() || parent_filter->isRowLevelFilterAfterRead())
            filter->markAsRowLevelFilterAfterRead();

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    return 0;
}

/// an always-true `FilterStep` stays above a join and splits the join graph
size_t tryReplaceAlwaysTrueFilter(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    auto * filter = typeid_cast<FilterStep *>(node->step.get());
    if (!filter || !filter->removesFilterColumn())
        return 0;

    auto & dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & filter_node = dag.findInOutputs(filter_column_name);
    if (filter_node.type != ActionsDAG::ActionType::COLUMN || !ConstantFilterDescription(*filter_node.column).always_true)
        return 0;

    dag.removeUnusedResult(filter_column_name);
    dag.removeUnusedActions(false, false);
    auto expression = std::make_unique<ExpressionStep>(filter->getInputHeaders().front(), std::move(dag));
    expression->setStepDescription(*filter);
    node->step = std::move(expression);
    return 1;
}

}
