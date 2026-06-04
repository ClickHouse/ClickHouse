#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Rewrite bare numeric columns used in boolean context inside a filter DAG into
/// explicit `notEquals(col, 0)` comparisons so that `KeyCondition` can build
/// index atoms from them. Without this, `WHERE id` (a bare INPUT node) produces
/// no primary-key condition, while the equivalent `WHERE id != 0` does.
///
/// The rewrite recursively descends through the same logical operators that
/// `RPNBuilder` traverses (`not` / `and` / `or`), so nested expressions like
/// `WHERE (id OR flag) AND value` are fully handled. This is intentionally
/// scoped to the second-pass filter-to-source attachment and never runs on
/// projection candidate DAGs or join-marker filters. See #89222.
void rewriteBareColumnFilters(ActionsDAG & dag, std::string & filter_column_name)
{
    auto is_bare_column = [](const ActionsDAG::Node * n) -> bool
    {
        while (n && n->type == ActionsDAG::ActionType::ALIAS)
            n = n->children.empty() ? nullptr : n->children.front();
        return n && n->type == ActionsDAG::ActionType::INPUT;
    };

    auto is_numeric_filter_type = [](const DataTypePtr & type) -> bool
    {
        auto inner = removeNullable(removeLowCardinality(type));
        if (inner->onlyNull())
            return false;
        WhichDataType which(inner);
        return which.isInteger() || which.isFloat();
    };

    auto is_logical = [](const ActionsDAG::Node * n) -> bool
    {
        if (n->type != ActionsDAG::ActionType::FUNCTION || !n->function_base)
            return false;
        const auto & name = n->function_base->getName();
        return name == "not" || name == "and" || name == "or";
    };

    FunctionOverloadResolverPtr ne_resolver;

    auto rewrite_bare = [&](const ActionsDAG::Node * bare_node) -> const ActionsDAG::Node *
    {
        if (!is_bare_column(bare_node) || !is_numeric_filter_type(bare_node->result_type))
            return nullptr;

        if (!ne_resolver)
            ne_resolver = FunctionFactory::instance().get("notEquals", nullptr);

        auto inner_type = removeNullable(removeLowCardinality(bare_node->result_type));
        auto zero_column = bare_node->result_type->createColumnConst(1, inner_type->getDefault());
        const auto & zero_node = dag.addColumn({zero_column, bare_node->result_type, bare_node->result_name + "__zero"});
        return &dag.addFunction(ne_resolver, {bare_node, &zero_node}, {});
    };

    /// Recursively rewrite bare-column leaves inside logical (`not`/`and`/`or`)
    /// sub-trees, mirroring the descent that `RPNBuilder` performs.
    std::function<const ActionsDAG::Node *(const ActionsDAG::Node *)> rewrite_recursive =
        [&](const ActionsDAG::Node * node) -> const ActionsDAG::Node *
    {
        if (const auto * r = rewrite_bare(node))
            return r;

        if (!is_logical(node))
            return nullptr;

        bool any_changed = false;
        ActionsDAG::NodeRawConstPtrs new_children = node->children;
        for (auto *& child : new_children)
        {
            if (const auto * r = rewrite_recursive(child))
            {
                child = r;
                any_changed = true;
            }
        }
        if (!any_changed)
            return nullptr;

        return &dag.addFunction(node->function_base, std::move(new_children), {});
    };

    for (auto *& output : dag.getOutputs())
    {
        if (output->result_name != filter_column_name)
            continue;

        if (const auto * replacement = rewrite_recursive(output))
        {
            filter_column_name = replacement->result_name;
            output = replacement;
        }

        break;
    }
}

}

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();
    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);

    /// Collect ExpressionStep DAGs encountered while walking up the plan.
    /// When a filter references columns produced by expressions (e.g., ALIAS
    /// columns computed in "Compute alias columns" step, or renamed in
    /// "Change column names to column identifiers" step), we compose the
    /// filter through these expression DAGs so that column references are
    /// resolved to physical columns. This is essential for correct index
    /// analysis when plan optimizations like mergeExpressions have not
    /// merged these steps into the filter.
    std::vector<const ActionsDAG *> expression_dags;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            auto filter_dag = filter_step->getExpression().clone();
            auto filter_column_name = filter_step->getFilterColumnName();

            /// Compose filter through accumulated expression DAGs
            /// (in bottom-to-top order). This resolves column identifiers
            /// to their underlying expressions, enabling correct index
            /// matching for ALIAS columns and renamed columns.
            for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
                filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));

            rewriteBareColumnFilters(filter_dag, filter_column_name);

            source_step_with_filter->addFilter(std::move(filter_dag), filter_column_name);
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            expression_dags.push_back(&expression_step->getExpression());
            continue;
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(iter->node->step.get()))
        {
            source_step_with_filter->addFilter(object_filter_step->getExpression().clone(), object_filter_step->getFilterColumnName());
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
