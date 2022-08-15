#include <Parsers/ASTWindowDefinition.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/IColumn.h>


namespace DB::QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(QueryPlan::Node * node)
{
    IQueryPlanStep * step = node->step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return reading;

    if (node->children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(node->children.front());

    return nullptr;
}

void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression, NameSet & filter_columns)
{
    /// Expression may replace a column. Check it.
    /// TODO: there may possibly be a chain of aliases, we may check it and update filter names
    for (auto & output : expression->getOutputs())
        if (filter_columns.contains(output->result_name) && output->type != ActionsDAG::ActionType::INPUT)
            filter_columns.erase(output->result_name);

    if (dag)
        dag = ActionsDAG::merge(std::move(*dag), std::move(*expression->clone()));
    else
        dag = expression->clone();
}

void buildSortingDAG(QueryPlan::Node * node, ActionsDAGPtr & dag, NameSet & filter_columns)
{
    IQueryPlanStep * step = node->step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return;

    if (node->children.size() != 1)
        return;

    buildSortingDAG(node->children.front(), dag, filter_columns);

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
        appendExpression(dag, expression->getExpression(), filter_columns);

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        appendExpression(dag, filter->getExpression(), filter_columns);
        filter_columns.insert(filter->getFilterColumnName());
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->arrayJoin()->columns;

        /// Can't use filter whihc happened before ARRAY JOIN.
        /// If there were a condition on const array, after ARRAY JOIN it can be broken.
        /// Example : 'SELECT y FROM (select y from table where x = 1) ARRAY JOIN [1, 2] as x ORDER BY x'
        /// where 'table' has it's own column 'x' in sorting key 'ORDER BY x'.
        for (const auto & column : array_joined_columns)
            filter_columns.erase(column);

        /// Remove array joined columns from outputs.
        /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
        ActionsDAG::NodeRawConstPtrs outputs;
        outputs.reserve(dag->getOutputs().size());

        for (const auto & output : dag->getOutputs())
        {
            if (!array_joined_columns.contains(output->result_name))
                outputs.push_back(output);
        }
    }
}

size_t calculateFixedPrefixSize(const ActionsDAG & dag, NameSet & filter_columns, const Names & sorting_key_columns)
{
    NameSet fiexd_columns;
    std::vector<const ActionsDAG::Node *> stack;
    for (const auto * output : dag.getOutputs())
    {
        if (auto it = filter_columns.find(output->result_name); it != filter_columns.end())
        {
            filter_columns.erase(it);
            stack.clear();
            stack.push_back(output);
            while (!stack.empty())
            {
                const auto * node = stack.back();
                stack.pop_back();
                if (node->type == ActionsDAG::ActionType::FUNCTION)
                {
                    const auto & name = node->function_base->getName();
                    if (name == "and")
                    {
                        for (const auto * arg : node->children)
                            stack.push_back(arg);
                    }
                    else if (name == "equals")
                    {
                        fiexd_columns.insert(node->result_name);
                    }
                }
            }
        }
    }

    size_t prefix_size = 0;
    for (const auto & name : sorting_key_columns)
    {
        if (!fiexd_columns.contains(name))
            break;

        ++prefix_size;
    }

    return prefix_size;
}

/// Optimize in case of exact match with order key element
/// or in some simple cases when order key element is wrapped into monotonic function.
/// Returns on of {-1, 0, 1} - direction of the match. 0 means - doesn't match.
int matchSortDescriptionAndKey(
    const ActionsDAGPtr & dag,
    const SortColumnDescription & sort_column,
    const std::string & sorting_key_column)
{
    /// If required order depend on collation, it cannot be matched with primary key order.
    /// Because primary keys cannot have collations.
    if (sort_column.collator)
        return 0;

    int current_direction = sort_column.direction;
    /// For the path: order by (sort_column, ...)
    bool exact_match = sort_column.column_name == sorting_key_column;
    if (exact_match && !dag)
        return current_direction;

    const auto * node = dag->tryFindInOutputs(sort_column.column_name);
    /// It is possible when e.g. sort by array joined column.
    if (!node)
        return 0;

    if (exact_match && node->type == ActionsDAG::ActionType::INPUT)
        return current_direction;

    while (node->children.size() == 1)
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & func = *node->function_base;
            if (!func.hasInformationAboutMonotonicity())
                return 0;

            auto monotonicity = func.getMonotonicityForRange(*func.getArgumentTypes().at(0), {}, {});
            if (!monotonicity.is_monotonic)
                return 0;

            if (!monotonicity.is_positive)
                current_direction *= -1;

        }
        else if (node->type != ActionsDAG::ActionType::ALIAS)
            return 0;

        node = node->children.front();
        if (node->result_name == sorting_key_column)
            return current_direction;
    }

    return 0;
}

SortDescription buildPrefixSortDescription(
    size_t fixed_prefix_size,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    const Names & sorting_key_columns)
{
    size_t descr_prefix_size = std::min(description.size(), sorting_key_columns.size() - fixed_prefix_size);

    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(description.size());

    for (size_t i = 0; i < fixed_prefix_size; ++i)
        order_key_prefix_descr.push_back(description[i]);

    int read_direction = description.at(0).direction;

    for (size_t i = 0; i < descr_prefix_size; ++i)
    {
        int current_direction = matchSortDescriptionAndKey(
            dag, description[i], sorting_key_columns[i + fixed_prefix_size]);

        if (!current_direction || (i > 0 && current_direction != read_direction))
            break;

        if (i == 0)
            read_direction = current_direction;

        order_key_prefix_descr.push_back(description[i]);
    }

    return order_key_prefix_descr;
}

void optimizeReadInOrder(QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        return;

    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting)
        return;

    ReadFromMergeTree * reading = findReadingStep(node.children.front());
    if (!reading)
        return;

    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    if (sorting_key.column_names.empty())
        return;

    ActionsDAGPtr dag;
    NameSet filter_columns;
    buildSortingDAG(node.children.front(), dag, filter_columns);

    const auto & description = sorting->getSortDescription();
    const auto & sorting_key_columns = sorting_key.column_names;

    size_t fixed_prefix_size = 0;
    if (dag)
        calculateFixedPrefixSize(*dag, filter_columns, sorting_key_columns);

    auto prefix_description = buildPrefixSortDescription(fixed_prefix_size, dag, description, sorting_key_columns);
}

size_t tryReuseStorageOrderingForWindowFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/)
{
    /// Find the following sequence of steps, add InputOrderInfo and apply prefix sort description to
    /// SortingStep:
    /// WindowStep <- SortingStep <- [Expression] <- [SettingQuotaAndLimits] <- ReadFromMergeTree

    auto * window_node = parent_node;
    auto * window = typeid_cast<WindowStep *>(window_node->step.get());
    if (!window)
        return 0;
    if (window_node->children.size() != 1)
        return 0;

    auto * sorting_node = window_node->children.front();
    auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting)
        return 0;
    if (sorting_node->children.size() != 1)
        return 0;

    auto * possible_read_from_merge_tree_node = sorting_node->children.front();

    if (typeid_cast<ExpressionStep *>(possible_read_from_merge_tree_node->step.get()))
    {
        if (possible_read_from_merge_tree_node->children.size() != 1)
            return 0;

        possible_read_from_merge_tree_node = possible_read_from_merge_tree_node->children.front();
    }

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(possible_read_from_merge_tree_node->step.get());
    if (!read_from_merge_tree)
    {
        return 0;
    }

    auto context = read_from_merge_tree->getContext();
    if (!context->getSettings().optimize_read_in_window_order)
    {
        return 0;
    }

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto * select_query = query_info.query->as<ASTSelectQuery>();

    ManyExpressionActions order_by_elements_actions;
    const auto & window_desc = window->getWindowDescription();

    for (const auto & actions_dag : window_desc.partition_by_actions)
    {
        order_by_elements_actions.emplace_back(
            std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(context, CompileExpressions::yes)));
    }

    for (const auto & actions_dag : window_desc.order_by_actions)
    {
        order_by_elements_actions.emplace_back(
            std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(context, CompileExpressions::yes)));
    }

    auto order_optimizer = std::make_shared<ReadInOrderOptimizer>(
            *select_query,
            order_by_elements_actions,
            window->getWindowDescription().full_sort_description,
            query_info.syntax_analyzer_result);

    read_from_merge_tree->setQueryInfoOrderOptimizer(order_optimizer);

    /// If we don't have filtration, we can pushdown limit to reading stage for optimizations.
    UInt64 limit = (select_query->hasFiltration() || select_query->groupBy()) ? 0 : InterpreterSelectQuery::getLimitForSorting(*select_query, context);

    auto order_info = order_optimizer->getInputOrder(
            query_info.projection ? query_info.projection->desc->metadata : read_from_merge_tree->getStorageMetadata(),
            context,
            limit);

    if (order_info)
    {
        read_from_merge_tree->setQueryInfoInputOrderInfo(order_info);
        sorting->convertToFinishSorting(order_info->order_key_prefix_descr);
    }

    return 0;
}

}
