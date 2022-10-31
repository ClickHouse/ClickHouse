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
#include <stack>


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

/// FixedColumns are columns which values become constants after filtering.
/// In a query "SELECT x, y, z FROM table WHERE x = 1 AND y = 'a' ORDER BY x, y, z"
/// Fixed columns are 'x' and 'y'.
using FixedColumns = std::unordered_set<const ActionsDAG::Node *>;

/// Right now we find only simple cases like 'and(..., and(..., and(column = value, ...), ...'
void appendFixedColumnsFromFilterExpression(const ActionsDAG::Node & filter_expression, FixedColumns & fiexd_columns)
{
    std::stack<const ActionsDAG::Node *> stack;
    stack.push(&filter_expression);

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (name == "and")
            {
                for (const auto * arg : node->children)
                    stack.push(arg);
            }
            else if (name == "equals")
            {
                const ActionsDAG::Node * maybe_fixed_column = nullptr;
                bool is_singe = true;
                for (const auto & child : node->children)
                {
                    if (!child->column)
                    {
                        if (!maybe_fixed_column)
                            maybe_fixed_column = child;
                        else
                            is_singe = false;
                    }
                }

                if (maybe_fixed_column && is_singe)
                {
                    std::cerr << "====== Added fixed column " << maybe_fixed_column->result_name << ' ' << static_cast<const void *>(maybe_fixed_column) << std::endl;
                    fiexd_columns.insert(maybe_fixed_column);
                }
            }
        }
    }
}

void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression)
{
    if (dag)
        dag->mergeInplace(std::move(*expression->clone()));
    else
        dag = expression->clone();
}

void buildSortingDAG(QueryPlan::Node * node, ActionsDAGPtr & dag, FixedColumns & fixed_columns)
{
    IQueryPlanStep * step = node->step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return;

    if (node->children.size() != 1)
        return;

    buildSortingDAG(node->children.front(), dag, fixed_columns);

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
        appendExpression(dag, expression->getExpression());

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        appendExpression(dag, filter->getExpression());
        if (const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName()))
            appendFixedColumnsFromFilterExpression(*filter_expression, fixed_columns);
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->arrayJoin()->columns;

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

void enreachFixedColumns(ActionsDAGPtr & dag, FixedColumns & fixed_columns)
{
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited;
    for (const auto & node : dag->getNodes())
    {
        if (visited.contains(&node))
            continue;

        stack.push({&node});
        visited.insert(&node);
        while (!stack.empty())
        {
            auto & frame = stack.top();
            for (; frame.next_child < frame.node->children.size(); ++frame.next_child)
                if (!visited.contains(frame.node->children[frame.next_child]))
                    break;

            if (frame.next_child < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.next_child];
                visited.insert(child);
                stack.push({child});
                ++frame.next_child;
            }
            else
            {
                /// Ignore constants here, will check them separately
                if (!frame.node->column)
                {
                    if (frame.node->type == ActionsDAG::ActionType::ALIAS)
                    {
                        if (fixed_columns.contains(frame.node->children.at(0)))
                            fixed_columns.insert(frame.node);
                    }
                    else if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
                    {
                        if (frame.node->function_base->isDeterministicInScopeOfQuery())
                        {
                            bool all_args_fixed_or_const = true;
                            for (const auto * child : frame.node->children)
                                if (!child->column || !fixed_columns.contains(child))
                                    all_args_fixed_or_const = false;

                            if (all_args_fixed_or_const)
                                fixed_columns.insert(frame.node);
                        }
                    }
                }
            }
        }
    }
}

struct MatchedTrees
{
    struct Monotonicity
    {
        int direction = 1;
        bool strict = true;
    };

    struct Match
    {
        const ActionsDAG::Node * node = nullptr;
        std::optional<Monotonicity> monotonicity;
    };

    using Matches = std::unordered_map<const ActionsDAG::Node *, Match>;
};

MatchedTrees::Matches matchTrees(const ActionsDAG & inner_dag, const ActionsDAG & outer_dag)
{
    using Parents = std::set<const ActionsDAG::Node *>;
    std::unordered_map<const ActionsDAG::Node *, Parents> inner_parents;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inner_inputs;

    {
        std::stack<const ActionsDAG::Node *> stack;
        for (const auto * out : inner_dag.getOutputs())
        {
            if (inner_parents.contains(out))
                continue;

            stack.push(out);
            inner_parents.emplace(out, Parents());
            while (!stack.empty())
            {
                const auto * node = stack.top();
                stack.pop();

                if (node->type == ActionsDAG::ActionType::INPUT)
                    inner_inputs.emplace(node->result_name, node);

                for (const auto * child : node->children)
                {
                    auto [it, inserted] = inner_parents.emplace(child, Parents());
                    it->second.emplace(node);

                    if (inserted)
                        stack.push(child);
                }
            }
        }
    }

    struct Frame
    {
        const ActionsDAG::Node * node;
        ActionsDAG::NodeRawConstPtrs mapped_children;
    };

    MatchedTrees::Matches matches;

    for (const auto * out : outer_dag.getOutputs())
    {
        std::stack<Frame> stack;
        stack.push(Frame{out, {}});
        while (!stack.empty())
        {
            auto & frame = stack.top();
            frame.mapped_children.reserve(frame.node->children.size());

            while (frame.mapped_children.size() < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.mapped_children.size()];
                auto it = matches.find(child);
                if (it == matches.end())
                {
                    stack.push(Frame{child, {}});
                    break;
                }
                frame.mapped_children.push_back(it->second.node);
            }

            if (frame.mapped_children.size() < frame.node->children.size())
                continue;

            if (frame.node->type == ActionsDAG::ActionType::INPUT)
            {
                const ActionsDAG::Node * mapped = nullptr;
                if (auto it = inner_inputs.find(frame.node->result_name); it != inner_inputs.end())
                    mapped = it->second;

                matches.emplace(frame.node, MatchedTrees::Match{.node = mapped});
            }
            else if (frame.node->type == ActionsDAG::ActionType::ALIAS)
            {
                matches.emplace(frame.node, matches[frame.node->children.at(0)]);
            }
            else if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
            {
                auto & match = matches[frame.node];

                bool found_all_children = true;
                for (const auto * child : frame.mapped_children)
                    if (!child)
                        found_all_children = false;

                if (found_all_children && !frame.mapped_children.empty())
                {
                    Parents container;
                    Parents * intersection = &inner_parents[frame.mapped_children[0]];

                    if (frame.mapped_children.size() > 1)
                    {
                        std::vector<Parents *> other_parents;
                        other_parents.reserve(frame.mapped_children.size());
                        for (size_t i = 1; i < frame.mapped_children.size(); ++i)
                            other_parents.push_back(&inner_parents[frame.mapped_children[i]]);

                        for (const auto * parent : *intersection)
                        {
                            bool is_common = true;
                            for (const auto * set : other_parents)
                            {
                                if (!set->contains(parent))
                                {
                                    is_common = false;
                                    break;
                                }
                            }

                            if (is_common)
                                container.insert(parent);
                        }

                        intersection = &container;
                    }

                    if (!intersection->empty())
                    {
                        auto func_name = frame.node->function_base->getName();
                        for (const auto * parent : *intersection)
                        {
                            if (parent->type == ActionsDAG::ActionType::FUNCTION && func_name == parent->function_base->getName())
                            {
                                match.node = parent;
                                break;
                            }
                        }
                    }
                }

                if (!match.node && frame.node->function_base->hasInformationAboutMonotonicity())
                {
                    size_t num_const_args = 0;
                    const ActionsDAG::Node * monotonic_child = nullptr;
                    for (const auto * child : frame.node->children)
                    {
                        if (child->column)
                            ++num_const_args;
                        else
                            monotonic_child = child;
                    }

                    if (monotonic_child && num_const_args + 1 == frame.node->children.size())
                    {
                        const auto & child_match = matches[monotonic_child];
                        if (child_match.node)
                        {
                            auto info = frame.node->function_base->getMonotonicityForRange(*monotonic_child->result_type, {}, {});
                            if (info.is_always_monotonic)
                            {
                                match.node = child_match.node;

                                MatchedTrees::Monotonicity monotonicity;
                                monotonicity.direction *= info.is_positive ? 1 : -1;
                                monotonicity.strict = info.is_strict;

                                if (child_match.monotonicity)
                                {
                                    monotonicity.direction *= child_match.monotonicity->direction;
                                    if (!child_match.monotonicity->strict)
                                        monotonicity.strict = false;
                                }

                                match.monotonicity = monotonicity;
                            }
                        }
                    }
                }
            }

            stack.pop();
        }
    }

    return matches;
}

InputOrderInfoPtr buildInputOrderInfo(
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    const ActionsDAG & sorting_key_dag,
    const Names & sorting_key_columns,
    size_t limit)
{
    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(description.size());

    MatchedTrees::Matches matches;
    FixedColumns fixed_key_columns;

    if (dag)
    {
        matches = matchTrees(sorting_key_dag, *dag);

        for (const auto & [node, match] : matches)
        {
            if (!match.monotonicity || match.monotonicity->strict)
            {
                if (match.node && fixed_columns.contains(node))
                    fixed_key_columns.insert(match.node);
            }
        }
    }

    /// This is a result direction we will read from MergeTree
    ///  1 - in order,
    /// -1 - in reverse order,
    ///  0 - usual read, don't apply optimization
    ///
    /// So far, 0 means any direction is possible. It is ok for constant prefix.
    int read_direction = 0;
    size_t next_descr_column = 0;
    size_t next_sort_key = 0;

    for (; next_descr_column < description.size() && next_sort_key < sorting_key_columns.size(); ++next_sort_key)
    {
        const auto & sorting_key_column = sorting_key_columns[next_sort_key];
        const auto & descr = description[next_descr_column];

        /// If required order depend on collation, it cannot be matched with primary key order.
        /// Because primary keys cannot have collations.
        if (descr.collator)
            break;

        /// Direction for current sort key.
        int current_direction = 0;
        bool strict_monotonic = true;

        const ActionsDAG::Node * sort_column_node = sorting_key_dag.tryFindInOutputs(sorting_key_column);
        /// This should not happen.
        if (!sort_column_node)
            break;

        if (!dag)
        {
            if (sort_column_node->type != ActionsDAG::ActionType::INPUT)
                break;

            if (descr.column_name != sorting_key_column)
                break;

            current_direction = descr.direction;
        }
        else
        {
            const ActionsDAG::Node * sort_node = dag->tryFindInOutputs(descr.column_name);
             /// It is possible when e.g. sort by array joined column.
            if (!sort_node)
                break;

            const auto & match = matches[sort_node];

            // std::cerr << "====== Finding match for " << sort_node->result_name << ' ' << static_cast<const void *>(sort_node) << std::endl;

            if (match.node && match.node == sort_column_node)
            {
                // std::cerr << "====== Found direct match" << std::endl;

                /// We try to find the match first even if column is fixed. In this case, potentially more keys will match.
                /// Example: 'table (x Int32, y Int32) ORDER BY x + 1, y + 1'
                ///          'SELECT x, y FROM table WHERE x = 42 ORDER BY x + 1, y + 1'
                /// Here, 'x + 1' would be a fixed point. But it is reasonable to read-in-order.

                current_direction = descr.direction;
                if (match.monotonicity)
                {
                    current_direction *= match.monotonicity->direction;
                    strict_monotonic = match.monotonicity->strict;
                }
            }
            else if (fixed_key_columns.contains(sort_column_node))
            {
                // std::cerr << "+++++++++ Found fixed key by match" << std::endl;
            }
            else
            {

                // std::cerr << "====== Check for fixed const : " << bool(sort_node->column) << " fixed : " << fixed_columns.contains(sort_node) << std::endl;
                bool is_fixed_column = sort_node->column || fixed_columns.contains(sort_node);
                if (!is_fixed_column)
                    break;
            }
        }

        /// read_direction == 0 means we can choose any global direction.
        /// current_direction == 0 means current key if fixed and any direction is possible for it.
        if (current_direction && read_direction && current_direction != read_direction)
            break;

        if (read_direction == 0)
            read_direction = current_direction;

        if (current_direction)
        {
            order_key_prefix_descr.push_back(description[next_descr_column]);
            ++next_descr_column;

            if (!strict_monotonic)
                break;
        }
    }

    if (read_direction == 0 || order_key_prefix_descr.empty())
        return nullptr;

    return std::make_shared<InputOrderInfo>(order_key_prefix_descr, next_sort_key, read_direction, limit);
}

void optimizeReadInOrder(QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        return;

    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting)
        return;

    if (sorting->getType() != SortingStep::Type::Full)
        return;

    ReadFromMergeTree * reading = findReadingStep(node.children.front());
    if (!reading)
        return;

    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    if (sorting_key.column_names.empty())
        return;

    ActionsDAGPtr dag;
    FixedColumns fixed_columns;
    buildSortingDAG(node.children.front(), dag, fixed_columns);

    const auto & description = sorting->getSortDescription();
    auto limit = sorting->getLimit();
    const auto & sorting_key_columns = sorting_key.column_names;

    auto order_info = buildInputOrderInfo(
        fixed_columns,
        dag, description,
        sorting_key.expression->getActionsDAG(), sorting_key_columns,
        limit);

    if (!order_info)
        return;

    reading->requestReadingInOrder(order_info->used_prefix_of_sorting_key_size, order_info->direction, order_info->limit);
    sorting->convertToFinishSorting(order_info->sort_description_for_merging);
}

size_t tryReuseStorageOrderingForWindowFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/)
{
    /// Find the following sequence of steps, add InputOrderInfo and apply prefix sort description to
    /// SortingStep:
    /// WindowStep <- SortingStep <- [Expression] <- ReadFromMergeTree

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
    if (!context->getSettings().optimize_read_in_window_order || context->getSettingsRef().allow_experimental_analyzer)
    {
        return 0;
    }

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto * select_query = query_info.query->as<ASTSelectQuery>();

    /// TODO: Analyzer syntax analyzer result
    if (!query_info.syntax_analyzer_result)
        return 0;

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

    /// If we don't have filtration, we can pushdown limit to reading stage for optimizations.
    UInt64 limit = (select_query->hasFiltration() || select_query->groupBy()) ? 0 : InterpreterSelectQuery::getLimitForSorting(*select_query, context);

    auto order_info = order_optimizer->getInputOrder(
            query_info.projection ? query_info.projection->desc->metadata : read_from_merge_tree->getStorageMetadata(),
            context,
            limit);

    if (order_info)
    {
        read_from_merge_tree->requestReadingInOrder(order_info->used_prefix_of_sorting_key_size, order_info->direction, order_info->limit);
        sorting->convertToFinishSorting(order_info->sort_description_for_merging);
    }

    return 0;
}

}
