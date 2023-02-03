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
#include <Storages/StorageMerge.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/IColumn.h>
#include <stack>


namespace DB::QueryPlanOptimizations
{

ISourceStep * checkSupportedReadingStep(IQueryPlanStep * step)
{
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// Already read-in-order, skip.
        if (reading->getQueryInfo().input_order_info)
            return nullptr;

        const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
        if (sorting_key.column_names.empty())
            return nullptr;

        return reading;
    }

    if (auto * merge = typeid_cast<ReadFromMerge *>(step))
    {
        const auto & tables = merge->getSelectedTables();
        if (tables.empty())
            return nullptr;

        for (const auto & table : tables)
        {
            auto storage = std::get<StoragePtr>(table);
            const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
            if (sorting_key.column_names.empty())
                return nullptr;
        }

        return merge;
    }

    return nullptr;
}

QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = checkSupportedReadingStep(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(*node.children.front());

    if (auto * distinct = typeid_cast<DistinctStep *>(step); distinct && distinct->isPreliminary())
        return findReadingStep(*node.children.front());

    return nullptr;
}

/// FixedColumns are columns which values become constants after filtering.
/// In a query "SELECT x, y, z FROM table WHERE x = 1 AND y = 'a' ORDER BY x, y, z"
/// Fixed columns are 'x' and 'y'.
using FixedColumns = std::unordered_set<const ActionsDAG::Node *>;

/// Right now we find only simple cases like 'and(..., and(..., and(column = value, ...), ...'
/// Injective functions are supported here. For a condition 'injectiveFunction(x) = 5' column 'x' is fixed.
void appendFixedColumnsFromFilterExpression(const ActionsDAG::Node & filter_expression, FixedColumns & fixed_columns)
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
                size_t num_constant_columns = 0;
                for (const auto & child : node->children)
                {
                    if (child->column)
                        ++num_constant_columns;
                    else
                        maybe_fixed_column = child;
                }

                if (maybe_fixed_column && num_constant_columns + 1 == node->children.size())
                {
                    //std::cerr << "====== Added fixed column " << maybe_fixed_column->result_name << ' ' << static_cast<const void *>(maybe_fixed_column) << std::endl;
                    fixed_columns.insert(maybe_fixed_column);

                    /// Support injective functions chain.
                    const ActionsDAG::Node * maybe_injective = maybe_fixed_column;
                    while (maybe_injective->type == ActionsDAG::ActionType::FUNCTION
                        && maybe_injective->children.size() == 1
                        && maybe_injective->function_base->isInjective({}))
                    {
                        maybe_injective = maybe_injective->children.front();
                        fixed_columns.insert(maybe_injective);
                    }
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

/// This function builds a common DAG which is a gerge of DAGs from Filter and Expression steps chain.
/// Additionally, build a set of fixed columns.
void buildSortingDAG(QueryPlan::Node & node, ActionsDAGPtr & dag, FixedColumns & fixed_columns, size_t & limit)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto * prewhere_info = reading->getPrewhereInfo())
        {
            /// Should ignore limit if there is filtering.
            limit = 0;

            if (prewhere_info->prewhere_actions)
            {
                //std::cerr << "====== Adding prewhere " << std::endl;
                appendExpression(dag, prewhere_info->prewhere_actions);
                if (const auto * filter_expression = dag->tryFindInOutputs(prewhere_info->prewhere_column_name))
                    appendFixedColumnsFromFilterExpression(*filter_expression, fixed_columns);
            }
        }
        return;
    }

    if (node.children.size() != 1)
        return;

    buildSortingDAG(*node.children.front(), dag, fixed_columns, limit);

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();

        /// Should ignore limit because arrayJoin() can reduce the number of rows in case of empty array.
        if (actions->hasArrayJoin())
            limit = 0;

        appendExpression(dag, actions);
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        /// Should ignore limit if there is filtering.
        limit = 0;

        appendExpression(dag, filter->getExpression());
        if (const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName()))
            appendFixedColumnsFromFilterExpression(*filter_expression, fixed_columns);
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        /// Should ignore limit because ARRAY JOIN can reduce the number of rows in case of empty array.
        /// But in case of LEFT ARRAY JOIN the result number of rows is always bigger.
        if (!array_join->arrayJoin()->is_left)
            limit = 0;

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

/// Add more functions to fixed columns.
/// Functions result is fixed if all arguments are fixed or constants.
void enreachFixedColumns(const ActionsDAG & dag, FixedColumns & fixed_columns)
{
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited;
    for (const auto & node : dag.getNodes())
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
                            //std::cerr << "*** enreachFixedColumns check " << frame.node->result_name << std::endl;
                            bool all_args_fixed_or_const = true;
                            for (const auto * child : frame.node->children)
                            {
                                if (!child->column && !fixed_columns.contains(child))
                                {
                                    //std::cerr << "*** enreachFixedColumns fail " << child->result_name <<  ' ' << static_cast<const void *>(child) << std::endl;
                                    all_args_fixed_or_const = false;
                                }
                            }

                            if (all_args_fixed_or_const)
                            {
                                //std::cerr << "*** enreachFixedColumns add " << frame.node->result_name << ' ' << static_cast<const void *>(frame.node) << std::endl;
                                fixed_columns.insert(frame.node);
                            }
                        }
                    }
                }

                stack.pop();
            }
        }
    }
}

/// This structure stores a node mapping from one DAG to another.
/// The rule is following:
/// * Input nodes are mapped by name.
/// * Function is mapped to function if all children are mapped and function names are same.
/// * Alias is mapped to it's children mapping.
/// * Monotonic function can be mapped to it's children mapping if direct mapping does not exist.
///   In this case, information about monotonicity is filled.
/// * Mapped node is nullptr if there is no mapping found.
///
/// Overall, directly mapped nodes represent equal calculations.
/// Notes:
/// * Mapped DAG can contain many nodes which represent the same calculation.
///   In this case mapping is ambiguous and only one node is mapped.
/// * Aliases for mapped DAG are not supported.
/// DAG for PK does not contain aliases and ambiguous nodes.
struct MatchedTrees
{
    /// Monotonicity is calculated for monotonic functions chain.
    /// Chain is not strict if there is any non-strict monotonic function.
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
    std::stack<Frame> stack;

    for (const auto & node : outer_dag.getNodes())
    {
        if (matches.contains(&node))
            continue;

        stack.push(Frame{&node, {}});
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
                    /// If match map does not contain a child, it was not visited.
                    stack.push(Frame{child, {}});
                    break;
                }
                /// A node from found match may be nullptr.
                /// It means that node is visited, but no match was found.
                frame.mapped_children.push_back(it->second.node);
            }

            if (frame.mapped_children.size() < frame.node->children.size())
                continue;

            /// Create an empty match for current node.
            /// natch.node will be set if match is found.
            auto & match = matches[frame.node];

            if (frame.node->type == ActionsDAG::ActionType::INPUT)
            {
                const ActionsDAG::Node * mapped = nullptr;
                if (auto it = inner_inputs.find(frame.node->result_name); it != inner_inputs.end())
                    mapped = it->second;

                match.node = mapped;
            }
            else if (frame.node->type == ActionsDAG::ActionType::ALIAS)
            {
                match = matches[frame.node->children.at(0)];
            }
            else if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
            {

                //std::cerr << "... Processing " << frame.node->function_base->getName() << std::endl;

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
                        size_t mapped_children_size = frame.mapped_children.size();
                        other_parents.reserve(mapped_children_size);
                        for (size_t i = 1; i < mapped_children_size; ++i)
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

                    //std::cerr << ".. Candidate parents " << intersection->size() << std::endl;

                    if (!intersection->empty())
                    {
                        auto func_name = frame.node->function_base->getName();
                        for (const auto * parent : *intersection)
                        {
                            //std::cerr << ".. candidate " << parent->result_name << std::endl;
                            if (parent->type == ActionsDAG::ActionType::FUNCTION && func_name == parent->function_base->getName())
                            {
                                const auto & children = parent->children;
                                size_t num_children = children.size();
                                if (frame.mapped_children.size() == num_children)
                                {
                                    bool all_children_matched = true;
                                    for (size_t i = 0; all_children_matched && i < num_children; ++i)
                                        all_children_matched = frame.mapped_children[i] == children[i];

                                    if (all_children_matched)
                                    {
                                        match.node = parent;
                                        break;
                                    }
                                }
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
                            if (info.is_monotonic)
                            {
                                MatchedTrees::Monotonicity monotonicity;
                                monotonicity.direction *= info.is_positive ? 1 : -1;
                                monotonicity.strict = info.is_strict;

                                if (child_match.monotonicity)
                                {
                                    monotonicity.direction *= child_match.monotonicity->direction;
                                    if (!child_match.monotonicity->strict)
                                        monotonicity.strict = false;
                                }

                                match.node = child_match.node;
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
    //std::cerr << "------- buildInputOrderInfo " << std::endl;
    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(description.size());

    MatchedTrees::Matches matches;
    FixedColumns fixed_key_columns;

    if (dag)
    {
        matches = matchTrees(sorting_key_dag, *dag);

        for (const auto & [node, match] : matches)
        {
            //std::cerr << "------- matching " << static_cast<const void *>(node) << " " << node->result_name
            //    << " to " << static_cast<const void *>(match.node) << " " << (match.node ? match.node->result_name : "") << std::endl;
            if (!match.monotonicity || match.monotonicity->strict)
            {
                if (match.node && fixed_columns.contains(node))
                    fixed_key_columns.insert(match.node);
            }
        }

        enreachFixedColumns(sorting_key_dag, fixed_key_columns);
    }

    /// This is a result direction we will read from MergeTree
    ///  1 - in order,
    /// -1 - in reverse order,
    ///  0 - usual read, don't apply optimization
    ///
    /// So far, 0 means any direction is possible. It is ok for constant prefix.
    int read_direction = 0;
    size_t next_description_column = 0;
    size_t next_sort_key = 0;

    while (next_description_column < description.size() && next_sort_key < sorting_key_columns.size())
    {
        const auto & sorting_key_column = sorting_key_columns[next_sort_key];
        const auto & sort_column_description = description[next_description_column];

        /// If required order depend on collation, it cannot be matched with primary key order.
        /// Because primary keys cannot have collations.
        if (sort_column_description.collator)
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
            /// This is possible if there were no Expression or Filter steps in Plan.
            /// Example: SELECT * FROM tab ORDER BY a, b

            if (sort_column_node->type != ActionsDAG::ActionType::INPUT)
                break;

            if (sort_column_description.column_name != sorting_key_column)
                break;

            current_direction = sort_column_description.direction;


            //std::cerr << "====== (no dag) Found direct match" << std::endl;

            ++next_description_column;
            ++next_sort_key;
        }
        else
        {
            const ActionsDAG::Node * sort_node = dag->tryFindInOutputs(sort_column_description.column_name);
             /// It is possible when e.g. sort by array joined column.
            if (!sort_node)
                break;

            const auto & match = matches[sort_node];

            //std::cerr << "====== Finding match for " << sort_column_node->result_name << ' ' << static_cast<const void *>(sort_column_node) << std::endl;

            if (match.node && match.node == sort_column_node)
            {
                //std::cerr << "====== Found direct match" << std::endl;

                /// We try to find the match first even if column is fixed. In this case, potentially more keys will match.
                /// Example: 'table (x Int32, y Int32) ORDER BY x + 1, y + 1'
                ///          'SELECT x, y FROM table WHERE x = 42 ORDER BY x + 1, y + 1'
                /// Here, 'x + 1' would be a fixed point. But it is reasonable to read-in-order.

                current_direction = sort_column_description.direction;
                if (match.monotonicity)
                {
                    current_direction *= match.monotonicity->direction;
                    strict_monotonic = match.monotonicity->strict;
                }

                ++next_description_column;
                ++next_sort_key;
            }
            else if (fixed_key_columns.contains(sort_column_node))
            {
                //std::cerr << "+++++++++ Found fixed key by match" << std::endl;
                ++next_sort_key;
            }
            else
            {

                //std::cerr << "====== Check for fixed const : " << bool(sort_node->column) << " fixed : " << fixed_columns.contains(sort_node) << std::endl;
                bool is_fixed_column = sort_node->column || fixed_columns.contains(sort_node);
                if (!is_fixed_column)
                    break;

                order_key_prefix_descr.push_back(sort_column_description);
                ++next_description_column;
            }
        }

        /// read_direction == 0 means we can choose any global direction.
        /// current_direction == 0 means current key if fixed and any direction is possible for it.
        if (current_direction && read_direction && current_direction != read_direction)
            break;

        if (read_direction == 0)
            read_direction = current_direction;

        if (current_direction)
            order_key_prefix_descr.push_back(sort_column_description);

        if (current_direction && !strict_monotonic)
            break;
    }

    if (read_direction == 0 || order_key_prefix_descr.empty())
        return nullptr;

    return std::make_shared<InputOrderInfo>(order_key_prefix_descr, next_sort_key, read_direction, limit);
}

/// We really need three different sort descriptions here.
/// For example:
///
///   create table tab (a Int32, b Int32, c Int32, d Int32) engine = MergeTree order by (a, b, c);
///   select a, any(b), c, d from tab where b = 1 group by a, c, d order by c, d;
///
/// We would like to have:
/// (a, b, c) - a sort description for reading from table (it's into input_order)
/// (a, c) - a sort description for merging (an input of AggregatingInOrderTransfrom is sorted by this GROUP BY keys)
/// (a, c, d) - a group by soer description (an input of FinishAggregatingInOrderTransform is sorted by all GROUP BY keys)
///
/// Sort description from input_order is not actually used. ReadFromMergeTree reads only PK prefix size.
/// We should remove it later.
struct AggregationInputOrder
{
    InputOrderInfoPtr input_order;
    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;
};

AggregationInputOrder buildInputOrderInfo(
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & group_by_keys,
    const ActionsDAG & sorting_key_dag,
    const Names & sorting_key_columns)
{
    MatchedTrees::Matches matches;
    FixedColumns fixed_key_columns;

    /// For every column in PK find any match from GROUP BY key.
    using ReverseMatches = std::unordered_map<const ActionsDAG::Node *, MatchedTrees::Matches::const_iterator>;
    ReverseMatches reverse_matches;

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

        enreachFixedColumns(sorting_key_dag, fixed_key_columns);

        for (auto it = matches.cbegin(); it != matches.cend(); ++it)
        {
            const MatchedTrees::Match * match = &it->second;
            if (match->node)
            {
                auto [jt, inserted] = reverse_matches.emplace(match->node, it);
                if (!inserted)
                {
                    /// Find the best match for PK node.
                    /// Direct match > strict monotonic > monotonic.
                    const MatchedTrees::Match * prev_match = &jt->second->second;
                    bool is_better = prev_match->monotonicity && !match->monotonicity;
                    if (!is_better)
                    {
                        bool both_monotionic = prev_match->monotonicity && match->monotonicity;
                        is_better = both_monotionic && match->monotonicity->strict && !prev_match->monotonicity->strict;
                    }

                    if (is_better)
                        jt->second = it;
                }
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
    size_t next_sort_key = 0;
    std::unordered_set<std::string_view> not_matched_group_by_keys(group_by_keys.begin(), group_by_keys.end());

    SortDescription group_by_sort_description;
    group_by_sort_description.reserve(group_by_keys.size());

    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(sorting_key_columns.size());

    while (!not_matched_group_by_keys.empty() && next_sort_key < sorting_key_columns.size())
    {
        const auto & sorting_key_column = sorting_key_columns[next_sort_key];

        /// Direction for current sort key.
        int current_direction = 0;
        bool strict_monotonic = true;
        std::unordered_set<std::string_view>::iterator group_by_key_it;

        const ActionsDAG::Node * sort_column_node = sorting_key_dag.tryFindInOutputs(sorting_key_column);
        /// This should not happen.
        if (!sort_column_node)
            break;

        if (!dag)
        {
            /// This is possible if there were no Expression or Filter steps in Plan.
            /// Example: SELECT * FROM tab ORDER BY a, b

            if (sort_column_node->type != ActionsDAG::ActionType::INPUT)
                break;

            group_by_key_it = not_matched_group_by_keys.find(sorting_key_column);
            if (group_by_key_it == not_matched_group_by_keys.end())
                break;

            current_direction = 1;

            //std::cerr << "====== (no dag) Found direct match" << std::endl;
            ++next_sort_key;
        }
        else
        {
            const MatchedTrees::Match * match = nullptr;
            const ActionsDAG::Node * group_by_key_node = nullptr;
            if (const auto match_it = reverse_matches.find(sort_column_node); match_it != reverse_matches.end())
            {
                group_by_key_node = match_it->second->first;
                match = &match_it->second->second;
            }

            //std::cerr << "====== Finding match for " << sort_column_node->result_name << ' ' << static_cast<const void *>(sort_column_node) << std::endl;

            if (match && match->node)
                group_by_key_it = not_matched_group_by_keys.find(group_by_key_node->result_name);

            if (match && match->node && group_by_key_it != not_matched_group_by_keys.end())
            {
                //std::cerr << "====== Found direct match" << std::endl;

                current_direction = 1;
                if (match->monotonicity)
                {
                    current_direction *= match->monotonicity->direction;
                    strict_monotonic = match->monotonicity->strict;
                }

                ++next_sort_key;
            }
            else if (fixed_key_columns.contains(sort_column_node))
            {
                //std::cerr << "+++++++++ Found fixed key by match" << std::endl;
                ++next_sort_key;
            }
            else
                break;
        }

        /// read_direction == 0 means we can choose any global direction.
        /// current_direction == 0 means current key if fixed and any direction is possible for it.
        if (current_direction && read_direction && current_direction != read_direction)
            break;

        if (read_direction == 0 && current_direction != 0)
            read_direction = current_direction;

        if (current_direction)
        {
            /// Aggregation in order will always read in table order.
            /// Here, current_direction is a direction which will be applied to every key.
            /// Example:
            ///   CREATE TABLE t (x, y, z) ENGINE = MergeTree ORDER BY (x, y)
            ///   SELECT ... FROM t GROUP BY negate(y), negate(x), z
            /// Here, current_direction will be -1 cause negate() is negative montonic,
            /// Prefix sort description for reading will be (negate(y) DESC, negate(x) DESC),
            /// Sort description for GROUP BY will be (negate(y) DESC, negate(x) DESC, z).
            //std::cerr << "---- adding " << std::string(*group_by_key_it) << std::endl;
            group_by_sort_description.emplace_back(SortColumnDescription(std::string(*group_by_key_it), current_direction));
            order_key_prefix_descr.emplace_back(SortColumnDescription(std::string(*group_by_key_it), current_direction));
            not_matched_group_by_keys.erase(group_by_key_it);
        }
        else
        {
            /// If column is fixed, will read it in table order as well.
            //std::cerr << "---- adding " << sorting_key_column << std::endl;
            order_key_prefix_descr.emplace_back(SortColumnDescription(sorting_key_column, 1));
        }

        if (current_direction && !strict_monotonic)
            break;
    }

    if (read_direction == 0 || group_by_sort_description.empty())
        return {};

    SortDescription sort_description_for_merging = group_by_sort_description;

    for (const auto & key : not_matched_group_by_keys)
        group_by_sort_description.emplace_back(SortColumnDescription(std::string(key)));

    auto input_order = std::make_shared<InputOrderInfo>(order_key_prefix_descr, next_sort_key, /*read_direction*/ 1, /* limit */ 0);
    return { std::move(input_order), std::move(sort_description_for_merging), std::move(group_by_sort_description) };
}

InputOrderInfoPtr buildInputOrderInfo(
    ReadFromMergeTree * reading,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    size_t limit)
{
    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    const auto & sorting_key_columns = sorting_key.column_names;

    return buildInputOrderInfo(
        fixed_columns,
        dag, description,
        sorting_key.expression->getActionsDAG(), sorting_key_columns,
        limit);
}

InputOrderInfoPtr buildInputOrderInfo(
    ReadFromMerge * merge,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    size_t limit)
{
    const auto & tables = merge->getSelectedTables();

    InputOrderInfoPtr order_info;
    for (const auto & table : tables)
    {
        auto storage = std::get<StoragePtr>(table);
        const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
        const auto & sorting_key_columns = sorting_key.column_names;

        if (sorting_key_columns.empty())
            return nullptr;

        auto table_order_info = buildInputOrderInfo(
            fixed_columns,
            dag, description,
            sorting_key.expression->getActionsDAG(), sorting_key_columns,
            limit);

        if (!table_order_info)
            return nullptr;

        if (!order_info)
            order_info = table_order_info;
        else if (*order_info != *table_order_info)
            return nullptr;
    }

    return order_info;
}

AggregationInputOrder buildInputOrderInfo(
    ReadFromMergeTree * reading,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & group_by_keys)
{
    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    const auto & sorting_key_columns = sorting_key.column_names;

    return buildInputOrderInfo(
        fixed_columns,
        dag, group_by_keys,
        sorting_key.expression->getActionsDAG(), sorting_key_columns);
}

AggregationInputOrder buildInputOrderInfo(
    ReadFromMerge * merge,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & group_by_keys)
{
    const auto & tables = merge->getSelectedTables();

    AggregationInputOrder order_info;
    for (const auto & table : tables)
    {
        auto storage = std::get<StoragePtr>(table);
        const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
        const auto & sorting_key_columns = sorting_key.column_names;

        if (sorting_key_columns.empty())
            return {};

        auto table_order_info = buildInputOrderInfo(
            fixed_columns,
            dag, group_by_keys,
            sorting_key.expression->getActionsDAG(), sorting_key_columns);

        if (!table_order_info.input_order)
            return {};

        if (!order_info.input_order)
            order_info = table_order_info;
        else if (*order_info.input_order != *table_order_info.input_order)
            return {};
    }

    return order_info;
}

InputOrderInfoPtr buildInputOrderInfo(SortingStep & sorting, QueryPlan::Node & node)
{
    QueryPlan::Node * reading_node = findReadingStep(node);
    if (!reading_node)
        return nullptr;

    const auto & description = sorting.getSortDescription();
    size_t limit = sorting.getLimit();

    ActionsDAGPtr dag;
    FixedColumns fixed_columns;
    buildSortingDAG(node, dag, fixed_columns, limit);

    if (dag && !fixed_columns.empty())
        enreachFixedColumns(*dag, fixed_columns);

    if (auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            reading,
            fixed_columns,
            dag, description,
            limit);

        if (order_info)
            reading->requestReadingInOrder(order_info->used_prefix_of_sorting_key_size, order_info->direction, order_info->limit);

        return order_info;
    }
    else if (auto * merge = typeid_cast<ReadFromMerge *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            merge,
            fixed_columns,
            dag, description,
            limit);

        if (order_info)
            merge->requestReadingInOrder(order_info);

        return order_info;
    }

    return nullptr;
}

AggregationInputOrder buildInputOrderInfo(AggregatingStep & aggregating, QueryPlan::Node & node)
{
    QueryPlan::Node * reading_node = findReadingStep(node);
    if (!reading_node)
        return {};

    const auto & keys = aggregating.getParams().keys;
    size_t limit = 0;

    ActionsDAGPtr dag;
    FixedColumns fixed_columns;
    buildSortingDAG(node, dag, fixed_columns, limit);

    if (dag && !fixed_columns.empty())
        enreachFixedColumns(*dag, fixed_columns);

    if (auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            reading,
            fixed_columns,
            dag, keys);

        if (order_info.input_order)
            reading->requestReadingInOrder(
                order_info.input_order->used_prefix_of_sorting_key_size,
                order_info.input_order->direction,
                order_info.input_order->limit);

        return order_info;
    }
    else if (auto * merge = typeid_cast<ReadFromMerge *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            merge,
            fixed_columns,
            dag, keys);

        if (order_info.input_order)
            merge->requestReadingInOrder(order_info.input_order);

        return order_info;
    }

    return {};
}

void optimizeReadInOrder(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 1)
        return;

    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting)
        return;

    //std::cerr << "---- optimizeReadInOrder found sorting" << std::endl;

    if (sorting->getType() != SortingStep::Type::Full)
        return;

    if (typeid_cast<UnionStep *>(node.children.front()->step.get()))
    {
        auto & union_node = node.children.front();

        std::vector<InputOrderInfoPtr> infos;
        const SortDescription * max_sort_descr = nullptr;
        infos.reserve(node.children.size());
        for (auto * child : union_node->children)
        {
            infos.push_back(buildInputOrderInfo(*sorting, *child));

            if (infos.back() && (!max_sort_descr || max_sort_descr->size() < infos.back()->sort_description_for_merging.size()))
                max_sort_descr = &infos.back()->sort_description_for_merging;
        }

        if (!max_sort_descr || max_sort_descr->empty())
            return;

        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];
            auto & child = union_node->children[i];

            QueryPlanStepPtr additional_sorting;

            if (!info)
            {
                auto limit = sorting->getLimit();
                /// If we have limit, it's better to sort up to full description and apply limit.
                /// We cannot sort up to partial read-in-order description with limit cause result set can be wrong.
                const auto & descr = limit ? sorting->getSortDescription() : *max_sort_descr;
                additional_sorting = std::make_unique<SortingStep>(
                    child->step->getOutputStream(),
                    descr,
                    limit, /// TODO: support limit with ties
                    sorting->getSettings(),
                    false);
            }
            else if (info->sort_description_for_merging.size() < max_sort_descr->size())
            {
                additional_sorting = std::make_unique<SortingStep>(
                    child->step->getOutputStream(),
                    info->sort_description_for_merging,
                    *max_sort_descr,
                    sorting->getSettings().max_block_size,
                    0); /// TODO: support limit with ties
            }

            if (additional_sorting)
            {
                auto & sort_node = nodes.emplace_back();
                sort_node.step = std::move(additional_sorting);
                sort_node.children.push_back(child);
                child = &sort_node;
            }
        }

        sorting->convertToFinishSorting(*max_sort_descr);
    }
    else if (auto order_info = buildInputOrderInfo(*sorting, *node.children.front()))
    {
        sorting->convertToFinishSorting(order_info->sort_description_for_merging);
    }
}

void optimizeAggregationInOrder(QueryPlan::Node & node, QueryPlan::Nodes &)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if (aggregating->inOrder() || aggregating->isGroupingSets())
        return;

    /// TODO: maybe add support for UNION later.
    if (auto order_info = buildInputOrderInfo(*aggregating, *node.children.front()); order_info.input_order)
    {
        aggregating->applyOrder(std::move(order_info.sort_description_for_merging), std::move(order_info.group_by_sort_description));
    }
}

/// This optimisation is obsolete and will be removed.
/// optimizeReadInOrder covers it.
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
    const auto & settings = context->getSettings();
    if (!settings.optimize_read_in_window_order || (settings.optimize_read_in_order && settings.query_plan_read_in_order) || context->getSettingsRef().allow_experimental_analyzer)
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
