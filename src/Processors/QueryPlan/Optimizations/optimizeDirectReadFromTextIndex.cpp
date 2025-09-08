#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Common/logger_useful.h>
#include <base/defines.h>

namespace DB::QueryPlanOptimizations
{

using IndexToConditionMap = std::unordered_map<String, MergeTreeIndexConditionText *>;

/// This class substitutes filters with text-search functions by virtual columns which skip IO and read less data.
///
/// The substitution is performed after the index analysis and before PREWHERE optimization:
/// 1, We need the result of index analysis.
/// 2. We want to leverage the PREWHERE for virtual columns, because text index
///    is usually created with high granularit and PREHERE with virtual columns
///    may significantly reduce the amount of data to read.
///
/// For example, for a query like
///     SELECT count() FROM table WHERE hasToken(text_col, 'token')
/// if 1) text_col has an associated text index called text_col_idx, and 2) hasToken is an replaceable function (according to
/// isReplaceableFunction), then this class replaces some nodes in the ActionsDAG (and references to them) to generate an
/// equivalent query
///     SELECT count() FROM table where __text_index_text_col_idx_hasToken_0
///
/// The supported functions can be found in MergeTreeIndexConditionText::isSupportedFunctionForDirectRead.
///
/// This class is a (C++) friend of ActionsDAG and can therefore access its private members.
///
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach simplifies the work by avoiding
/// conflicts and minimizing coupling between this optimization and ActionsDAG.
class FullTextMatchingFunctionDAGReplacer
{
public:
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const IndexToConditionMap & index_conditions_)
        : actions_dag(actions_dag_)
        , index_conditions(index_conditions_)
    {
    }

    /// This optimization replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0
    ///
    /// Returns a pair of (added columns by index name, removed columns)
    std::pair<IndexReadColumns, Names> replace()
    {
        IndexReadColumns added_columns;
        Names original_inputs = actions_dag.getRequiredColumnsNames();

        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            auto replaced = tryReplaceFunctionNodeInplace(node);

            if (replaced.has_value())
            {
                const auto & [index_name, column_name] = replaced.value();
                added_columns[index_name].emplace_back(column_name, node.result_type);
            }
        }

        if (added_columns.empty())
            return {{}, {}};

        actions_dag.removeUnusedActions();

        Names removed_columns;
        Names replaced_columns = actions_dag.getRequiredColumnsNames();
        NameSet replaced_columns_set(replaced_columns.begin(), replaced_columns.end());

        for (const auto & column : original_inputs)
        {
            if (!replaced_columns_set.contains(column))
                removed_columns.push_back(column);
        }

        return std::make_pair(added_columns, removed_columns);
    }

private:
    ActionsDAG & actions_dag;
    std::unordered_map<String, MergeTreeIndexConditionText *> index_conditions;

    bool isSupportedCondition(const ActionsDAG::Node & lhs_arg, const ActionsDAG::Node & rhs_arg, const MergeTreeIndexConditionText & condition) const
    {
        const auto & header = condition.getHeader();

        if (lhs_arg.type == ActionsDAG::ActionType::INPUT && rhs_arg.type == ActionsDAG::ActionType::COLUMN)
            return header.has(lhs_arg.result_name);

        if (lhs_arg.type == ActionsDAG::ActionType::COLUMN && rhs_arg.type == ActionsDAG::ActionType::INPUT)
            return header.has(rhs_arg.result_name);

        return false;
    }

    /// Attempt to add a new node with the replacement function.
    /// This also adds extra input columns if needed.
    /// Returns the pair of (index name, virtual column name) if the replacement is successful.
    std::optional<std::pair<String, String>> tryReplaceFunctionNodeInplace(ActionsDAG::Node & function_node)
    {
        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function)
            return std::nullopt;

        if (function_node.children.size() != 2)
            return std::nullopt;

        if (!MergeTreeIndexConditionText::isSupportedFunctionForDirectRead(function_node.function->getName()))
            return std::nullopt;

        IndexToConditionMap::iterator selected_condition_it = index_conditions.end();

        for (auto it = index_conditions.begin(); it != index_conditions.end(); ++it)
        {
            if (isSupportedCondition(*function_node.children[0], *function_node.children[1], *it->second))
            {
                if (selected_condition_it != index_conditions.end())
                    return std::nullopt;

                selected_condition_it = it;
            }
        }

        if (selected_condition_it == index_conditions.end())
            return std::nullopt;

        const auto & [index_name, condition] = *selected_condition_it;
        auto search_query = condition->createSearchQuery(function_node);

        if (!search_query)
            return std::nullopt;

        auto virtual_column_name = condition->replaceToVirtualColumn(*search_query, index_name);
        if (!virtual_column_name)
            return std::nullopt;

        function_node.type = ActionsDAG::ActionType::INPUT;
        function_node.result_name = virtual_column_name.value();
        function_node.function.reset();
        function_node.function_base.reset();
        function_node.children.clear();
        actions_dag.inputs.push_back(&function_node);

        return std::make_pair(index_name, virtual_column_name.value());
    }
};

/// Text index search queries have this form:
///     SELECT [...]
///     FROM tab
///     WHERE text_function(...), [...]
/// where
/// - text_function is a text-matching functions, e.g. 'hasToken',
///   text-matching functions expect that the column on which the function is called has a text index
///
/// This function replaces text function nodes from the user query (using semi-brute-force process) with internal virtual columns which use only
/// the index information to bypass the normal column scan which can consume significant amount of the execution time.
/// The optimization checks if the function's column node is a text column with a text index.
void optimizeDirectReadFromTextIndex(const Stack & stack, QueryPlan::Nodes & /*nodes*/)
{
    if (stack.size() < 2)
        return;

    const auto & frame = stack.back();

    /// Expect this query plan:
    /// FilterStep
    ///    ^
    ///    |
    /// ReadFromMergeTree

    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    IndexToConditionMap index_conditions;
    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (auto * text_index_condition = typeid_cast<MergeTreeIndexConditionText *>(index.condition.get()))
            index_conditions[index.index->index.name] = text_index_condition;
    }

    if (index_conditions.empty())
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    /// Now try to modify the ActionsDAG.
    auto & filter_dag = filter_step->getExpression();
    FullTextMatchingFunctionDAGReplacer replacer(filter_dag, index_conditions);
    auto [added_columns, removed_columns] = replacer.replace();

    if (added_columns.empty())
        return;

    read_from_merge_tree_step->replaceColumnsForTextSearch(added_columns, removed_columns);

    bool removes_filter_column = filter_step->removesFilterColumn();
    auto new_filter_column_name = filter_dag.getOutputs().front()->result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_step->getExpression().clone(), new_filter_column_name, removes_filter_column);
}

}
