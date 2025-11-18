#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/defines.h>

namespace DB::QueryPlanOptimizations
{

using IndexToConditionMap = std::unordered_map<String, MergeTreeIndexConditionText *>;

String getNameWithoutAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children[0];

    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        String result_name = node->function_base->getName() + "(";
        for (size_t i = 0; i < node->children.size(); ++i)
        {
            if (i)
                result_name += ", ";

            result_name += getNameWithoutAliases(node->children[i]);
        }

        result_name += ")";
        return result_name;
    }

    return node->result_name;
}

String optimizationInfoToString(const IndexReadColumns & added_columns, const Names & removed_columns)
{
    chassert(!added_columns.empty());

    String result = "Added: [";

    /// This will list the index and the new associated columns
    size_t idx = 0;
    for (const auto & [_, columns_names_and_types] : added_columns)
    {
        for (const String & column_name : columns_names_and_types.getNames())
        {
            if (++idx > 1)
                result += ", ";
            result += column_name;
        }
    }
    result += "]";

    if (!removed_columns.empty())
    {
        result += ", Removed: [";
        for (size_t i = 0; i < removed_columns.size(); ++i)
        {
            if (i > 0)
                result += ", ";
            result += removed_columns[i];
        }
        result += "]";
    }
    return result;
}

/// This class substitutes filters with text-search functions by virtual columns which skip IO and read less data.
///
/// The substitution is performed after the index analysis and before PREWHERE optimization:
/// 1, We need the result of index analysis.
/// 2. We want to leverage the PREWHERE for virtual columns, because text index
///    is usually created with high granularity and PREWHERE with virtual columns
///    may significantly reduce the amount of data to read.
///
/// For example, for a query like:
///     SELECT count() FROM table WHERE hasToken(text_col, 'token')
/// if 1) text_col has an associated text index called text_col_idx, and 2) hasToken is an replaceable function,
/// then this class replaces some nodes in the ActionsDAG (and references to them) to generate an equivalent query:
///     SELECT count() FROM table where __text_index_text_col_idx_hasToken_0
///
/// The supported functions can be found in MergeTreeIndexConditionText::isSupportedFunctionForDirectRead.
///
/// This class is a (C++) friend of ActionsDAG and can therefore access its private members.
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach
/// simplifies the work by avoiding conflicts and minimizing coupling between this optimization and ActionsDAG.
class FullTextMatchingFunctionDAGReplacer
{
public:
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const IndexToConditionMap & index_conditions_)
        : actions_dag(actions_dag_)
        , index_conditions(index_conditions_)
    {
    }

    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0
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
        using enum ActionsDAG::ActionType;
        const auto & header = condition.getHeader();

        if ((lhs_arg.type == INPUT || lhs_arg.type == FUNCTION) && rhs_arg.type == COLUMN)
        {
            auto lhs_name_without_aliases = getNameWithoutAliases(&lhs_arg);
            return header.has(lhs_name_without_aliases);
        }

        if (lhs_arg.type == COLUMN && (rhs_arg.type == INPUT || rhs_arg.type == FUNCTION))
        {
            auto rhs_name_without_aliases = getNameWithoutAliases(&rhs_arg);
            return header.has(rhs_name_without_aliases);
        }

        return false;
    }

    /// Attempts to add a new node with the replacement virtual column.
    /// Returns the pair of (index name, virtual column name) if the replacement is successful.
    std::optional<std::pair<String, String>> tryReplaceFunctionNodeInplace(ActionsDAG::Node & function_node)
    {
        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function)
            return std::nullopt;

        if (function_node.children.size() != 2)
            return std::nullopt;

        if (!MergeTreeIndexConditionText::isSupportedFunctionForDirectRead(function_node.function->getName()))
            return std::nullopt;

        auto selected_it = std::ranges::find_if(index_conditions, [&](const auto & index_with_condition)
        {
            return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second);
        });

        if (selected_it == index_conditions.end())
            return std::nullopt;

        size_t num_supported_conditions = std::ranges::count_if(index_conditions, [&](const auto & index_with_condition)
        {
            return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second);
        });

        /// Do not optimize if there are multiple text indexes set for the column.
        /// It is not clear which index to use.
        if (num_supported_conditions != 1)
            return std::nullopt;

        const auto & [index_name, condition] = *selected_it;
        auto search_query = condition->createTextSearchQuery(function_node);

        if (!search_query)
            return std::nullopt;

        auto virtual_column_name = condition->replaceToVirtualColumn(*search_query, index_name);
        if (!virtual_column_name)
            return std::nullopt;

        function_node.type = ActionsDAG::ActionType::INPUT;
        function_node.result_type = std::make_shared<DataTypeUInt8>();
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
/// - text_function is a text-matching functions, e.g. 'hasToken'
/// - text-matching functions expect that the column on which the function is called has a text index
///
/// This function replaces text function nodes from the user query (using semi-brute-force process) with internal virtual columns
/// which use only the index information to bypass the normal column scan which can consume significant amount of the execution time.
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

    ReadFromMergeTree * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

    IndexToConditionMap index_conditions;
    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (auto * text_index_condition = typeid_cast<MergeTreeIndexConditionText *>(index.condition.get()))
        {
            /// Index may be not materialized in some parts, e.g. after ALTER ADD INDEX query.
            /// TODO: support partial read from text index with fallback to the brute-force
            /// search for parts where index is not materialized.
            bool has_index_in_all_parts = std::ranges::all_of(unique_parts, [&](const auto & part)
            {
                return !!index.index->getDeserializedFormat(part->checksums, index.index->getFileName());
            });

            if (has_index_in_all_parts)
                index_conditions[index.index->index.name] = text_index_condition;
        }
    }

    if (index_conditions.empty())
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    FilterStep * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    ActionsDAG & filter_dag = filter_step->getExpression();
    const ActionsDAG::Node & filter_column_node = filter_dag.findInOutputs(filter_step->getFilterColumnName());

    FullTextMatchingFunctionDAGReplacer replacer(filter_dag, index_conditions);
    auto [added_columns, removed_columns] = replacer.replace();

    if (added_columns.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(added_columns, removed_columns));

    read_from_merge_tree_step->createReadTasksForTextIndex(indexes->skip_indexes, added_columns, removed_columns);

    bool removes_filter_column = filter_step->removesFilterColumn();

    /// The original node (pointer address) should be preserved in the DAG outputs because we replace in-place.
    /// However, the `result_name` could be different if the output column was replaced.
    chassert(std::ranges::contains(filter_dag.getOutputs(), &filter_column_node));

    const String new_filter_column_name = filter_column_node.result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
}

}
