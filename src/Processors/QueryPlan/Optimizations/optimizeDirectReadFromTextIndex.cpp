#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <base/defines.h>

#include <algorithm>
#include <cstdio>
#include <memory>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct IndexInfo : public IndexSize
{
    String name;

    IndexInfo(String name_, IndexSize index_size)
        : IndexSize(index_size)
        , name(name_)
    {
    }
};

std::unordered_map<String, IndexInfo> getIndexInfosForColumns(const ReadFromMergeTree & read_from_mergetree_step)
{
    std::unordered_map<String, IndexInfo> columns_to_index_infos;

    auto metadata = read_from_mergetree_step.getStorageMetadata();
    if (!metadata || !metadata->hasSecondaryIndices())
        return {};

    auto secondary_index_sizes = read_from_mergetree_step.getMergeTreeData().getSecondaryIndexSizes();

    /// Get the list of columns: text_index we use latter and construct the size information needed by Replacer
    for (const auto & index_description : metadata->getSecondaryIndices())
    {
        if (index_description.type != "text")
            continue;

        auto size_it = secondary_index_sizes.find(index_description.name);
        if (size_it == secondary_index_sizes.end())
            continue;

        /// Poor man's detection if the index is not materialized.
        /// TODO This needs more work because the materialization is per part but here we check per column.
        if (size_it->second.marks == 0 || size_it->second.data_uncompressed == 0)
            continue;

        chassert(index_description.column_names.size() == 1);
        columns_to_index_infos.emplace(index_description.column_names.front(), IndexInfo(index_description.name, size_it->second));
    }

    return columns_to_index_infos;
}

}

/// This class substitutes filters with text-search functions by new internal functions which skip IO and read less data.
///
/// The substitution is performed in this early optimization step because:
/// 1, We want to exclude the column information to read as soon as possible
/// 2. At some point we pretend to take advantage of lazy materialization
///
/// The optimization could be implemented in two variants: function -> function and function -> column. We choose the first approach because
/// in the new function the main cost is the decompression of the compressed posting lists (currently stored as roaring bitmap). So, the
/// function -> function approach is more efficient and parallelizable.
///
/// For a query like
///     SELECT count() FROM table WHERE hasToken(text_col, 'token')
/// if 1) text_col has an associated text index called text_col_idx, and 2) hasToken is an replaceable function (according to
/// isReplaceableFunction), then this class replaces some nodes in the ActionsDAG (and references to them) to generate an
/// equivalent query
///     SELECT count() FROM table where _hasToken_index('text_col_idx', 'token', _part_index, _part_offset)
///
/// The new query will execute a lot faster the original one because:
/// 1. It does not require direct access to text_col but only to text_col_idx (intended to be much smaller than the text column)
/// 2. With no access needed if can totally bypass the cost IO operations
/// 3. The text index was already read during the granules filter step
/// 4. Still uses all the parallelization infrastructure out of the box
///
/// The main entry point of this class are the constructor and the replace function. All the other api is support functionality
/// to ensure that the replacement is possible and correct.
///
/// This class is a (C++) friend of ActionsDAG and can therefore access its private members.
///
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach simplifies the work by avoiding
/// conflicts and minimizing coupling between this optimization and ActionsDAG.
class FullTextMatchingFunctionDAGReplacer
{
public:
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const std::unordered_map<String, IndexInfo> & columns_to_index_info_)
        : actions_dag(actions_dag_)
        , columns_to_index_info(columns_to_index_info_)
    {
    }

    /// This optimization replaces text-search functions by internal functions.
    /// Example: hasToken(text_col, 'token') -> hasToken('text_col_idx', 'token', _part_index, _part_offset)
    ///
    /// In detail:
    /// 0. Insert a new temporal node with the replacement function, including extra column nodes.
    /// 1. Remove the `text_col` column input + node if it is no longer referenced after substitution.
    /// 2. Replace inplace the old function node with the new one (memcpy to keep the memory address)
    /// 3. Pop the temporal node.
    /// 4. Update the output references if needed.
    ///
    /// The function returns a pair with <number of replacements, wector with removed column names>
    /// Not all replacements end with a column removal, so the number of replacements >= column names size
    std::pair<IndexReadTasks, Names> replace()
    {
        IndexReadTasks index_read_tasks;
        Names original_inputs = actions_dag.getRequiredColumnsNames();

        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            auto text_search_mode = getTextSearchMode(node);
            if (!text_search_mode.has_value())
                continue;

            if (!hasChildColumnNodeWithTextIndex(node))
                continue;

            if (actions_dag.getOutputs().empty() || actions_dag.getOutputs().front()->result_name != node.result_name)
                continue;

            auto replaced = tryReplaceFunctionNodeInplace(node);

            if (replaced.has_value())
            {
                const auto & [index_name, column_name] = replaced.value();

                auto & index_task = index_read_tasks[index_name];
                index_task.search_modes.push_back(text_search_mode.value());
                index_task.columns.emplace_back(column_name, node.result_type);

                auto split = actions_dag.splitActionsForFilter(column_name);

                PrewhereExprStep step
                {
                    .type = PrewhereExprStep::Filter,
                    .actions = std::make_shared<ExpressionActions>(split.first.clone()),
                    .filter_column_name = split.first.getOutputs().front()->result_name,
                    .remove_filter_column = false,
                    .need_filter = true,
                    .perform_alter_conversions = true,
                    .mutation_version = std::nullopt,
                };

                index_task.prewhere_step = std::make_shared<PrewhereExprStep>(std::move(step));
            }
        }

        if (index_read_tasks.empty())
            return {{}, {}};

        actions_dag.removeUnusedActions();

        Names removed_columns;
        Names replaced_columns = actions_dag.getRequiredColumnsNames();
        std::ranges::set_difference(original_inputs, replaced_columns, std::back_inserter(removed_columns));

        return {index_read_tasks, removed_columns};
    }

private:
    ActionsDAG & actions_dag;
    const std::unordered_map<String, IndexInfo> & columns_to_index_info;

    static std::optional<TextSearchMode> getTextSearchMode(const ActionsDAG::Node & node)
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function)
            return std::nullopt;

        const auto & function_name = node.function->getName();

        if (function_name == "searchAny")
            return TextSearchMode::Any;

        if (function_name == "hasToken" || function_name == "searchAll")
            return TextSearchMode::All;

        return std::nullopt;
    }

    bool isColumnNodeWithTextIndex(const ActionsDAG::Node * node) const
    {
        return (node->type == ActionsDAG::ActionType::INPUT
                && node->result_type->getTypeId() == TypeIndex::String
                && node->children.empty()
                && columns_to_index_info.contains(node->result_name));
    }

    bool hasChildColumnNodeWithTextIndex(const ActionsDAG::Node & node) const
    {
        return std::ranges::any_of(
            node.children,
            [&](const ActionsDAG::Node * child) { return isColumnNodeWithTextIndex(child); }
        );
    }

    /// Attempt to add a new node with the replacement function.
    /// This also adds extra input columns if needed.
    /// Returns the number of columns (inputs) replaced with an index name in the new function.
    std::optional<std::pair<String, String>> tryReplaceFunctionNodeInplace(ActionsDAG::Node & function_node)
    {
        chassert(getTextSearchMode(function_node).has_value());

        size_t num_text_columns = 0;
        size_t text_column_position = 0;

        for (size_t i = 0; i < function_node.children.size(); ++i)
        {
            if (isColumnNodeWithTextIndex(function_node.children[i]))
            {
                ++num_text_columns;
                text_column_position = i;
            }
        }

        if (num_text_columns != 1)
            return std::nullopt;

        const auto & index_info = columns_to_index_info.at(function_node.children[text_column_position]->result_name);
        String virtual_column_name = fmt::format("{}{}_{}", TEXT_INDEX_VIRTUAL_COLUMN_PREFIX, index_info.name, function_node.function->getName());

        function_node.type = ActionsDAG::ActionType::INPUT;
        function_node.result_name = virtual_column_name;
        function_node.function.reset();
        function_node.function_base.reset();
        function_node.children.clear();
        actions_dag.inputs.push_back(&function_node);

        return std::make_pair(index_info.name, virtual_column_name);
    }
};

/// Text index search queries have this form:
///     SELECT [...]
///     FROM tab
///     WHERE text_function(...), [...]
/// where
/// - text_function is a text-matching functions, e.g. 'hasToken',
///   text-matching functions expect that the column on which the function is called has a text index
/// TODO: add support for other function, before that support AND and OR operators
///
/// This function replaces text function nodes from the user query (using semi-brute-force process) with internal functions which use only
/// the index information to bypass the normal column scan (read step) which can consume more than 90% of execution time. The optimization
/// checks if the function's column node is a text column with a text index.
///
/// (*) Text search only makes sense if a text index exists on text. In the scope of this function, we don't care.
///     That check is left to query runtime, ReadFromMergeTree specifically.
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

    /// Get information of MATERIALIZED indices
    std::unordered_map<String, IndexInfo> columns_to_index_info = getIndexInfosForColumns(*read_from_merge_tree_step);
    if (columns_to_index_info.empty())
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    /// If the expression contains no columns with a text index as input, do nothing.
    bool input_node_has_column_with_text_index = false;
    for (const auto * const input_node : filter_step->getExpression().getInputs())
    {
        if (columns_to_index_info.contains(input_node->result_name))
        {
            input_node_has_column_with_text_index = true;
            break;
        }
    }

    if (!input_node_has_column_with_text_index)
        return;

    /// Now try to modify the ActionsDAG.
    FullTextMatchingFunctionDAGReplacer replacer(filter_step->getExpression(), columns_to_index_info);
    const auto [added_index_tasks, removed_columns] = replacer.replace();

    if (added_index_tasks.empty())
        return;

    read_from_merge_tree_step->replaceColumnsForTextSearch(removed_columns, added_index_tasks);
    auto new_filter_column_name = filter_step->getExpression().getOutputs().front()->result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_step->getExpression().clone(), new_filter_column_name, true);
}

}
