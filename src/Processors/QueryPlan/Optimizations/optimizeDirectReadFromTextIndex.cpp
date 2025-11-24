#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Common/logger_useful.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/defines.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

using IndexConditionsMap = absl::flat_hash_map<String, const MergeTreeIndexWithCondition *>;
using NodesReplacementMap = absl::flat_hash_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

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

const ActionsDAG::Node * replaceNodes(ActionsDAG & dag, const ActionsDAG::Node * node, const NodesReplacementMap & replacements)
{
    if (auto it = replacements.find(node); it != replacements.end())
    {
        return it->second;
    }
    else if (node->type == ActionsDAG::ActionType::ALIAS)
    {
        const auto * old_child = node->children[0];
        const auto * new_child = replaceNodes(dag, old_child, replacements);

        if (old_child != new_child)
            return &dag.addAlias(*new_child, node->result_name);
    }
    else if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        auto old_children = node->children;
        std::vector<const ActionsDAG::Node *> new_children;

        for (const auto & child : old_children)
            new_children.push_back(replaceNodes(dag, child, replacements));

        if (new_children != old_children)
            return &dag.addFunction(node->function_base, new_children, "");
    }

    return node;
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

<<<<<<< HEAD
=======
/// Helper function.
/// Collects index conditions from the given ReadFromMergeTree step and stores them in index_conditions.
static void collectTextIndexConditions(const ReadFromMergeTree * read_from_merge_tree_step, IndexToConditionMap & text_index_conditions)
{
    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

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
                text_index_conditions[index.index->index.name] = text_index_condition;
        }
    }
}

static bool isSupportedCondition(const ActionsDAG::Node & lhs_arg, const ActionsDAG::Node & rhs_arg, const MergeTreeIndexConditionText & condition)
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

static bool isSupportedDirectReadTextFunctionNode(const ActionsDAG::Node & function_node, const IndexToConditionMap & index_conditions)
{
    if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function)
        return false;

    if (function_node.children.size() != 2)
        return false;

    if (!MergeTreeIndexConditionText::isSupportedFunctionForDirectRead(function_node.function->getName()))
        return false;

    auto selected_it = std::ranges::find_if(index_conditions, [&](const auto & index_with_condition)
    {
        return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second);
    });

    if (selected_it == index_conditions.end())
        return false;

    size_t num_supported_conditions = std::ranges::count_if(index_conditions, [&](const auto & index_with_condition)
    {
        return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second);
    });

    /// Do not optimize if there are multiple text indexes set for the column.
    /// It is not clear which index to use.
    return num_supported_conditions == 1;
}

static bool isSupportedDirectReadTextFunctionsWithinDAG(const ActionsDAG & dag, const IndexToConditionMap & index_conditions)
{
    for (const auto & node : dag.getNodes())
        if (isSupportedDirectReadTextFunctionNode(node, index_conditions))
            return true;
    return false;
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere
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
/// This class is a (C++) friend of ActionsDAG and can therefore access its private members.
/// Some of the functions implemented here could be added to ActionsDAG directly, but this wrapper approach
/// simplifies the work by avoiding conflicts and minimizing coupling between this optimization and ActionsDAG.
class FullTextMatchingFunctionDAGReplacer
{
public:
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const IndexConditionsMap & index_conditions_)
        : actions_dag(actions_dag_)
        , index_conditions(index_conditions_)
    {
    }

<<<<<<< HEAD
    struct ResultReplacement
=======
    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0
    /// Returns a pair of (added columns by index name, removed columns)
    std::pair<IndexReadColumns, Names> replace(bool add_alias = false)
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere
    {
        IndexReadColumns added_columns;
        Names removed_columns;
        const ActionsDAG::Node * filter_node = nullptr;
    };

    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0.
    ResultReplacement replace(const ContextPtr & context, const String & filter_column_name)
    {
        ResultReplacement result;
        NodesReplacementMap replacements;
        Names original_inputs = actions_dag.getRequiredColumnsNames();
<<<<<<< HEAD
        const auto * filter_node = &actions_dag.findInOutputs(filter_column_name);

        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            auto replaced = tryReplaceFunctionNode(node, context);

            if (replaced.has_value())
            {
                replacements[&node] = replaced->node;

                for (const auto & [index_name, column_name] : replaced->index_name_to_virtual_column)
                    result.added_columns[index_name].emplace_back(column_name, std::make_shared<DataTypeUInt8>());
            }
        }

        if (result.added_columns.empty())
            return result;
=======
        NamesWithAliases names_with_aliases;
        NameSet output_result_names;
        for (const auto & output : actions_dag.getOutputs())
            output_result_names.insert(output->result_name);
        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            String origin_result_name = node.result_name;
            auto replaced = tryReplaceFunctionNodeInplace(node);

            if (replaced.has_value())
            {
                const auto & [index_name, column_name] = replaced.value();
                if (output_result_names.contains(origin_result_name))
                    names_with_aliases.push_back({column_name, origin_result_name});
                added_columns[index_name].emplace_back(column_name, node.result_type);
            }
        }

        if (added_columns.empty())
            return {{}, {}};
        if (add_alias && !names_with_aliases.empty())
            actions_dag.addAliases(names_with_aliases);
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere

        for (auto & output : actions_dag.outputs)
        {
            bool is_filter_node = output == filter_node;
            output = replaceNodes(actions_dag, output, replacements);

            if (is_filter_node)
                filter_node = output;
        }

        result.filter_node = filter_node;
        actions_dag.removeUnusedActions();

        Names replaced_columns = actions_dag.getRequiredColumnsNames();
        NameSet replaced_columns_set(replaced_columns.begin(), replaced_columns.end());

        for (const auto & column : original_inputs)
        {
            if (!replaced_columns_set.contains(column))
                result.removed_columns.push_back(column);
        }

        return result;
    }

private:
<<<<<<< HEAD
    struct NodeReplacement
    {
        const ActionsDAG::Node * node;
        std::unordered_map<String, String> index_name_to_virtual_column;
    };

    ActionsDAG & actions_dag;
    IndexConditionsMap index_conditions;
=======
    ActionsDAG & actions_dag;
    std::unordered_map<String, MergeTreeIndexConditionText *> index_conditions;
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere

    /// Attempts to add a new node with the replacement virtual column.
    /// Returns the pair of (index name, virtual column name) if the replacement is successful.
    std::optional<NodeReplacement> tryReplaceFunctionNode(ActionsDAG::Node & function_node, const ContextPtr & context)
    {
<<<<<<< HEAD
        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function || !function_node.function_base)
            return std::nullopt;

        /// If function returns not UInt8 type, it is not a predicate and cannot be optimized by the text index.
        if (!WhichDataType(function_node.result_type).isUInt8())
            return std::nullopt;

        struct SelectedCondition
=======
        if (!isSupportedDirectReadTextFunctionNode(function_node, index_conditions))
            return std::nullopt;

        auto selected_it = std::ranges::find_if(index_conditions, [&](const auto & index_with_condition)
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere
        {
            TextSearchQueryPtr search_query;
            String index_name;
            String virtual_column_name;
        };

        NameSet used_index_columns;
        std::vector<SelectedCondition> selected_conditions;
        bool has_exact_search = false;

        for (const auto & [index_name, index] : index_conditions)
        {
            auto & text_index_condition = typeid_cast<MergeTreeIndexConditionText &>(*index->condition);
            const auto & index_header = text_index_condition.getHeader();

            /// Do not optimize if there are multiple text indexes set for the same expression.
            /// It is ambiguous which index to use. However, we allow to use several indexes for different expressions.
            /// for example, we can use indexes both for mapKeys(m) and mapValues(m) in one function m['key'] = 'value'.
            if (index_header.columns() != 1 || used_index_columns.contains(index_header.begin()->name))
                return std::nullopt;

            auto search_query = text_index_condition.createTextSearchQuery(function_node);
            if (!search_query || search_query->direct_read_mode == TextIndexDirectReadMode::None)
                continue;

            auto virtual_column_name = text_index_condition.replaceToVirtualColumn(*search_query, index_name);
            if (!virtual_column_name)
                continue;

            selected_conditions.push_back({search_query, index_name, *virtual_column_name});
            used_index_columns.insert(index_header.begin()->name);

            if (search_query->direct_read_mode == TextIndexDirectReadMode::Exact)
                has_exact_search = true;
        }

        if (selected_conditions.empty())
            return std::nullopt;

        NodeReplacement replacement;

        /// Sort conditions to produce stable output for EXPLAIN query.
        std::ranges::sort(selected_conditions, [](const auto & lhs, const auto & rhs)
        {
            return lhs.virtual_column_name < rhs.virtual_column_name;
        });

<<<<<<< HEAD
        /// If we have only one condition with exact search, we can use
        /// only virtual column and remove the original condition.
        if (selected_conditions.size() == 1 && has_exact_search)
        {
            const auto & condition = selected_conditions.front();
            replacement.index_name_to_virtual_column[condition.index_name] = condition.virtual_column_name;
            replacement.node = &actions_dag.addInput(condition.virtual_column_name, std::make_shared<DataTypeUInt8>());
            return replacement;
        }

        /// Otherwise, combine all conditions with the AND function.
        ActionsDAG::NodeRawConstPtrs children;
        auto function_builder = FunctionFactory::instance().get("and", context);

        for (const auto & condition : selected_conditions)
        {
            replacement.index_name_to_virtual_column[condition.index_name] = condition.virtual_column_name;
            children.push_back(&actions_dag.addInput(condition.virtual_column_name, std::make_shared<DataTypeUInt8>()));
        }
=======
        if (selected_it == index_conditions.end())
            return std::nullopt;

        const auto & [index_name, condition] = *selected_it;
        auto search_query = condition->createTextSearchQuery(function_node);
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere

        if (!has_exact_search)
        {
            children.push_back(&function_node);
        }

        replacement.node = &actions_dag.addFunction(function_builder, children, "");
        return replacement;
    }
};

static bool applyTextIndexDirectReadToDAG(ReadFromMergeTree * read_from_merge_tree_step, ActionsDAG & filter_dag, const IndexToConditionMap & text_index_conditions, bool add_alias)
{
    FullTextMatchingFunctionDAGReplacer replacer(filter_dag, text_index_conditions);
    auto [added_columns, removed_columns] = replacer.replace(add_alias);

    if (added_columns.empty())
        return false;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(added_columns, removed_columns));

    const auto & indexes = read_from_merge_tree_step->getIndexes();
    read_from_merge_tree_step->createReadTasksForTextIndex(indexes->skip_indexes, added_columns, removed_columns);
    return true;
}
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
void optimizeWhereDirectReadFromTextIndex(const Stack & stack, QueryPlan::Nodes & /*nodes*/)
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

<<<<<<< HEAD
    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

    IndexConditionsMap index_conditions;
    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (typeid_cast<MergeTreeIndexConditionText *>(index.condition.get()))
        {
            /// Index may be not materialized in some parts, e.g. after ALTER ADD INDEX query.
            /// TODO: support partial read from text index with fallback to the brute-force
            /// search for parts where index is not materialized.
            bool has_index_in_all_parts = std::ranges::all_of(unique_parts, [&](const auto & part)
            {
                return !!index.index->getDeserializedFormat(part->checksums, index.index->getFileName());
            });

            if (has_index_in_all_parts)
                index_conditions[index.index->index.name] = &index;
        }
    }

=======
    IndexToConditionMap index_conditions;
    collectTextIndexConditions(read_from_merge_tree_step, index_conditions);
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere
    if (index_conditions.empty())
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    FilterStep * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    ActionsDAG & filter_dag = filter_step->getExpression();
<<<<<<< HEAD
    FullTextMatchingFunctionDAGReplacer replacer(filter_dag, index_conditions);
    auto result = replacer.replace(read_from_merge_tree_step->getContext(), filter_step->getFilterColumnName());

    if (result.added_columns.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(result.added_columns, result.removed_columns));

    bool removes_filter_column = filter_step->removesFilterColumn();
    read_from_merge_tree_step->createReadTasksForTextIndex(indexes->skip_indexes, result.added_columns, result.removed_columns);

    auto new_filter_column_name = result.filter_node->result_name;
=======
    const ActionsDAG::Node & filter_column_node = filter_dag.findInOutputs(filter_step->getFilterColumnName());

    /// Simply forbid the case that PREWHERE and WHERE contain eligible functions.
    auto prewhere_info = read_from_merge_tree_step->getPrewhereInfo();
    if (prewhere_info && isSupportedDirectReadTextFunctionsWithinDAG(prewhere_info->prewhere_actions, index_conditions) && isSupportedDirectReadTextFunctionsWithinDAG(filter_dag, index_conditions))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Using text index in both PREWHERE and WHERE is not supported. Please move all text-index conditions into PREWHERE.");

    bool applied = applyTextIndexDirectReadToDAG(read_from_merge_tree_step, filter_dag, index_conditions, false);
    if (!applied)
        return;

     bool removes_filter_column = filter_step->removesFilterColumn();
    /// The original node (pointer address) should be preserved in the DAG outputs because we replace in-place.
    /// However, the `result_name` could be different if the output column was replaced.
    chassert(std::ranges::contains(filter_dag.getOutputs(), &filter_column_node));

    const String new_filter_column_name = filter_column_node.result_name;
>>>>>>> fastio/enable_using_inverted_text_index_in_prewhere
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
}

/// Text index search queries have this form:
///     SELECT [...]
///     FROM tab
///     PREWHERE text_function(...), [...]
/// where
/// - text_function is a text-matching functions, e.g. 'hasToken'
/// - text-matching functions expect that the column on which the function is called has a text index
///
/// Same as `optimizeWhereDirectReadFromTextIndex, this function replaces text function nodes from the user query (using semi-brute-force process) with internal virtual columns
/// which use only the index information to bypass the normal column scan which can consume significant amount of the execution time.
void optimizePrewhereDirectReadFromTextIndex(const Stack & stack, QueryPlan::Nodes & /*nodes*/)
{
    const auto & frame = stack.back();
    ReadFromMergeTree * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    IndexToConditionMap index_conditions;
    collectTextIndexConditions(read_from_merge_tree_step, index_conditions);
    if (index_conditions.empty())
        return;

    auto prewhere_info = read_from_merge_tree_step->getPrewhereInfo();
    if (!prewhere_info)
        return;

    read_from_merge_tree_step->updatePrewhereInfo({});
    auto cloned_prewhere_info = prewhere_info->clone();
    auto applied = applyTextIndexDirectReadToDAG(read_from_merge_tree_step, cloned_prewhere_info.prewhere_actions, index_conditions, true);

    if (!applied)
    {
        read_from_merge_tree_step->updatePrewhereInfo(prewhere_info);
        return;
    }

    /// Finally, assign the corrected PrewhereInfo back to the plan node.
    auto modified_prewhere_info = std::make_shared<PrewhereInfo>(std::move(cloned_prewhere_info));
    read_from_merge_tree_step->updatePrewhereInfo(modified_prewhere_info);
}

/// Applies text index–based direct-read optimizations to the query.
/// This includes both WHERE and PREWHERE clauses.
///
/// - optimizeWhereDirectReadFromTextIndex(): push down text-index
///   filters from the WHERE clause.
/// - optimizePrewhereDirectReadFromTextIndex(): apply the same
///   optimization for the PREWHERE clause before data reads.
void optimizeDirectReadFromTextIndex(const Stack & stack, QueryPlan::Nodes & nodes)
{
    optimizeWhereDirectReadFromTextIndex(stack, nodes);
    optimizePrewhereDirectReadFromTextIndex(stack, nodes);
}

}
