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

namespace DB::QueryPlanOptimizations
{

using IndexConditionsMap = std::unordered_map<String, const MergeTreeIndexWithCondition *>;
using NodesReplacementMap = std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

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
    FullTextMatchingFunctionDAGReplacer(ActionsDAG & actions_dag_, const IndexConditionsMap & index_conditions_)
        : actions_dag(actions_dag_)
        , index_conditions(index_conditions_)
    {
    }

    struct ResultReplacement
    {
        IndexReadColumns added_columns;
        Names removed_columns;
        const ActionsDAG::Node * filter_node = nullptr;
    };

    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0
    /// Returns a pair of (added columns by index name, removed columns)
    ResultReplacement replace(const ContextPtr & context, const String & filter_column_name)
    {
        ResultReplacement result;
        NodesReplacementMap replacements;
        Names original_inputs = actions_dag.getRequiredColumnsNames();
        const auto * filter_node = &actions_dag.findInOutputs(filter_column_name);

        for (ActionsDAG::Node & node : actions_dag.nodes)
        {
            auto replaced = tryReplaceFunctionNode(node, context);

            if (replaced.has_value())
            {
                result.added_columns[replaced->index_name].emplace_back(replaced->column_name, std::make_shared<DataTypeUInt8>());
                replacements[&node] = replaced->node;
            }
        }

        if (result.added_columns.empty())
        {
            return result;
        }

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
    struct NodeReplacement
    {
        String index_name;
        String column_name;
        const ActionsDAG::Node * node;
    };

    ActionsDAG & actions_dag;
    IndexConditionsMap index_conditions;

    bool isSupportedCondition(const ActionsDAG::Node & lhs_arg, const ActionsDAG::Node & rhs_arg, const IMergeTreeIndexCondition & condition) const
    {
        using enum ActionsDAG::ActionType;
        const auto & text_index_condition = typeid_cast<const MergeTreeIndexConditionText &>(condition);
        const auto & header = text_index_condition.getHeader();

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
    std::optional<NodeReplacement> tryReplaceFunctionNode(ActionsDAG::Node & function_node, const ContextPtr & context)
    {
        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function)
            return std::nullopt;

        if (function_node.children.size() != 2)
            return std::nullopt;

        auto selected_it = std::ranges::find_if(index_conditions, [&](const auto & index_with_condition)
        {
            return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second->condition);
        });

        if (selected_it == index_conditions.end())
            return std::nullopt;

        size_t num_supported_conditions = std::ranges::count_if(index_conditions, [&](const auto & index_with_condition)
        {
            return isSupportedCondition(*function_node.children[0], *function_node.children[1], *index_with_condition.second->condition);
        });

        /// Do not optimize if there are multiple text indexes set for the column.
        /// It is not clear which index to use.
        if (num_supported_conditions != 1)
            return std::nullopt;

        const auto & [index_name, condition] = *selected_it;
        auto & text_index_condition = typeid_cast<MergeTreeIndexConditionText &>(*condition->condition);

        auto direct_read_mode = text_index_condition.getDirectReadMode(function_node.function->getName());
        if (direct_read_mode == TextIndexDirectReadMode::None)
            return std::nullopt;

        auto search_query = text_index_condition.createTextSearchQuery(function_node);
        if (!search_query)
            return std::nullopt;

        auto virtual_column_name = text_index_condition.replaceToVirtualColumn(*search_query, index_name);
        if (!virtual_column_name)
            return std::nullopt;

        NodeReplacement replacement;
        replacement.index_name = index_name;
        replacement.column_name = virtual_column_name.value();

        switch (direct_read_mode)
        {
            case TextIndexDirectReadMode::None:
            {
                return std::nullopt;
            }
            case TextIndexDirectReadMode::Exact:
            {
                replacement.node = &actions_dag.addInput(virtual_column_name.value(), std::make_shared<DataTypeUInt8>());
                return replacement;
            }
            case TextIndexDirectReadMode::Hint:
            {
                auto function_builder = FunctionFactory::instance().get("and", context);
                const auto & input_virtual_column = actions_dag.addInput(virtual_column_name.value(), std::make_shared<DataTypeUInt8>());
                replacement.node = &actions_dag.addFunction(function_builder, {&input_virtual_column, &function_node}, "");
                return replacement;
            }
        }
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
                return !!index.index->getDeserializedFormat(part->getDataPartStorage(), index.index->getFileName());
            });

            if (has_index_in_all_parts)
                index_conditions[index.index->index.name] = &index;
        }
    }

    if (index_conditions.empty())
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    FilterStep * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    ActionsDAG & filter_dag = filter_step->getExpression();
    FullTextMatchingFunctionDAGReplacer replacer(filter_dag, index_conditions);
    auto result = replacer.replace(read_from_merge_tree_step->getContext(), filter_step->getFilterColumnName());

    if (result.added_columns.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(result.added_columns, result.removed_columns));

    bool removes_filter_column = filter_step->removesFilterColumn();
    read_from_merge_tree_step->replaceColumnsForTextSearch(result.added_columns, result.removed_columns);

    auto new_filter_column_name = result.filter_node->result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
}

}
