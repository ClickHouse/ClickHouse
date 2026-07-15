#include <Access/ContextAccess.h>
#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexBloomSliced.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/defines.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

using NodesReplacementMap = absl::flat_hash_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

struct IndexReadInfo
{
    const MergeTreeIndexWithCondition * index;
    bool is_materialized;
    bool is_fully_materialized;
};

using IndexReadInfos = absl::flat_hash_map<String, IndexReadInfo>;
using TextIndexReadInfos = IndexReadInfos;
using BloomSlicedIndexReadInfos = IndexReadInfos;

String getNameWithoutAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
    {
        node = node->children[0];
    }

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

/// Check if a node with the given canonical name exists as a subexpression within the DAG rooted at `node`.
bool hasSubexpression(const ActionsDAG::Node * node, const String & subexpression_name)
{
    if (getNameWithoutAliases(node) == subexpression_name)
        return true;

    for (const auto * child : node->children)
    {
        if (hasSubexpression(child, subexpression_name))
            return true;
    }

    return false;
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
    for (const auto & [_, added_virtual_columns] : added_columns)
    {
        for (const auto & added_virtual_column : added_virtual_columns)
        {
            if (++idx > 1)
                result += ", ";
            result += added_virtual_column.name;
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

// Shared direct-read/hint helpers.

template <typename IndexCondition>
void collectIndexReadInfos(
    const ReadFromMergeTree * read_from_merge_tree_step,
    IndexReadInfos & index_read_infos,
    const char * disabled_reason_prefix,
    bool decline_if_parts_have_patches)
{
    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromIndex");
    auto metadata_snapshot = read_from_merge_tree_step->getStorageMetadata();
    auto mutations_snapshot = read_from_merge_tree_step->getMutationsSnapshot();
    auto context = read_from_merge_tree_step->getContext();

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

    /// Compute the union of updated columns only across the parts that will actually be read by this step.
    /// Using `mutations_snapshot->getAllUpdatedColumns` directly would include pending updates from
    /// other partitions/parts not in `parts_with_ranges`, disabling direct index reads and token hints even when
    /// the queried parts have no on-the-fly updates for the index columns.
    NameSet all_updated_columns;
    bool any_part_has_patches = false;
    for (const auto & part : unique_parts)
    {
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
        );
        const auto & part_updated_columns = alter_conversions->getAllUpdatedColumns();
        all_updated_columns.insert(part_updated_columns.begin(), part_updated_columns.end());
        any_part_has_patches |= alter_conversions->hasPatches();
    }

    /// The index read step produced by this optimization is prepended to the reader chain and
    /// reads no physical data, so it cannot anchor patch application, which aligns patches by
    /// `_part_offset`. Adding the patch system columns to that step turns it into a mixed step
    /// that is no longer dispatched to the index reader, and the virtual column is not produced.
    /// This happens even when the patched column is unrelated to the index column (hence the
    /// per-index `canUseIndex` check below is not enough). Whether that is a correctness problem
    /// depends on how the caller consumes the virtual column, so declining is the caller's choice
    /// (see `decline_if_parts_have_patches` at the call sites).
    if (decline_if_parts_have_patches && any_part_has_patches)
    {
        LOG_TRACE(logger, "{} because some parts have patch parts (lightweight updates)", disabled_reason_prefix);
        return;
    }

    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (!typeid_cast<const IndexCondition *>(index.condition_template->generateUnsubstituted().get()))
            continue;

        if (auto result = MergeTreeDataSelectExecutor::canUseIndex(index.index, metadata_snapshot, all_updated_columns); !result)
        {
            LOG_TRACE(logger, "{}. Reason: {}", disabled_reason_prefix, result.error().text);
            continue;
        }

        /// Index may be not materialized in some parts, e.g. after ALTER ADD INDEX query.
        size_t num_materialized_parts = std::ranges::count_if(unique_parts, [&](const auto & part)
        {
            return !!index.index->getDeserializedFormat(part->checksums, index.index->getFileName(), &part->getDataPartStorage());
        });

        index_read_infos[index.index->index.name] =
        {
            .index = &index,
            .is_materialized = num_materialized_parts > 0,
            .is_fully_materialized = num_materialized_parts == unique_parts.size()
        };
    }
}

void collectTextIndexReadInfos(const ReadFromMergeTree * read_from_merge_tree_step, TextIndexReadInfos & text_index_read_infos)
{
    /// Text direct read replaces the predicate with the index virtual column, and the virtual
    /// column's default expression re-evaluates the search on the (absent) source column, so a
    /// step polluted by patch system columns drops rows or throws UNKNOWN_IDENTIFIER. Fall back
    /// to regular index reading for the whole query when any queried part has patch parts.
    collectIndexReadInfos<MergeTreeIndexConditionText>(
        read_from_merge_tree_step,
        text_index_read_infos,
        "Cannot use direct reading from text index",
        /*decline_if_parts_have_patches=*/ true);
}

void collectBloomSlicedIndexReadInfos(const ReadFromMergeTree * read_from_merge_tree_step, BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos)
{
    if (read_from_merge_tree_step->isQueryWithFinal())
        return;

    /// Unlike text direct read, the bloom_sliced hint keeps the hint step when parts have patch
    /// parts: this is intentional, per-part fail-open degradation rather than a whole-query
    /// decline. The hint is only ever a pre-filter AND-ed with the original predicate (see
    /// `prependBloomSlicedHintToPrewhereInfo`), and its virtual column's default expression is
    /// the literal 1 (see `buildBloomSlicedHintDAG`). For a part with patch parts, the patch
    /// system columns turn the hint step into a mixed step that is not dispatched to the index
    /// reader; the virtual column is then not produced and the reader default-fills it with the
    /// fail-open literal, so every row passes the hint and the original predicate does the real
    /// filtering: results stay correct, and the hint keeps pruning in parts without patches.
    collectIndexReadInfos<MergeTreeIndexConditionBloomSliced>(
        read_from_merge_tree_step,
        bloom_sliced_index_read_infos,
        "Cannot add bloom_sliced token hint",
        /*decline_if_parts_have_patches=*/ false);
}

// Text-index direct-read and preprocessing helpers.

/// Converts an ActionsDAG node to an AST node.
/// It is not correct in the general case, but is
/// sufficient for expressions that can be used with a text index.
/// `captured` maps a lambda's captured-column names to the nodes that supply their values in the
/// outer DAG, so references to them inside the lambda body are inlined (typically as literals)
/// instead of being emitted as bare, unresolvable identifiers.
ASTPtr convertNodeToAST(const ActionsDAG::Node & node, const std::unordered_map<std::string, const ActionsDAG::Node *> & captured = {});

/// Reconstructs a captured lambda (e.g. the `x -> f(x)` inside arrayMap) as `lambda(tuple(args), body)`.
/// `captured_values` are the columns supplied for the capture, aligned with capture.captured_names.
ASTPtr convertCapturedLambdaToAST(const FunctionCapture & function_capture, const ActionsDAG::NodeRawConstPtrs & captured_values)
{
    const auto & capture = function_capture.getCapture();
    const auto & capture_dag = function_capture.getAcionsDAG();
    if (capture_dag.getOutputs().size() != 1 || captured_values.size() != capture.captured_names.size())
        return nullptr;

    /// Bind each captured column to the value passed into the capture so the body has no dangling refs.
    std::unordered_map<std::string, const ActionsDAG::Node *> body_captured;
    for (size_t i = 0; i < capture.captured_names.size(); ++i)
        body_captured.emplace(capture.captured_names[i], captured_values[i]);

    auto arguments = make_intrusive<ASTFunction>();
    arguments->name = "tuple";
    arguments->arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(arguments->arguments);
    for (const auto & lambda_argument : capture.lambda_arguments)
        arguments->arguments->children.push_back(make_intrusive<ASTIdentifier>(lambda_argument.name));

    auto lambda = make_intrusive<ASTFunction>();
    lambda->name = "lambda";
    lambda->arguments = make_intrusive<ASTExpressionList>();
    lambda->children.push_back(lambda->arguments);
    lambda->arguments->children.push_back(std::move(arguments));
    lambda->arguments->children.push_back(convertNodeToAST(*capture_dag.getOutputs().front(), body_captured));
    return lambda;
}

ASTPtr convertNodeToAST(const ActionsDAG::Node & node, const std::unordered_map<std::string, const ActionsDAG::Node *> & captured)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
            if (auto it = captured.find(node.result_name); it != captured.end())
                return convertNodeToAST(*it->second);
            return make_intrusive<ASTIdentifier>(node.result_name);

        case ActionsDAG::ActionType::COLUMN:
            return node.column ? make_intrusive<ASTLiteral>((*node.column)[0]) : make_intrusive<ASTLiteral>(Field{});

        case ActionsDAG::ActionType::ALIAS:
            return node.children.empty() ? nullptr : convertNodeToAST(*node.children[0], captured);

        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node.function_base)
                return nullptr;

            if (const auto * function_capture = dynamic_cast<const FunctionCapture *>(node.function_base.get()))
                return convertCapturedLambdaToAST(*function_capture, node.children);

            auto function = make_intrusive<ASTFunction>();
            function->arguments = make_intrusive<ASTExpressionList>();
            function->children.push_back(function->arguments);
            function->name = node.function_base->getName();
            for (const auto * child : node.children)
            {
                if (auto arg_ast = convertNodeToAST(*child, captured))
                    function->arguments->children.push_back(arg_ast);
            }

            return function;
        }
        default:
            return nullptr;
    }
}

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
/// Also this class processes text index functions (hasToken, hasAllTokens, hasAnyTokens):
/// applies tokenizer and preprocessors (lower, upper, etc.) for the haystack and needles arguments.
/// It allows their standalone execution without the direct read from text index.
/// It is required to return the the same results as with the direct read.
///
/// For example, for the index `idx_s (s) type = text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s))`
/// the function `hasAllTokens(s, 'some needles')` will be replaced by `hasAllTokens(lower(s), ['some', 'needles'], 'splitByNonAlpha')`.
class TextIndexDAGReplacer
{
public:
    TextIndexDAGReplacer(ActionsDAG & actions_dag_, const TextIndexReadInfos & text_index_read_infos_, bool direct_read_from_text_index_)
        : actions_dag(actions_dag_)
        , text_index_read_infos(text_index_read_infos_)
        , direct_read_from_text_index(direct_read_from_text_index_)
    {
    }

    struct ResultReplacement
    {
        IndexReadColumns added_columns;
        Names removed_columns;
        const ActionsDAG::Node * filter_node = nullptr;
    };

    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0.
    ///
    /// Applies preprocessor, tokenizer and postprocessor in chain for text-search functions.
    /// Example: hasAllTokens(text_col, 'token1 token2') -> hasToken(lower(text_col), ['token1', 'token2'], 'splitByNonAlpha').
    ResultReplacement replace(const ContextPtr & context, const String & filter_column_name)
    {
        ResultReplacement result;
        NodesReplacementMap replacements;
        Names original_inputs = actions_dag.getRequiredColumnsNames();
        const auto * filter_node = &actions_dag.findInOutputs(filter_column_name);

        /// Cache for added input nodes for each virtual column.
        std::unordered_map<String, const ActionsDAG::Node *> virtual_column_to_node;

        /// Pre-populate the cache with any text-index virtual column inputs that are already present in this DAG from a previous
        /// optimization pass. This prevents them from being re-added to `added_columns` when the same DAG is processed again.
        ///
        /// See: https://github.com/ClickHouse/ClickHouse/issues/101913#issuecomment-4198784580
        for (const auto * input : actions_dag.getInputs())
        {
            if (input->result_name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX))
                virtual_column_to_node.emplace(input->result_name, input);
        }

        /// Copy pointers to nodes to avoid the modification of nodes in the dag while iterating over them.
        auto nodes_ptrs = actions_dag.getNodesPointers();

        for (const auto * node : nodes_ptrs)
        {
            auto replaced = processFunctionNode(*node, virtual_column_to_node, context);

            if (replaced.node != node)
                replacements[node] = replaced.node;

            for (auto & [index_name, virtual_column] : replaced.added_virtual_columns)
                result.added_columns[index_name].add(std::move(virtual_column));
        }

        if (replacements.empty())
            return result;

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
        const ActionsDAG::Node * node = nullptr;
        std::unordered_map<String, VirtualColumnDescription> added_virtual_columns;
    };

    ActionsDAG & actions_dag;
    TextIndexReadInfos text_index_read_infos;
    bool direct_read_from_text_index = false;

    struct SelectedCondition
    {
        TextSearchQueryPtr search_query;
        String index_name;
        String virtual_column_name;
        const IndexReadInfo * info = nullptr;
    };

    /// has/hasAll/hasAny operate on array elements directly, bypassing the tokenizer, preprocessor, and postprocessor.
    static bool needApplyTokenizer(const String & function_name)
    {
        return function_name == "hasAllTokens" || function_name == "hasAnyTokens" || function_name == "hasPhrase";
    }

    /// Returns true for functions that require applying the preprocessor to the haystack.
    /// has/hasAll/hasAny bypass both transforms.
    static bool needApplyPreprocessor(const String & function_name)
    {
        return function_name == "hasToken"
            || function_name == "hasAllTokens" || function_name == "hasAnyTokens" || function_name == "hasPhrase";
    }

    /// Returns true for functions that require applying the postprocessor to the haystack and needle.
    static bool needApplyPostprocessor(const String & function_name)
    {
        return function_name == "hasToken"
            || function_name == "hasAllTokens" || function_name == "hasAnyTokens"
            || function_name == "hasPhrase";
    }

    std::vector<SelectedCondition> selectConditions(const ActionsDAG::Node & function_node, const ContextPtr & context)
    {
        /// Canonicalize the function-node subtree so that the serialized column names
        /// fed to MergeTreeIndexConditionText::traverseFunctionNode match the ones
        /// produced when the condition was originally constructed in ReadFromMergeTree::applyFilters.
        ActionsDAGWithInversionPushDown canonical_dag(&function_node, context, /* boolean_context */ false);
        const auto & canonical_node = canonical_dag.predicate ? *canonical_dag.predicate : function_node;

        NameSet used_index_columns;
        std::vector<SelectedCondition> selected_conditions;

        for (const auto & [index_name, info] : text_index_read_infos)
        {
            auto & text_index_condition = typeid_cast<MergeTreeIndexConditionText &>(*info.index->condition_template->generateUnsubstituted());
            const auto & index_header = text_index_condition.getHeader();

            /// Take the first text index if there are multiple text indexes set for the same expression.
            /// It is ambiguous which index to use. However, we allow to use several indexes for different expressions.
            /// for example, we can use indexes both for mapKeys(m) and mapValues(m) in one function m['key'] = 'value'.
            if (index_header.columns() != 1 || used_index_columns.contains(index_header.begin()->name))
                continue;

            auto search_query = text_index_condition.createTextSearchQuery(canonical_node);
            if (!search_query)
                continue;

            /// For None mode, the condition is still needed for preprocessing (tokenizer/preprocessor injection).
            if (search_query->direct_read_mode == TextIndexDirectReadMode::None)
            {
                selected_conditions.emplace_back(search_query, index_name, String{}, &info);
                used_index_columns.insert(index_header.begin()->name);
                continue;
            }

            auto virtual_column_name = text_index_condition.replaceToVirtualColumn(*search_query, index_name);
            if (!virtual_column_name)
                continue;

            selected_conditions.emplace_back(search_query, index_name, *virtual_column_name, &info);
            used_index_columns.insert(index_header.begin()->name);
        }

        return selected_conditions;
    }

    NodeReplacement processFunctionNode(
        const ActionsDAG::Node & function_node,
        std::unordered_map<String, const ActionsDAG::Node *> & virtual_column_to_node,
        const ContextPtr & context)
    {
        NodeReplacement replacement;
        replacement.node = &function_node;

        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function || !function_node.function_base)
            return replacement;

        /// Skip if function is not a predicate. It doesn't make sense to analyze it.
        if (!function_node.result_type->canBeUsedInBooleanContext())
            return replacement;

        auto function_name = function_node.function_base->getName();
        bool need_transform_function = needApplyTokenizer(function_name) || needApplyPreprocessor(function_name);

        /// Early exit if there is nothing to process.
        if (!need_transform_function && !direct_read_from_text_index)
            return replacement;

        auto selected_conditions = selectConditions(function_node, context);
        if (selected_conditions.empty())
            return replacement;

        /// Sort conditions to produce stable output for EXPLAIN query.
        std::ranges::sort(selected_conditions, [](const auto & lhs, const auto & rhs)
        {
            return lhs.virtual_column_name < rhs.virtual_column_name;
        });

        if (need_transform_function)
            processTextIndexFunction(replacement, selected_conditions, context);

        if (direct_read_from_text_index)
            replaceFunctionsToVirtualColumns(replacement, selected_conditions, virtual_column_to_node, context);

        return replacement;
    }

    /// Applies preprocessor, tokenizer and postprocessor for text-search functions.
    void processTextIndexFunction(
        NodeReplacement & replacement,
        const std::vector<SelectedCondition> & selected_conditions,
        const ContextPtr & context)
    {
        const auto & function_node = *replacement.node;
        if (selected_conditions.size() != 1 || function_node.children.size() != 2)
            return;

        auto new_children = function_node.children;
        const auto & arg_haystack = new_children[0];
        const auto & arg_needles = new_children[1];

        if (arg_needles->type != ActionsDAG::ActionType::COLUMN || !arg_needles->column)
            return;

        if (arg_needles->column->onlyNull())
            return;

        Field needles_field = (*arg_needles->column)[0];
        DataTypePtr needles_type = arg_needles->result_type;

        const auto & condition = selected_conditions.front();
        const auto & condition_text = typeid_cast<MergeTreeIndexConditionText &>(*condition.info->index->condition_template->generateUnsubstituted());
        auto preprocessor = condition_text.getPreprocessor();
        auto postprocessor = condition_text.getPostprocessor();
        const bool has_postprocessor = postprocessor && postprocessor->hasActions();
        const auto * tokenizer = condition_text.getTokenizer();
        auto function_name = replacement.node->function_base->getName();

        if (needApplyPreprocessor(function_name) && preprocessor && preprocessor->hasActions())
        {
            const auto & preprocessor_dag = preprocessor->getOriginalActionsDAG();
            chassert(preprocessor_dag.getOutputs().size() == 1);
            const auto & preprocessor_output = preprocessor_dag.getOutputs().front();
            auto haystack_name = getNameWithoutAliases(arg_haystack);

            /// Check that preprocessor contains current expression as its argument.
            if (hasSubexpression(preprocessor_output, haystack_name))
            {
                ActionsDAG::NodeRawConstPtrs merged_outputs;
                actions_dag.mergeNodes(preprocessor_dag.clone(), &merged_outputs);

                chassert(merged_outputs.size() == 1);
                new_children[0] = merged_outputs.front();

                /// Needles in array are not processed and passed as is.
                if (needles_field.getType() == Field::Types::String)
                {
                    needles_field = preprocessor->processConstant(needles_field.safeGet<String>());
                    needles_type = std::make_shared<DataTypeString>();
                }
            }
        }

        if (needApplyTokenizer(function_node.function_base->getName()) && tokenizer)
        {
            const String tokenizer_description = tokenizer->getDescription();

            /// Add argument with tokenizer definition.
            DataTypePtr arg_type = std::make_shared<DataTypeString>();
            MutableColumnConstPtr arg_column = arg_type->createColumnConst(0, Field(tokenizer_description));
            String name = quoteString(tokenizer_description);
            const ActionsDAG::Node & new_child = actions_dag.addColumn(std::move(arg_column), std::move(arg_type), std::move(name));
            new_children.push_back(&new_child);

            /// Convert needles to array if they are a string by applying a tokenizer.
            /// For hasPhrase the phrase must stay as a string — tokenization is done inside hasPhrase itself.
            const bool convert_needle_to_array = function_name == "hasAnyTokens" || function_name == "hasAllTokens";
            if (convert_needle_to_array && needles_field.getType() == Field::Types::String)
            {
                VectorWithMemoryTracking<String> needles_array;
                const auto & needles_string = needles_field.safeGet<String>();
                tokenizer->stringToTokens(needles_string.data(), needles_string.size(), needles_array);
                /// Skip tokenizer-specific compaction when a postprocessor is configured: these needle tokens
                /// are postprocessed and deduplicated below instead, because sparseGrams containment
                /// compaction is unsound after a postprocessor (it can drop a required token).
                if (!has_postprocessor)
                    needles_array = tokenizer->compactTokens(needles_array);
                needles_field = Array(needles_array.begin(), needles_array.end());
                needles_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
            }
        }

        /// Rewrite the haystack into the postprocessed tokens the index stores, so the row-level
        /// function still matches when the index isn't read directly (direct read off, or unmaterialized
        /// parts). getOriginalActionsDAG yields an Array(String) of postprocessed tokens.
        if (needApplyPostprocessor(function_name) && has_postprocessor)
        {
            auto haystack_name = getNameWithoutAliases(new_children[0]);
            ActionsDAG::NodeRawConstPtrs merged_outputs;
            actions_dag.mergeNodes(
                postprocessor->getOriginalActionsDAG(haystack_name, new_children[0]->result_type, tokenizer->getDescription()),
                &merged_outputs);
            chassert(merged_outputs.size() == 1);
            new_children[0] = merged_outputs.front();

            /// new_children[0] is now an Array(String) of FINAL postprocessed tokens. hasAnyTokens /
            /// hasAllTokens would otherwise re-tokenize each array element with the tokenizer argument,
            /// re-splitting tokens the index stores whole (e.g. a postprocessor that emits separators like
            /// concat(val, ' x')). Match the elements verbatim by switching the tokenizer argument to 'array'.
            if (function_name == "hasAnyTokens" || function_name == "hasAllTokens")
            {
                chassert(new_children.size() == 3);
                DataTypePtr arg_type = std::make_shared<DataTypeString>();
                const String array_tokenizer_desc = ArrayTokenizer::getName();
                MutableColumnConstPtr arg_column = arg_type->createColumnConst(0, Field(array_tokenizer_desc));
                new_children[2] = &actions_dag.addColumn(std::move(arg_column), arg_type, quoteString(array_tokenizer_desc));
            }

            /// hasToken and hasPhrase take a String haystack, so rejoin the postprocessed tokens with a
            /// separator; the function re-tokenizes them. Tokens the postprocessor dropped are empty array
            /// elements that become adjacent separators and produce no token on re-split, reproducing the
            /// index's dense position sequence. hasAnyTokens/hasAllTokens accept the Array(String) directly.
            if (function_name == "hasToken" || function_name == "hasPhrase")
            {
                DataTypePtr separator_type = std::make_shared<DataTypeString>();
                MutableColumnConstPtr separator_column = separator_type->createColumnConst(0, Field(String(" ")));
                const ActionsDAG::Node & separator = actions_dag.addColumn(std::move(separator_column), separator_type, "' '");
                FunctionOverloadResolverPtr concat = FunctionFactory::instance().get("arrayStringConcat", context);
                new_children[0] = &actions_dag.addFunction(concat, {new_children[0], &separator}, "");
            }

            if (function_name == "hasPhrase" && needles_field.getType() == Field::Types::String)
            {
                /// The needle is a phrase: tokenize it, postprocess each token (dropping empties), and rejoin
                /// with a space so hasPhrase re-tokenizes it into the same dense postprocessed token sequence
                /// the index stored.
                const auto & phrase = needles_field.safeGet<String>();
                VectorWithMemoryTracking<String> tokens;
                tokenizer->stringToTokens(phrase.data(), phrase.size(), tokens);
                tokens = postprocessor->processTokens(std::move(tokens));

                String joined;
                for (const auto & token : tokens)
                {
                    if (std::ranges::any_of(token, isTokenSeparator))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index postprocessor produced an invalid token '{}'", token);
                    if (!joined.empty())
                        joined += ' ';
                    joined += token;
                }
                needles_field = joined;
            }
            else if (needles_field.getType() == Field::Types::String)
            {
                /// hasToken case: single token string. If the postprocessor drops the needle (stop-word
                /// filter, etc.), the empty needle is fine — hasToken returns 0 on it, matching the
                /// index-condition empty-sentinel that no granule contains.
                /// If the postprocessed token contains separator characters it would be ill-formed as a
                /// hasToken* needle (BAD_ARGUMENTS / NULL on non-indexed parts in Exact mode), so keep
                /// the original needle in that case.
                VectorWithMemoryTracking<String> tokens = postprocessor->processTokens(VectorWithMemoryTracking<String>{needles_field.safeGet<String>()});
                if (tokens.empty())
                    needles_field = String{};
                else if (std::ranges::any_of(tokens.front(), isTokenSeparator))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index postprocessor produced an invalid token '{}'", tokens.front());
                else
                    needles_field = tokens.front();
            }
            else if (needles_field.getType() == Field::Types::Array)
            {
                const auto & src_array = needles_field.safeGet<Array>();
                VectorWithMemoryTracking<String> tokens;
                for (const Field & element : src_array)
                    if (element.getType() == Field::Types::String)
                        tokens.push_back(element.safeGet<String>());
                /// Postprocess, then deduplicate. Do not run tokenizer-specific compaction: sparseGrams
                /// containment compaction is unsound after a postprocessor (see stringToTokens) and could
                /// drop a required token, disagreeing with the materialized index.
                tokens = postprocessor->processTokens(std::move(tokens));
                std::unordered_set<String> unique_tokens(tokens.begin(), tokens.end());
                needles_field = Array(unique_tokens.begin(), unique_tokens.end());
                needles_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
            }
        }

        /// Recreate an argument with needles.
        auto needles_column = needles_type->createColumnConst(0, needles_field);
        new_children[1] = &actions_dag.addColumn(std::move(needles_column), needles_type, applyVisitor(FieldVisitorToString(), needles_field));

        /// Recreate a function object because we have modified the arguments.
        FunctionOverloadResolverPtr new_function_base = FunctionFactory::instance().get(function_name, context);
        const ActionsDAG::Node * new_function_node = &actions_dag.addFunction(new_function_base, new_children, "");

        if (!new_function_node->result_type->equals(*function_node.result_type))
            new_function_node = &actions_dag.addCast(*new_function_node, function_node.result_type, "", context);

        replacement.node = &actions_dag.addAlias(*new_function_node, function_node.result_name);
    }

    /// Optimizes text-search functions by replacing them with virtual columns.
    void replaceFunctionsToVirtualColumns(
        NodeReplacement & replacement,
        const std::vector<SelectedCondition> & all_conditions,
        std::unordered_map<String, const ActionsDAG::Node *> & virtual_column_to_node,
        const ContextPtr & context)
    {
        const ActionsDAG::Node & function_node = *replacement.node;

        std::vector<SelectedCondition> selected_conditions;
        for (const auto & condition : all_conditions)
        {
            if (condition.search_query->direct_read_mode != TextIndexDirectReadMode::None)
                selected_conditions.push_back(condition);
        }
        if (selected_conditions.empty())
            return;

        bool has_exact_search = false;
        bool has_materialized_index = false;

        for (const auto & condition : selected_conditions)
        {
            has_materialized_index |= condition.info->is_materialized;
            has_exact_search |= condition.search_query->direct_read_mode == TextIndexDirectReadMode::Exact;
        }

        /// It doesn't make sense to optimize if index is not materialized in any data part.
        if (!has_materialized_index)
            return;

        auto add_condition_to_input = [&](const SelectedCondition & condition)
        {
            auto [it, inserted] = virtual_column_to_node.try_emplace(condition.virtual_column_name);

            if (inserted)
            {
                /// Create a default expression for the virtual column.
                /// It will be executed by merge tree reader when index is not materialized in the data part.
                ASTPtr default_expression;

                if (condition.search_query->direct_read_mode == TextIndexDirectReadMode::Exact)
                    default_expression = convertNodeToAST(function_node);
                /// Do not execute the default expression for hint mode, because it will be executed anyway in the original predicate.
                else if (condition.search_query->direct_read_mode == TextIndexDirectReadMode::Hint)
                    default_expression = make_intrusive<ASTLiteral>(Field(1));

                VirtualColumnDescription virtual_column(condition.virtual_column_name, std::make_shared<DataTypeUInt8>(), /*codec=*/ nullptr, condition.index_name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::Reader);
                virtual_column.default_desc.kind = ColumnDefaultKind::Default;
                virtual_column.default_desc.expression = std::move(default_expression);

                it->second = &actions_dag.addInput(condition.virtual_column_name, std::make_shared<DataTypeUInt8>());
                replacement.added_virtual_columns.emplace(condition.index_name, std::move(virtual_column));
            }

            return it->second;
        };

        /// If we have only one condition with exact search, we can use
        /// only virtual column and remove the original condition.
        if (selected_conditions.size() == 1 && has_exact_search)
        {
            replacement.node = add_condition_to_input(selected_conditions.front());
        }
        else /// Otherwise, combine all conditions with the AND function.
        {
            ActionsDAG::NodeRawConstPtrs children;
            auto function_builder = FunctionFactory::instance().get("and", context);

            for (const auto & condition : selected_conditions)
                children.push_back(add_condition_to_input(condition));

            if (!has_exact_search)
                children.push_back(&function_node);

            replacement.node = &actions_dag.addFunction(function_builder, children, "");
        }

        /// If the type of original function does not match the type of replacement,
        /// add a cast to the replacement to match the expected type (e.g. hasAnyTokens('hello world', toNullable('world'))).
        /// It can happen when the original function returns Nullable or LowCardinality type and replacement doesn't.
        if (!function_node.result_type->equals(*replacement.node->result_type))
            replacement.node = &actions_dag.addCast(*replacement.node, function_node.result_type, "", context);

        /// Preserve the original column name so that downstream steps (e.g. ExpressionStep for SELECT)
        /// that reference the predicate by its original name can still find it in the block.
        if (replacement.node->result_name != function_node.result_name)
            replacement.node = &actions_dag.addAlias(*replacement.node, function_node.result_name);
    }
};

static const ActionsDAG::Node * processAndOptimizeTextIndexDAG(
    ReadFromMergeTree & read_from_merge_tree_step,
    ActionsDAG & filter_dag,
    const TextIndexReadInfos & text_index_read_infos,
    const String & filter_column_name,
    bool direct_read_from_text_index)
{
    TextIndexDAGReplacer replacer(filter_dag, text_index_read_infos, direct_read_from_text_index);
    auto result = replacer.replace(read_from_merge_tree_step.getContext(), filter_column_name);

    /// Even when no virtual columns are added (added_columns is empty),
    /// the DAG may have been modified by text index preprocessing
    /// (e.g. applying tokenizer/preprocessor to hasAnyTokens).
    /// In that case, result.filter_node is non-null and we must return it
    /// so the caller can update the filter column name to match the modified DAG.
    if (result.added_columns.empty())
        return result.filter_node;

    auto logger = getLogger("processAndOptimizeTextIndexFunctions");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(result.added_columns, result.removed_columns));

    /// Log partially materialized text indexes
    for (const auto & [index_name, info] : text_index_read_infos)
    {
        if (!info.is_fully_materialized)
            LOG_DEBUG(logger, "Text index '{}' is not fully materialized. In some parts, direct read from text index cannot be used.", index_name);
    }

    const auto & indexes = read_from_merge_tree_step.getIndexes();
    bool is_final = read_from_merge_tree_step.isQueryWithFinal();
    read_from_merge_tree_step.createReadTasksForTextIndex(indexes->skip_indexes, result.added_columns, result.removed_columns, is_final);
    return result.filter_node;
}

static bool processAndOptimizeTextIndexFunctionsInPrewhere(
    ReadFromMergeTree & read_from_merge_tree_step,
    const PrewhereInfoPtr & prewhere_info,
    const TextIndexReadInfos & text_index_read_infos,
    bool direct_read_from_text_index)
{
    read_from_merge_tree_step.updatePrewhereInfo({});
    auto cloned_prewhere_info = prewhere_info->clone();
    const auto * result_filter_node = processAndOptimizeTextIndexDAG(read_from_merge_tree_step, cloned_prewhere_info.prewhere_actions, text_index_read_infos, cloned_prewhere_info.prewhere_column_name, direct_read_from_text_index);

    if (!result_filter_node)
    {
        read_from_merge_tree_step.updatePrewhereInfo(prewhere_info);
        return false;
    }

    cloned_prewhere_info.prewhere_column_name = result_filter_node->result_name;
    auto modified_prewhere_info = std::make_shared<PrewhereInfo>(std::move(cloned_prewhere_info));
    read_from_merge_tree_step.updatePrewhereInfo(modified_prewhere_info);
    return true;
}

namespace
{

// bloom_sliced token hint helpers.

struct BloomSlicedHintDAG
{
    ActionsDAG actions;
    String filter_column_name;
    IndexReadColumns added_columns;
};

struct BloomSlicedHintPredicate
{
    String index_name;
    String index_column_name;
    BloomSlicedTokenPredicate predicate;
};

std::optional<BloomSlicedHintPredicate> tryCreateBloomSlicedTokenPredicate(
    ReadFromMergeTree & read_from_merge_tree_step,
    const ActionsDAG::Node & node,
    const BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function || !node.function_base)
        return std::nullopt;

    if (!node.result_type->canBeUsedInBooleanContext())
        return std::nullopt;

    ActionsDAGWithInversionPushDown canonical_dag(&node, read_from_merge_tree_step.getContext(), /* boolean_context */ false);
    const auto & canonical_node = canonical_dag.predicate ? *canonical_dag.predicate : node;

    for (const auto & [_, info] : bloom_sliced_index_read_infos)
    {
        if (!info.is_materialized)
            continue;

        auto & condition = typeid_cast<MergeTreeIndexConditionBloomSliced &>(*info.index->condition_template->generateUnsubstituted());
        auto predicate = condition.createTokenPredicate(canonical_node, read_from_merge_tree_step.getContext());
        if (predicate)
        {
            return BloomSlicedHintPredicate{
                .index_name = info.index->index->index.name,
                .index_column_name = info.index->index->index.column_names.front(),
                .predicate = std::move(*predicate)};
        }
    }

    return std::nullopt;
}

void collectBloomSlicedHintPredicatesFromConjunction(
    ReadFromMergeTree & read_from_merge_tree_step,
    const ActionsDAG::Node & node,
    const BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos,
    std::vector<BloomSlicedHintPredicate> & predicates)
{
    if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base && node.function_base->getName() == "and")
    {
        for (const auto * child : node.children)
            collectBloomSlicedHintPredicatesFromConjunction(read_from_merge_tree_step, *child, bloom_sliced_index_read_infos, predicates);
        return;
    }

    if (auto predicate = tryCreateBloomSlicedTokenPredicate(read_from_merge_tree_step, node, bloom_sliced_index_read_infos))
        predicates.push_back(std::move(*predicate));
}

std::optional<BloomSlicedHintDAG> buildBloomSlicedHintDAG(
    ReadFromMergeTree & read_from_merge_tree_step,
    const std::vector<BloomSlicedHintPredicate> & predicates,
    const BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos)
{
    if (predicates.empty())
        return std::nullopt;

    BloomSlicedHintDAG result;
    std::unordered_map<String, const ActionsDAG::Node *> virtual_column_to_node;
    NameSet used_index_columns;

    for (const auto & predicate : predicates)
    {
        if (used_index_columns.contains(predicate.index_column_name))
            continue;

        auto info_it = bloom_sliced_index_read_infos.find(predicate.index_name);
        if (info_it == bloom_sliced_index_read_infos.end() || !info_it->second.is_materialized)
            continue;

        auto & condition = typeid_cast<MergeTreeIndexConditionBloomSliced &>(*info_it->second.index->condition_template->generateUnsubstituted());
        auto virtual_column_name = condition.replaceToVirtualColumn(predicate.predicate, predicate.index_name);
        if (virtual_column_to_node.contains(virtual_column_name))
            continue;

        VirtualColumnDescription virtual_column(
            virtual_column_name,
            std::make_shared<DataTypeUInt8>(),
            /*codec=*/ nullptr,
            predicate.index_name,
            VirtualsKind::Ephemeral,
            VirtualsMaterializationPlace::Reader);
        /// The fail-open default is load-bearing: whenever the reader does not produce the hint
        /// virtual column (the index is not materialized in a part, or patch parts turn the hint
        /// step into a mixed step that is not dispatched to the index reader), the column is
        /// default-filled with the literal 1 and every row passes the hint. Correctness then
        /// rests on the original predicate, which is always kept as a conjunct of the hint (see
        /// `prependBloomSlicedHintToPrewhereInfo` / `makeBloomSlicedHintPrewhereInfo` plus the
        /// plan-level filter). Do not replace this with an expression over the source column:
        /// that would re-introduce the text-direct-read patch-parts bug (dropped rows /
        /// UNKNOWN_IDENTIFIER for MATERIALIZED columns).
        virtual_column.default_desc.kind = ColumnDefaultKind::Default;
        virtual_column.default_desc.expression = make_intrusive<ASTLiteral>(Field(1));

        const auto & input = result.actions.addInput(virtual_column_name, std::make_shared<DataTypeUInt8>());
        virtual_column_to_node.emplace(virtual_column_name, &input);
        result.added_columns[predicate.index_name].add(std::move(virtual_column));
        used_index_columns.insert(predicate.index_column_name);
    }

    if (virtual_column_to_node.empty())
        return std::nullopt;

    ActionsDAG::NodeRawConstPtrs children;
    std::vector<String> virtual_column_names;
    virtual_column_names.reserve(virtual_column_to_node.size());
    for (const auto & [virtual_column_name, _] : virtual_column_to_node)
        virtual_column_names.push_back(virtual_column_name);
    std::ranges::sort(virtual_column_names);
    children.reserve(virtual_column_names.size());
    for (const auto & virtual_column_name : virtual_column_names)
        children.push_back(virtual_column_to_node.at(virtual_column_name));

    const ActionsDAG::Node * filter_node = nullptr;
    if (children.size() == 1)
    {
        filter_node = children.front();
    }
    else
    {
        auto function_builder = FunctionFactory::instance().get("and", read_from_merge_tree_step.getContext());
        filter_node = &result.actions.addFunction(function_builder, children, "");
    }

    result.filter_column_name = filter_node->result_name;
    result.actions.getOutputs().push_back(filter_node);

    auto logger = getLogger("processAndOptimizeBloomSlicedIndexFunctions");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(result.added_columns, {}));

    return result;
}

PrewhereInfoPtr makeBloomSlicedHintPrewhereInfo(BloomSlicedHintDAG hint)
{
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = std::move(hint.actions);
    prewhere_info->prewhere_column_name = std::move(hint.filter_column_name);
    prewhere_info->remove_prewhere_column = true;
    prewhere_info->need_filter = true;
    return prewhere_info;
}

PrewhereInfoPtr prependBloomSlicedHintToPrewhereInfo(
    ReadFromMergeTree & read_from_merge_tree_step,
    BloomSlicedHintDAG hint,
    const PrewhereInfoPtr & prewhere_info)
{
    auto result = std::make_shared<PrewhereInfo>();
    result->prewhere_actions = std::move(hint.actions);
    const auto * hint_filter_node = &result->prewhere_actions.findInOutputs(hint.filter_column_name);

    auto cloned_prewhere_info = prewhere_info->clone();
    const String original_filter_column_name = cloned_prewhere_info.prewhere_column_name;
    const bool original_remove_prewhere_column = cloned_prewhere_info.remove_prewhere_column;

    ActionsDAG::NodeRawConstPtrs original_outputs_in_combined;
    result->prewhere_actions.mergeNodes(std::move(cloned_prewhere_info.prewhere_actions), &original_outputs_in_combined);

    const ActionsDAG::Node * original_filter_node = nullptr;
    for (const auto * node : original_outputs_in_combined)
    {
        if (node->result_name == original_filter_column_name)
        {
            original_filter_node = node;
            break;
        }
    }

    if (!original_filter_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Original PREWHERE filter column {} not found after adding bloom_sliced hint", original_filter_column_name);

    auto & outputs = result->prewhere_actions.getOutputs();
    outputs.clear();
    for (const auto * node : original_outputs_in_combined)
        if (std::ranges::find(outputs, node) == outputs.end())
            outputs.push_back(node);

    if (original_remove_prewhere_column)
        std::erase(outputs, original_filter_node);

    auto function_builder = FunctionFactory::instance().get("and", read_from_merge_tree_step.getContext());
    const auto * combined_filter_node = &result->prewhere_actions.addFunction(function_builder, {hint_filter_node, original_filter_node}, "");
    outputs.push_back(combined_filter_node);

    result->prewhere_column_name = combined_filter_node->result_name;
    result->remove_prewhere_column = true;
    result->need_filter = prewhere_info->need_filter;
    return result;
}

bool addBloomSlicedHintReadTasks(ReadFromMergeTree & read_from_merge_tree_step, const IndexReadColumns & added_columns)
{
    if (added_columns.empty())
        return false;

    const auto & indexes = read_from_merge_tree_step.getIndexes();
    read_from_merge_tree_step.createReadTasksForTextIndex(indexes->skip_indexes, added_columns, {}, /*is_final=*/ false);
    return true;
}

bool processAndOptimizeBloomSlicedIndexFunctionsInPrewhere(
    ReadFromMergeTree & read_from_merge_tree_step,
    const PrewhereInfoPtr & prewhere_info,
    const BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos)
{
    const auto & filter_node = prewhere_info->prewhere_actions.findInOutputs(prewhere_info->prewhere_column_name);
    std::vector<BloomSlicedHintPredicate> predicates;
    collectBloomSlicedHintPredicatesFromConjunction(read_from_merge_tree_step, filter_node, bloom_sliced_index_read_infos, predicates);

    auto hint = buildBloomSlicedHintDAG(read_from_merge_tree_step, predicates, bloom_sliced_index_read_infos);
    if (!hint)
        return false;

    auto added_columns = hint->added_columns;
    auto modified_prewhere_info = prependBloomSlicedHintToPrewhereInfo(read_from_merge_tree_step, std::move(*hint), prewhere_info);
    addBloomSlicedHintReadTasks(read_from_merge_tree_step, added_columns);
    read_from_merge_tree_step.updatePrewhereInfo(modified_prewhere_info);
    return true;
}

const ActionsDAG::Node * processAndOptimizeBloomSlicedIndexFunctionsInWhere(
    ReadFromMergeTree & read_from_merge_tree_step,
    ActionsDAG & filter_dag,
    const BloomSlicedIndexReadInfos & bloom_sliced_index_read_infos,
    const String & filter_column_name)
{
    const auto & filter_node = filter_dag.findInOutputs(filter_column_name);
    std::vector<BloomSlicedHintPredicate> predicates;
    collectBloomSlicedHintPredicatesFromConjunction(read_from_merge_tree_step, filter_node, bloom_sliced_index_read_infos, predicates);

    auto hint = buildBloomSlicedHintDAG(read_from_merge_tree_step, predicates, bloom_sliced_index_read_infos);
    if (!hint)
        return nullptr;

    auto added_columns = hint->added_columns;

    /// The read step may already have a PrewhereInfo (e.g. an explicit user PREWHERE
    /// without token predicates). `updatePrewhereInfo` replaces the existing PrewhereInfo,
    /// so the hint must be prepended to it instead of overwriting it - otherwise the
    /// user predicate would be dropped from the plan and wrong rows would be returned.
    PrewhereInfoPtr hint_prewhere_info;
    if (auto existing_prewhere_info = read_from_merge_tree_step.getPrewhereInfo())
        hint_prewhere_info = prependBloomSlicedHintToPrewhereInfo(read_from_merge_tree_step, std::move(*hint), existing_prewhere_info);
    else
        hint_prewhere_info = makeBloomSlicedHintPrewhereInfo(std::move(*hint));

    addBloomSlicedHintReadTasks(read_from_merge_tree_step, added_columns);
    read_from_merge_tree_step.updatePrewhereInfo(hint_prewhere_info);

    return &filter_dag.findInOutputs(filter_column_name);
}

}

// Generic plan entry point.

/// Applies direct index-read and hint optimizations to the query plan.
///
/// Always preprocesses `hasAllTokens`/`hasAnyTokens` arguments with text index metadata
/// (preprocessor wrapping, string-to-array tokenization, tokenizer arguments).
///
/// When `direct_read_from_text_index` is true, also replaces text-search functions
/// with virtual columns for direct text-index reads. When
/// `direct_read_from_bloom_sliced_index` is true, may add `bloom_sliced` token
/// hint virtual columns as staged PREWHERE filters.
///
/// See `TextIndexDAGReplacer` and the `bloom_sliced` helpers above for more details.
void processAndOptimizeIndexFunctions(
    const Stack & stack,
    QueryPlan::Nodes & /*nodes*/,
    bool direct_read_from_text_index,
    bool direct_read_from_bloom_sliced_index)
{
    const auto & frame = stack.back();
    ReadFromMergeTree * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    TextIndexReadInfos text_index_read_infos;
    collectTextIndexReadInfos(read_from_merge_tree_step, text_index_read_infos);

    BloomSlicedIndexReadInfos bloom_sliced_index_read_infos;
    if (direct_read_from_bloom_sliced_index && text_index_read_infos.empty())
        collectBloomSlicedIndexReadInfos(read_from_merge_tree_step, bloom_sliced_index_read_infos);

    if (text_index_read_infos.empty() && bloom_sliced_index_read_infos.empty())
        return;

    bool optimized = false;
    if (auto prewhere_info = read_from_merge_tree_step->getPrewhereInfo())
    {
        if (!text_index_read_infos.empty())
            optimized = processAndOptimizeTextIndexFunctionsInPrewhere(*read_from_merge_tree_step, prewhere_info, text_index_read_infos, direct_read_from_text_index);
        else
            optimized = processAndOptimizeBloomSlicedIndexFunctionsInPrewhere(*read_from_merge_tree_step, prewhere_info, bloom_sliced_index_read_infos);
    }

    if (stack.size() < 2)
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());

    if (!filter_step)
        return;

    ActionsDAG & filter_dag = filter_step->getExpression();
    const ActionsDAG::Node * result_filter_node = nullptr;
    if (!text_index_read_infos.empty())
    {
        result_filter_node = processAndOptimizeTextIndexDAG(
            *read_from_merge_tree_step,
            filter_dag,
            text_index_read_infos,
            filter_step->getFilterColumnName(),
            direct_read_from_text_index && !optimized);
    }
    else if (!bloom_sliced_index_read_infos.empty() && !optimized)
    {
        result_filter_node = processAndOptimizeBloomSlicedIndexFunctionsInWhere(
            *read_from_merge_tree_step,
            filter_dag,
            bloom_sliced_index_read_infos,
            filter_step->getFilterColumnName());
    }

    if (!result_filter_node)
        return;

    bool removes_filter_column = filter_step->removesFilterColumn();
    auto new_filter_column_name = result_filter_node->result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
}

}
