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
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/defines.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

using NodesReplacementMap = absl::flat_hash_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

struct TextIndexReadInfo
{
    const MergeTreeIndexWithCondition * index;
    bool is_materialized;
    bool is_fully_materialized;
};

using TextIndexReadInfos = absl::flat_hash_map<String, TextIndexReadInfo>;

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

/// Helper function.
/// Collects index conditions from the given ReadFromMergeTree step and stores them in text_index_read_infos.
void collectTextIndexReadInfos(const ReadFromMergeTree * read_from_merge_tree_step, TextIndexReadInfos & text_index_read_infos)
{
    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || indexes->skip_indexes.useful_indices.empty())
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    auto metadata_snapshot = read_from_merge_tree_step->getStorageMetadata();
    auto mutations_snapshot = read_from_merge_tree_step->getMutationsSnapshot();
    auto context = read_from_merge_tree_step->getContext();

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

    /// Compute the union of updated columns only across the parts that will actually be read by this step.
    /// Using `mutations_snapshot->getAllUpdatedColumns()` directly would include pending updates from
    /// other partitions/parts not in `parts_with_ranges`, disabling direct text index reads even when
    /// the queried parts have no on-the-fly updates for the index columns.
    NameSet all_updated_columns;
    for (const auto & part : unique_parts)
    {
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
        );
        const auto & part_updated_columns = alter_conversions->getAllUpdatedColumns();
        all_updated_columns.insert(part_updated_columns.begin(), part_updated_columns.end());
    }

    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (!index.index->isTextIndex())
            continue;

        if (auto result = MergeTreeDataSelectExecutor::canUseIndex(index.index, metadata_snapshot, all_updated_columns); !result)
        {
            LOG_TRACE(logger, "Cannot use direct reading from text index. Reason: {}", result.error().text);
            continue;
        }

        /// Index may be not materialized in some parts, e.g. after ALTER ADD INDEX query.
        size_t num_materialized_parts = std::ranges::count_if(unique_parts, [&](const auto & part)
        {
            return !!index.index->getDeserializedFormat(part->checksums, index.index->getFileName(), &part->getDataPartStorage());
        });

        text_index_read_infos[index.index->index.name] =
        {
            .index = &index,
            .is_materialized = num_materialized_parts > 0,
            .is_fully_materialized = num_materialized_parts == unique_parts.size()
        };
    }
}

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
/// It allows their stadalone executions without the direct read from text index.
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
        const TextIndexReadInfo * info = nullptr;
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
            if (search_query->getDirectReadMode() == TextIndexDirectReadMode::None)
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
            if (condition.search_query->getDirectReadMode() != TextIndexDirectReadMode::None)
                selected_conditions.push_back(condition);
        }
        if (selected_conditions.empty())
            return;

        bool has_exact_search = false;
        bool has_materialized_index = false;

        for (const auto & condition : selected_conditions)
        {
            has_materialized_index |= condition.info->is_materialized;
            has_exact_search |= condition.search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact;
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

                if (condition.search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact)
                    default_expression = convertNodeToAST(function_node);
                /// Do not execute the default expression for hint mode, because it will be executed anyway in the original predicate.
                else if (condition.search_query->getDirectReadMode() == TextIndexDirectReadMode::Hint)
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

/// Applies text index optimizations to the query plan.
///
/// Always preprocesses `hasAllTokens`/`hasAnyTokens` arguments with text index metadata
/// (preprocessor wrapping, string-to-array tokenization, tokenizer arguments).
///
/// When `direct_read_from_text_index` is true, also replaces text-search functions
/// with virtual columns for direct index reads (both WHERE and PREWHERE clauses).
///
/// See TextIndexDAGReplacer class for more details.
void processAndOptimizeTextIndexFunctions(const Stack & stack, QueryPlan::Nodes & /*nodes*/, bool direct_read_from_text_index)
{
    const auto & frame = stack.back();
    ReadFromMergeTree * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    TextIndexReadInfos text_index_read_infos;
    collectTextIndexReadInfos(read_from_merge_tree_step, text_index_read_infos);
    if (text_index_read_infos.empty())
        return;

    bool optimized = false;
    if (auto prewhere_info = read_from_merge_tree_step->getPrewhereInfo())
        optimized = processAndOptimizeTextIndexFunctionsInPrewhere(*read_from_merge_tree_step, prewhere_info, text_index_read_infos, direct_read_from_text_index);

    if (stack.size() < 2)
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());

    if (!filter_step)
        return;

    ActionsDAG & filter_dag = filter_step->getExpression();
    const auto * result_filter_node = processAndOptimizeTextIndexDAG(*read_from_merge_tree_step, filter_dag, text_index_read_infos, filter_step->getFilterColumnName(), direct_read_from_text_index && !optimized);

    if (!result_filter_node)
        return;

    bool removes_filter_column = filter_step->removesFilterColumn();
    auto new_filter_column_name = result_filter_node->result_name;
    filter_node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
}

}
