#include <Access/ContextAccess.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTopKFilter.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Functions/bm25.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Core/Settings.h>
#include <base/defines.h>
#include <absl/container/flat_hash_set.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace DB::Setting
{
    extern const SettingsBool allow_experimental_bm25_scoring;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

using NodesReplacementMap = absl::flat_hash_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

struct TextIndexReadInfo
{
    MergeTreeIndexConditionPtr condition;
    const MergeTreeIndexWithCondition * index = nullptr;
    /// Keeps the index helper alive, `condition` holds a non-owning pointer into its tokenizer.
    MergeTreeIndexPtr index_helper = nullptr;
    bool is_materialized = false;
    bool is_fully_materialized = false;
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

    /// Render a constant by value: its result_name can differ between the query and preprocessor DAGs,
    /// so comparing by value keeps the haystack and preprocessor names consistent.
    if (node->type == ActionsDAG::ActionType::COLUMN && node->column)
        return applyVisitor(FieldVisitorToString(), node->column->getField());

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

/// Collects the result names of every node in a filter DAG, to test whether a predicate is part of it.
void collectNodeNames(const ActionsDAG::Node * node, NameSet & names)
{
    if (!node || !names.emplace(node->result_name).second)
        return;

    for (const auto * child : node->children)
        collectNodeNames(child, names);
}

/// A function above a replaced node is rebuilt under a name derived from its new arguments, or with `keep_names`
/// under the name of the original node.
const ActionsDAG::Node * replaceNodes(ActionsDAG & dag, const ActionsDAG::Node * node, const NodesReplacementMap & replacements, bool keep_names = false)
{
    if (auto it = replacements.find(node); it != replacements.end())
    {
        return it->second;
    }
    else if (node->type == ActionsDAG::ActionType::ALIAS)
    {
        const auto * old_child = node->children[0];
        const auto * new_child = replaceNodes(dag, old_child, replacements, keep_names);

        if (old_child != new_child)
            return &dag.addAlias(*new_child, node->result_name);
    }
    else if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        auto old_children = node->children;
        std::vector<const ActionsDAG::Node *> new_children;

        for (const auto & child : old_children)
            new_children.push_back(replaceNodes(dag, child, replacements, keep_names));

        if (new_children != old_children)
            return &dag.addFunction(node->function_base, new_children, keep_names ? node->result_name : "");
    }

    return node;
}

String optimizationInfoToString(const IndexReadTasks & index_read_tasks, const Names & removed_columns)
{
    chassert(!index_read_tasks.empty());

    String result = "Added: [";

    /// This will list the index and the new associated columns
    size_t idx = 0;
    for (const auto & [_, index_read_task] : index_read_tasks)
    {
        for (const auto & added_virtual_column : index_read_task.columns)
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

/// The `bm25()` calls of the plan fragment above a read step. They share one parameter pair, and the
/// pass replaces all of them by one assembled score expression, output under each call's result name.
struct BM25Rewrite
{
    /// Indexes into `result_names_by_dag`: the PREWHERE of the read step, then the steps of the walk, the first of them
    /// being the WHERE filter.
    static constexpr size_t PREWHERE_DAG = 0;
    static constexpr size_t WHERE_DAG = 1;

    BM25Params params;
    /// The result names of the calls in each DAG of the fragment, bottom-up.
    std::vector<NameOrderedSet> result_names_by_dag;
    /// A filter condition (of the PREWHERE or a filter step) calls `bm25()`.
    bool is_in_filter_condition = false;
    /// The filter assembling the score takes it as a column computed ahead of the PREWHERE: for the dynamic top-k filter
    /// of `ORDER BY bm25() LIMIT n` (see `installBM25TopKFilter`), or for a PREWHERE that calls `bm25()` without
    /// having a scoring predicate (see `addBM25ScoreToPrewhere`).
    bool assemble_in_prewhere = false;

    /// The result names of the calls in the DAGs above `dag`: the step computing the score outputs them, and the
    /// steps in between pass them through.
    NameOrderedSet getResultNamesAbove(size_t dag) const
    {
        NameOrderedSet result;
        for (size_t i = dag + 1; i < result_names_by_dag.size(); ++i)
            result.insert(result_names_by_dag[i].begin(), result_names_by_dag[i].end());
        return result;
    }

    bool hasResultName(const String & name) const
    {
        return std::ranges::any_of(result_names_by_dag, [&](const auto & names) { return names.contains(name); });
    }
};

bool isBM25FunctionNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION && node.function_base && node.function_base->getName() == "bm25";
}

BM25Params parseBM25Node(const ActionsDAG::Node & node)
{
    ColumnsWithTypeAndName arguments;
    for (const auto * child : node.children)
    {
        if (child->type != ActionsDAG::ActionType::COLUMN || !child->column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arguments of function bm25 must be constants");

        arguments.emplace_back(child->column, child->result_type, child->result_name);
    }

    return parseBM25FunctionArguments(arguments);
}

ActionsDAG::NodeRawConstPtrs collectBM25Nodes(const ActionsDAG & dag)
{
    ActionsDAG::NodeRawConstPtrs result;
    for (const auto & node : dag.getNodes())
    {
        if (isBM25FunctionNode(node))
            result.push_back(&node);
    }

    return result;
}

/// Whether the filter condition `filter_column_name` of `dag` calls `bm25()`.
bool filterConditionCallsBM25(const ActionsDAG & dag, const String & filter_column_name)
{
    std::vector<const ActionsDAG::Node *> to_visit{&dag.findInOutputs(filter_column_name)};
    absl::flat_hash_set<const ActionsDAG::Node *> visited;

    while (!to_visit.empty())
    {
        const auto * node = to_visit.back();
        to_visit.pop_back();

        if (!visited.insert(node).second)
            continue;

        if (isBM25FunctionNode(*node))
            return true;

        for (const auto * child : node->children)
            to_visit.push_back(child);
    }

    return false;
}

/// Adds the result names of the `bm25()` calls of `dag` to `result_names`, rejecting a second parameter pair.
void addBM25Nodes(const ActionsDAG & dag, std::optional<BM25Params> & params, NameOrderedSet & result_names)
{
    for (const auto * node : collectBM25Nodes(dag))
    {
        auto node_params = parseBM25Node(*node);

        if (!params)
        {
            params = node_params;
        }
        else if (params->k1 != node_params.k1 || params->b != node_params.b)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function bm25 is used with two different parameter pairs in one query, (k1 = {}, b = {}) and (k1 = {}, b = {}), which is not supported",
                params->k1, params->b, node_params.k1, node_params.b);
        }

        result_names.insert(node->result_name);
    }
}

/// Replaces the `bm25()` calls of `dag` (a step above the one computing the score) by inputs of the same name,
/// and passes through those of `result_names_above`. Returns false if there is no call.
/// The outputs keep their names: the step's filter column and the steps above refer to them, and the name of a
/// filter condition need not follow from its arguments, since `FilterStep` strips the aliases below it
/// (e.g. `greater(__table1.score, 1_UInt8)` over `bm25()`).
bool replaceBM25NodesWithInputs(ActionsDAG & dag, const NameOrderedSet & result_names_above)
{
    auto nodes = collectBM25Nodes(dag);
    if (nodes.empty())
        return false;

    NodesReplacementMap replacements;
    std::map<String, const ActionsDAG::Node *> inputs_by_name;

    for (const auto * node : nodes)
    {
        auto [it, inserted] = inputs_by_name.try_emplace(node->result_name);
        if (inserted)
            it->second = &dag.addInput(node->result_name, node->result_type);

        replacements[node] = it->second;
    }

    auto & outputs = dag.getOutputs();
    for (auto & output : outputs)
        output = replaceNodes(dag, output, replacements, /*keep_names=*/ true);

    for (const auto & [name, input] : inputs_by_name)
    {
        if (result_names_above.contains(name) && std::ranges::find(outputs, input) == outputs.end())
            outputs.push_back(input);
    }

    dag.removeUnusedActions(/*allow_remove_inputs=*/ false);
    return true;
}

/// Columns changed on the fly by patches, mutations and masking policies. The index is not used for them in the
/// whole query: the preprocessor rewrite and the on-fly mutation steps do not depend on the part. They are taken
/// from the whole snapshot to avoid looking up the patches of each part.
NameSet getColumnsUpdatedOnFly(const ReadFromMergeTree & read_from_merge_tree_step)
{
    NameSet updated_columns = read_from_merge_tree_step.getMutationsSnapshot()->getColumnsChangedOnFly();

#if CLICKHOUSE_CLOUD
    /// Masking policies are the same for all parts.
    const auto & context = read_from_merge_tree_step.getContext();
    const auto & storage_id = read_from_merge_tree_step.getMergeTreeData().getStorageID();

    for (const auto & command : MergeTreeData::getMaskingPolicyCommands(storage_id, context->getAccess()->getEnabledMaskingPolicies()))
        AlterConversions::addUpdatedColumns(command, updated_columns);
#endif

    return updated_columns;
}

/// Helper function.
/// Collects index conditions from the given ReadFromMergeTree step and stores them in text_index_read_infos.
void collectTextIndexReadInfos(const ReadFromMergeTree * read_from_merge_tree_step, TextIndexReadInfos & text_index_read_infos)
{
    auto component_guard = Coordination::setCurrentComponent("optimizeDirectReadFromTextIndex");

    /// Everything below is needed only for text indexes.
    const auto & indexes = read_from_merge_tree_step->getIndexes();
    if (!indexes || std::ranges::none_of(indexes->skip_indexes.useful_indices, [](const auto & index) { return index.index->isTextIndex(); }))
        return;

    const RangesInDataParts & parts_with_ranges = read_from_merge_tree_step->getParts();
    if (parts_with_ranges.empty())
        return;

    auto logger = getLogger("optimizeDirectReadFromTextIndex");
    auto metadata_snapshot = read_from_merge_tree_step->getStorageMetadata();

    std::unordered_set<DataPartPtr> unique_parts;
    for (const auto & part : parts_with_ranges)
        unique_parts.insert(part.data_part);

    auto updated_columns = getColumnsUpdatedOnFly(*read_from_merge_tree_step);

    for (const auto & index : indexes->skip_indexes.useful_indices)
    {
        if (!index.index->isTextIndex())
            continue;

        if (auto result = MergeTreeDataSelectExecutor::canUseIndex(index.index, metadata_snapshot, updated_columns); !result)
        {
            LOG_TRACE(logger, "Cannot use direct reading from text index. Reason: {}", result.error().text);
            continue;
        }

        /// Index may be not materialized in some parts, e.g. after ALTER ADD INDEX query.
        size_t num_materialized_parts = std::ranges::count_if(unique_parts, [&](const auto & part)
        {
            return !!index.index->getDeserializedFormat(*part, index.index->getFileName());
        });

        text_index_read_infos[index.index->index.name] =
        {
            .condition = index.condition_template->generateUnsubstituted(),
            .index = &index,
            .is_materialized = num_materialized_parts > 0,
            .is_fully_materialized = num_materialized_parts == unique_parts.size(),
        };
    }
}

/// Adds an inject-only condition (from the index definition, null predicate) for every text index not already
/// collected as a direct-read entry, so the rewrite covers all text indexes even without skip-index analysis.
void collectTextIndexInjectInfos(const ReadFromMergeTree * read_from_merge_tree_step, TextIndexReadInfos & text_index_infos)
{
    auto metadata_snapshot = read_from_merge_tree_step->getStorageMetadata();
    const auto & all_indexes = metadata_snapshot->getSecondaryIndices();
    if (all_indexes.empty())
        return;

    const auto & data = read_from_merge_tree_step->getMergeTreeData();
    auto context = read_from_merge_tree_step->getContext();
    const auto & merge_tree_settings = *data.getSettings();

    for (const auto & index_description : all_indexes)
    {
        /// Cheap pre-filter to skip constructing helpers for unrelated secondary indexes.
        if (index_description.type != "text")
            continue;

        /// Already collected as a direct-read entry (from useful_indices); keep its analyzed condition.
        if (text_index_infos.contains(index_description.name))
            continue;

        auto index_helper = MergeTreeIndexFactory::instance().get(metadata_snapshot, index_description, merge_tree_settings);
        if (!index_helper->isTextIndex())
            continue;

        auto condition = index_helper->createIndexCondition(/*predicate=*/ nullptr, context);
        text_index_infos[index_description.name] = TextIndexReadInfo
        {
            .condition = std::move(condition),
            .index_helper = std::move(index_helper),
        };
    }
}

ASTPtr convertSetColumnToAST(const IColumn & column)
{
    const auto * column_set = checkAndGetColumnConstData<const ColumnSet>(&column);
    if (!column_set)
        column_set = checkAndGetColumn<const ColumnSet>(&column);
    if (!column_set)
        return nullptr;

    auto future_set = column_set->getData();
    const auto * set_from_tuple = dynamic_cast<const FutureSetFromTuple *>(future_set.get());
    if (!set_from_tuple)
        return nullptr;

    const Columns key_columns = set_from_tuple->getKeyColumns();
    if (key_columns.empty() || key_columns.front()->empty())
        return nullptr;

    const size_t num_elements = key_columns.front()->size();
    auto elements = makeASTFunction("tuple");
    elements->arguments->children.reserve(num_elements);

    for (size_t i = 0; i < num_elements; ++i)
    {
        if (key_columns.size() == 1)
        {
            elements->arguments->children.push_back(make_intrusive<ASTLiteral>((*key_columns.front())[i]));
            continue;
        }

        /// A set over a tuple left-hand side, e.g. `(a, b) IN ((1, 2), (3, 4))`.
        Tuple key;
        key.reserve(key_columns.size());
        for (const auto & key_column : key_columns)
            key.push_back((*key_column)[i]);
        elements->arguments->children.push_back(make_intrusive<ASTLiteral>(Field(std::move(key))));
    }

    return elements;
}

/// Converts an ActionsDAG node to an AST node.
/// It is not correct in the general case, but is
/// sufficient for expressions that can be used with a text index.
/// Returns `nullptr` if any part has no AST representation: a partial conversion would change the meaning.
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

    auto body = convertNodeToAST(*capture_dag.getOutputs().front(), body_captured);
    if (!body)
        return nullptr;

    lambda->arguments->children.push_back(std::move(body));
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
        {
            if (!node.column)
                return nullptr;

            /// A `Set` column has no `Field`, so emitting it as a literal would give `x IN NULL`.
            if (WhichDataType(node.result_type).isSet())
                return convertSetColumnToAST(*node.column);

            return make_intrusive<ASTLiteral>((*node.column)[0]);
        }

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
                auto arg_ast = convertNodeToAST(*child, captured);
                if (!arg_ast)
                    return nullptr;

                function->arguments->children.push_back(std::move(arg_ast));
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
    TextIndexDAGReplacer(
        ActionsDAG & actions_dag_,
        const TextIndexReadInfos & text_index_read_infos_,
        bool direct_read_from_text_index_,
        bool is_filter_dag_,
        bool require_index_analyzed_predicate_ = false,
        NameSet * scoring_predicate_indexes_ = nullptr,
        const BM25Rewrite * bm25_rewrite_ = nullptr,
        NameOrderedSet bm25_result_names_above_ = {})
        : actions_dag(actions_dag_)
        , text_index_read_infos(text_index_read_infos_)
        , direct_read_from_text_index(direct_read_from_text_index_)
        , is_filter_dag(is_filter_dag_)
        , require_index_analyzed_predicate(require_index_analyzed_predicate_)
        , scoring_predicate_indexes(scoring_predicate_indexes_)
        , bm25_rewrite(bm25_rewrite_)
        , bm25_result_names_above(std::move(bm25_result_names_above_))
    {
        /// Register the text-index virtual column inputs that are already present in this DAG from a previous
        /// optimization pass. This prevents them from being re-added to `index_read_tasks` when the same DAG is processed again.
        for (const auto * input : actions_dag.getInputs())
        {
            if (input->result_name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX))
                virtual_column_inputs.emplace(input->result_name, input);
        }
    }

    /// The `bm25()` score assembled over the score columns of the scoring predicates of a filter DAG.
    struct BM25Assembly
    {
        /// The score node (`Float32`) inside the rewritten DAG, null when the filter has no scoring predicate on
        /// the direct-read path. Valid only while that DAG lives: the steps are rebuilt from clones.
        const ActionsDAG::Node * score = nullptr;
        /// A standalone copy of the score expression (its single output) over the read step's columns.
        std::shared_ptr<const ActionsDAG> score_dag;
        String index_name;
        /// Occurrences of scoring predicates in the score expression.
        size_t num_predicates = 0;
        /// The score columns the expression reads.
        NameSet score_columns;

        bool isAssembled() const { return num_predicates > 0; }
    };

    struct ResultReplacement
    {
        IndexReadTasks index_read_tasks;
        Names removed_columns;
        const ActionsDAG::Node * filter_node = nullptr;
        BM25Assembly bm25;
    };

    /// Replaces text-search functions by virtual columns.
    /// Example: hasToken(text_col, 'token') -> __text_index_text_col_idx_hasToken_0.
    ///
    /// Applies preprocessor, tokenizer and postprocessor in chain for text-search functions.
    /// Example: hasAllTokens(text_col, 'token1 token2') -> hasAllTokens(lower(text_col), ['token1', 'token2'], 'splitByNonAlpha').
    /// Pass an empty `filter_column_name` for DAGs without a single filter output (e.g. a SELECT-list ExpressionStep).
    /// Returns `std::nullopt` if no function node was rewritten.
    std::optional<ResultReplacement> replace(const ContextPtr & context, const String & filter_column_name)
    {
        NodesReplacementMap replacements;
        Names original_inputs = actions_dag.getRequiredColumnsNames();
        const auto * filter_node = filter_column_name.empty() ? nullptr : &actions_dag.findInOutputs(filter_column_name);

        /// Nodes participating in the filter condition, for collecting scoring predicates: a text
        /// function this DAG computes only for the SELECT list does not filter rows and must not count.
        absl::flat_hash_set<const ActionsDAG::Node *> filter_condition_nodes;
        if (scoring_predicate_indexes && filter_node)
        {
            std::vector<const ActionsDAG::Node *> to_visit{filter_node};
            while (!to_visit.empty())
            {
                const auto * visited = to_visit.back();
                to_visit.pop_back();

                if (!filter_condition_nodes.insert(visited).second)
                    continue;

                for (const auto * child : visited->children)
                    to_visit.push_back(child);
            }
        }

        /// Copy pointers to nodes to avoid the modification of nodes in the dag while iterating over them.
        auto nodes_ptrs = actions_dag.getNodesPointers();

        for (const auto * node : nodes_ptrs)
        {
            const bool collect_scoring_predicates = scoring_predicate_indexes && (!filter_node || filter_condition_nodes.contains(node));
            const auto * replaced_node = processFunctionNode(*node, context, collect_scoring_predicates);
            if (replaced_node != node)
                replacements[node] = replaced_node;
        }

        if (replacements.empty())
            return std::nullopt;

        for (auto & output : actions_dag.outputs)
        {
            bool is_filter_node = output == filter_node;
            output = replaceNodes(actions_dag, output, replacements);

            if (is_filter_node)
                filter_node = output;
        }

        /// Assemble `bm25()` over the rewritten filter. It is built here so that the score columns survive
        /// `removeUnusedActions`. Unless the score is computed ahead of the PREWHERE, this DAG replaces the calls
        /// by the score and outputs it under the names of the calls above; otherwise the calls of this DAG
        /// become inputs of the same name.
        BM25Assembly bm25;
        if (bm25_rewrite && filter_node)
        {
            bm25.score = assembleScore(filter_node, /*on_root_conjunction=*/ true, bm25, context);

            if (bm25.score)
            {
                if (!WhichDataType(bm25.score->result_type).isFloat32())
                    bm25.score = &actions_dag.addCast(*bm25.score, std::make_shared<DataTypeFloat32>(), "", context);

                /// The steps are rebuilt from clones of their DAGs, so the score node does not outlive this DAG.
                bm25.score_dag = std::make_shared<const ActionsDAG>(ActionsDAG::cloneSubDAG({bm25.score}, /*remove_aliases=*/ false));

                /// A condition on the score rebuilds the filter output in place: find it again afterwards.
                const size_t filter_output_position = std::ranges::find(actions_dag.outputs, filter_node) - actions_dag.outputs.begin();
                chassert(filter_output_position < actions_dag.outputs.size());

                if (bm25_rewrite->assemble_in_prewhere)
                {
                    replaceBM25NodesWithInputs(actions_dag, bm25_result_names_above);
                }
                else
                {
                    NodesReplacementMap bm25_replacements;
                    std::unordered_map<String, const ActionsDAG::Node *> aliases;

                    for (const auto * node : collectBM25Nodes(actions_dag))
                    {
                        auto [it, inserted] = aliases.try_emplace(node->result_name);
                        if (inserted)
                            it->second = &actions_dag.addAlias(*bm25.score, node->result_name);
                        bm25_replacements[node] = it->second;
                    }

                    /// The steps above may read a condition on the score by its name (see `replaceBM25NodesWithInputs`).
                    for (auto & output : actions_dag.outputs)
                        output = replaceNodes(actions_dag, output, bm25_replacements, /*keep_names=*/ true);

                    for (const auto & name : bm25_result_names_above)
                    {
                        bool is_output = std::ranges::any_of(actions_dag.outputs, [&](const auto * output) { return output->result_name == name; });
                        if (is_output)
                            continue;

                        auto [it, inserted] = aliases.try_emplace(name);
                        if (inserted)
                            it->second = &actions_dag.addAlias(*bm25.score, name);
                        actions_dag.outputs.push_back(it->second);
                    }
                }

                filter_node = actions_dag.outputs[filter_output_position];
            }
        }

        actions_dag.removeUnusedActions();

        ResultReplacement result;
        result.filter_node = filter_node;
        result.bm25 = std::move(bm25);

        Names replaced_columns = actions_dag.getRequiredColumnsNames();
        NameSet replaced_columns_set(replaced_columns.begin(), replaced_columns.end());

        for (const auto & column : original_inputs)
        {
            if (!replaced_columns_set.contains(column))
                result.removed_columns.push_back(column);
        }

        /// A virtual column is read only if its input survived `removeUnusedActions`: the rewrite can
        /// keep a different index's virtual (or the original expression) instead, leaving this one unused.
        /// The score columns of the assembled `bm25()` are read even when the PREWHERE computes it instead of this DAG.
        for (auto & [index_name, index_read_task] : index_read_tasks)
        {
            std::erase_if(index_read_task.columns, [&](const auto & column)
            {
                return !replaced_columns_set.contains(column.name) && !result.bm25.score_columns.contains(column.name);
            });

            if (!index_read_task.columns.empty())
                result.index_read_tasks.emplace(index_name, std::move(index_read_task));
        }

        return result;
    }

private:
    /// A scoring predicate replaced by a match column: its score column input and search query.
    struct ScoringLeaf
    {
        const ActionsDAG::Node * score_input = nullptr;
        TextSearchQueryPtr search_query;
        String index_name;
    };

    /// A text index selected to process a text-search function.
    struct SelectedCondition
    {
        String index_name;
        const TextIndexReadInfo * info = nullptr;
        /// Whether this predicate participated in skip-index analysis (always true unless `require_index_analyzed_predicate`).
        bool is_index_analyzed = true;
        /// The virtual column replacing the function. An empty name means the inject-only rewrite (no direct read).
        IndexReadTask::Column virtual_column;
    };

    ActionsDAG & actions_dag;
    TextIndexReadInfos text_index_read_infos;
    bool direct_read_from_text_index = false;
    /// True while rewriting a WHERE/PREWHERE filter DAG; false for SELECT-list / above-scan rewrites.
    bool is_filter_dag = false;
    /// True while rewriting a DAG excluded from index analysis (a PREWHERE deferred after FINAL).
    bool require_index_analyzed_predicate = false;
    /// When set, collects the names of scoring-enabled text indexes that have a scoring-eligible
    /// text-search predicate (`hasToken` / `hasAnyTokens` / `hasAllTokens`) in this filter DAG.
    NameSet * scoring_predicate_indexes = nullptr;
    /// When set, the query computes `bm25()`: the scoring predicates replaced for the direct read also get
    /// score columns, and the score is assembled over the filter (see `assembleScore`).
    const BM25Rewrite * bm25_rewrite = nullptr;
    /// The result names of the `bm25()` calls in the steps above this DAG, which it outputs if it assembles the score.
    NameOrderedSet bm25_result_names_above;
    /// Per-index cache of the node names in the index-analysis filter DAG.
    std::unordered_map<String, NameSet> index_analyzed_predicate_names;
    /// Input nodes of the virtual columns in the DAG, by column name.
    std::unordered_map<String, const ActionsDAG::Node *> virtual_column_inputs;
    /// Read tasks of the virtual columns added to the DAG, by index name. `replace` keeps only the columns still used after the rewrite.
    IndexReadTasks index_read_tasks;
    /// The nodes of the rewritten filter that stand for scoring predicates.
    absl::flat_hash_map<const ActionsDAG::Node *, ScoringLeaf> scoring_leaves;
    /// Whether a node of the rewritten filter depends on a `bm25()` call.
    absl::flat_hash_map<const ActionsDAG::Node *, bool> depends_on_bm25;
    const ActionsDAG::Node * float32_zero = nullptr;

    /// True if index analysis saw this exact predicate, i.e. it also appears in a filter that was not deferred.
    bool isIndexAnalyzedPredicate(const String & index_name, const TextIndexReadInfo & info, const ActionsDAG::Node & predicate)
    {
        if (!info.index)
            return false;

        auto it = index_analyzed_predicate_names.find(index_name);
        if (it == index_analyzed_predicate_names.end())
        {
            NameSet names;
            if (const auto * filter_dag = info.index->condition_template->getFilterDAG())
                collectNodeNames(filter_dag->predicate, names);

            it = index_analyzed_predicate_names.emplace(index_name, std::move(names)).first;
        }

        return it->second.contains(predicate.result_name);
    }

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

    std::vector<SelectedCondition> selectConditions(const ActionsDAG::Node & function_node, const ContextPtr & context, bool collect_scoring_predicates)
    {
        /// Canonicalize the function-node subtree so that the serialized column names
        /// fed to MergeTreeIndexConditionText::traverseFunctionNode match the ones
        /// produced when the condition was originally constructed in ReadFromMergeTree::applyFilters.
        ActionsDAGWithInversionPushDown canonical_dag(&function_node, context, /* boolean_context */ false);
        const auto & canonical_node = canonical_dag.predicate ? *canonical_dag.predicate : function_node;

        /// The index is analyzed under a `CAST` that drops `Nullable` and throws on NULL. Direct read replaces or
        /// short-circuits the predicate, so `NOT hasToken(CAST(s, 'String'), 'a')` would return the NULL row
        /// instead of throwing. Use the index only to skip granules then.
        const bool drops_nullable = std::ranges::any_of(canonical_node.children, [](const auto * argument)
        {
            return unwrapLosslessConversion(argument, /*allow_drop_nullable=*/ false) != unwrapLosslessConversion(argument);
        });

        NameSet used_index_columns;
        std::vector<SelectedCondition> selected_conditions;

        for (const auto & [index_name, info] : text_index_read_infos)
        {
            const auto & text_index_condition = typeid_cast<const MergeTreeIndexConditionText &>(*info.condition);
            const auto & index_header = text_index_condition.getHeader();

            /// Take the first text index if there are multiple text indexes set for the same expression.
            /// It is ambiguous which index to use. However, we allow to use several indexes for different expressions.
            /// for example, we can use indexes both for mapKeys(m) and mapValues(m) in one function m['key'] = 'value'.
            if (index_header.columns() != 1 || used_index_columns.contains(index_header.begin()->name))
                continue;

            auto search_query = text_index_condition.createTextSearchQuery(canonical_node);
            if (!search_query)
                continue;

            /// The search query is built from the canonicalized subtree, but the rewrites below apply to
            /// the original node, so check that node as well.
            if (!text_index_condition.canAnswerFunctionNode(function_node))
                continue;

            SelectedCondition condition
            {
                .index_name = index_name,
                .info = &info,
                .is_index_analyzed = !require_index_analyzed_predicate || isIndexAnalyzedPredicate(index_name, info, canonical_node),
                .virtual_column = {},
            };

            /// Use direct read only when enabled and the entry is direct-read-eligible (has `index`). Otherwise, or if
            /// the query has no virtual column, just inject the tokenizer/preprocessor/postprocessor, same as None mode.
            if (direct_read_from_text_index
                && info.index
                && !drops_nullable
                && search_query->getDirectReadMode() != TextIndexDirectReadMode::None)
            {
                if (auto virtual_column_name = text_index_condition.tryGetVirtualColumnName(*search_query, index_name))
                    condition.virtual_column = {*virtual_column_name, std::make_shared<DataTypeUInt8>(), search_query, nullptr};
            }

            if (collect_scoring_predicates)
                collectScoringPredicateIndex(index_name, info, *search_query);

            selected_conditions.push_back(std::move(condition));
            used_index_columns.insert(index_header.begin()->name);
        }

        return selected_conditions;
    }

    /// Records the scoring-enabled text index of a scoring-eligible predicate that participates in the
    /// filter condition, regardless of whether the predicate is rewritten for the direct read.
    void collectScoringPredicateIndex(const String & index_name, const TextIndexReadInfo & info, const TextSearchQuery & search_query) const
    {
        const auto & function_name = search_query.getFunctionName();
        if (function_name != "hasToken" && function_name != "hasAnyTokens" && function_name != "hasAllTokens")
            return;

        if (search_query.getTokens().empty())
            return;

        const IMergeTreeIndex * index_ptr = info.index ? info.index->index.get() : info.index_helper.get();
        const auto * text_index = typeid_cast<const MergeTreeIndexText *>(index_ptr);

        if (text_index && text_index->getParams().hasScoring())
            scoring_predicate_indexes->insert(index_name);
    }

    /// Returns the node replacing `function_node`, or `function_node` itself if it is not rewritten.
    const ActionsDAG::Node * processFunctionNode(const ActionsDAG::Node & function_node, const ContextPtr & context, bool collect_scoring_predicates)
    {
        if (function_node.type != ActionsDAG::ActionType::FUNCTION || !function_node.function || !function_node.function_base)
            return &function_node;

        /// Skip if function is not a predicate. It doesn't make sense to analyze it.
        if (!function_node.result_type->canBeUsedInBooleanContext())
            return &function_node;

        auto function_name = function_node.function_base->getName();
        bool need_transform_function = needApplyTokenizer(function_name) || needApplyPreprocessor(function_name);

        /// Early exit if there is nothing to process.
        if (!need_transform_function && !direct_read_from_text_index && !collect_scoring_predicates)
            return &function_node;

        auto selected_conditions = selectConditions(function_node, context, collect_scoring_predicates);
        if (selected_conditions.empty())
            return &function_node;

        const ActionsDAG::Node * node = &function_node;

        if (need_transform_function)
            node = processTextIndexFunction(*node, selected_conditions, context);

        if (direct_read_from_text_index)
            node = replaceFunctionsToVirtualColumns(*node, std::move(selected_conditions), context);

        return node;
    }

    /// Whether the direct read of the predicate gives a BM25 score: one of the three scoring functions,
    /// read exactly from a text index with `scoring = 'bm25'`.
    static bool isScoringCondition(const SelectedCondition & condition)
    {
        const auto & search_query = *condition.virtual_column.search_query;
        const auto & function_name = search_query.getFunctionName();
        if (function_name != "hasToken" && function_name != "hasAnyTokens" && function_name != "hasAllTokens")
            return false;

        if (search_query.getDirectReadMode() != TextIndexDirectReadMode::Exact || search_query.getTokens().empty())
            return false;

        const auto * text_index = condition.info->index ? typeid_cast<const MergeTreeIndexText *>(condition.info->index->index.get()) : nullptr;
        return text_index && text_index->getParams().hasScoring();
    }

    const ActionsDAG::Node & getFloat32Zero()
    {
        if (!float32_zero)
        {
            auto type = std::make_shared<DataTypeFloat32>();
            float32_zero = &actions_dag.addColumn(type->createColumnConst(1, Float32(0)), type, "_CAST(0, 'Float32')");
        }

        return *float32_zero;
    }

    bool dependsOnBM25(const ActionsDAG::Node * node)
    {
        if (auto it = depends_on_bm25.find(node); it != depends_on_bm25.end())
            return it->second;

        bool result = isBM25FunctionNode(*node) || std::ranges::any_of(node->children, [&](const auto * child) { return dependsOnBM25(child); });
        depends_on_bm25.emplace(node, result);
        return result;
    }

    /// The condition at `node` without the conditions that call `bm25()`, removed from its conjunctions and
    /// disjunctions. Null when nothing remains.
    const ActionsDAG::Node * removeBM25Conditions(const ActionsDAG::Node * node, const ContextPtr & context)
    {
        if (!dependsOnBM25(node))
            return node;

        if (node->type == ActionsDAG::ActionType::ALIAS)
            return removeBM25Conditions(node->children.at(0), context);

        const String function_name = node->type == ActionsDAG::ActionType::FUNCTION && node->function_base ? node->function_base->getName() : "";
        if (function_name != "and" && function_name != "or")
            return nullptr;

        ActionsDAG::NodeRawConstPtrs children;
        for (const auto * child : node->children)
        {
            if (const auto * remaining = removeBM25Conditions(child, context))
                children.push_back(remaining);
        }

        if (children.size() <= 1)
            return children.empty() ? nullptr : children.front();

        return &actions_dag.addFunction(FunctionFactory::instance().get(function_name, context), children, "");
    }

    /// The `bm25()` score of the filter subtree at `node`, mirroring its boolean structure: a scoring predicate
    /// is its (masked) score column; `OR` adds the scores of its children; `AND` adds them when the whole
    /// conjunction holds, with no mask on the root conjunction because its rows are filtered anyway; anything
    /// else (`NOT`, non-text predicates, opaque functions) adds nothing. Null when the subtree has no score.
    /// A condition calling `bm25()` itself adds nothing and is not part of the masks, so the score does not depend on itself.
    const ActionsDAG::Node * assembleScore(const ActionsDAG::Node * node, bool on_root_conjunction, BM25Assembly & assembly, const ContextPtr & context)
    {
        /// A replaced predicate is registered by its final node, which may be an alias of the match column.
        if (auto it = scoring_leaves.find(node); it != scoring_leaves.end())
        {
            const auto & leaf = it->second;

            if (assembly.num_predicates > 0 && assembly.index_name != leaf.index_name)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function bm25 with predicates on multiple scoring text indexes ({} and {}) is not supported",
                    assembly.index_name, leaf.index_name);
            }

            assembly.index_name = leaf.index_name;
            ++assembly.num_predicates;
            assembly.score_columns.insert(leaf.score_input->result_name);
            return leaf.score_input;
        }

        if (node->type == ActionsDAG::ActionType::ALIAS)
            return assembleScore(node->children.at(0), on_root_conjunction, assembly, context);

        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
            return nullptr;

        const auto & function_name = node->function_base->getName();
        if (function_name != "and" && function_name != "or")
            return nullptr;

        const bool is_conjunction = function_name == "and";
        ActionsDAG::NodeRawConstPtrs scores;

        for (const auto * child : node->children)
        {
            if (const auto * score = assembleScore(child, on_root_conjunction && is_conjunction, assembly, context))
                scores.push_back(score);
        }

        if (scores.empty())
            return nullptr;

        const ActionsDAG::Node * sum = scores.front();
        auto plus = FunctionFactory::instance().get("plus", context);
        for (size_t i = 1; i < scores.size(); ++i)
            sum = &actions_dag.addFunction(plus, {sum, scores[i]}, "");

        if (is_conjunction && !on_root_conjunction)
        {
            /// Not empty: the conditions calling `bm25()` have no score, the ones that do remain.
            const auto * mask = removeBM25Conditions(node, context);
            chassert(mask);

            auto if_function = FunctionFactory::instance().get("if", context);
            sum = &actions_dag.addFunction(if_function, {mask, sum, &getFloat32Zero()}, "");
        }

        return sum;
    }

    /// Applies preprocessor, tokenizer and postprocessor for text-search functions.
    const ActionsDAG::Node * processTextIndexFunction(
        const ActionsDAG::Node & function_node,
        const std::vector<SelectedCondition> & selected_conditions,
        const ContextPtr & context)
    {
        if (selected_conditions.size() != 1 || function_node.children.size() < 2 || function_node.children.size() > 3)
            return &function_node;

        auto new_children = function_node.children;
        const auto & arg_haystack = new_children[0];
        const auto & arg_needles = new_children[1];

        if (arg_needles->type != ActionsDAG::ActionType::COLUMN || !arg_needles->column)
            return &function_node;

        if (arg_needles->column->onlyNull())
            return &function_node;

        Field needles_field = (*arg_needles->column)[0];
        DataTypePtr needles_type = arg_needles->result_type;

        const auto & condition = selected_conditions.front();
        const auto & condition_text = typeid_cast<MergeTreeIndexConditionText &>(*condition.info->condition);
        auto preprocessor = condition_text.getPreprocessor();
        auto postprocessor = condition_text.getPostprocessor();
        const bool has_postprocessor = postprocessor && postprocessor->hasActions();
        const auto * tokenizer = condition_text.getTokenizer();
        auto function_name = function_node.function_base->getName();

        /// Preprocessor: only for an index-analyzed predicate in this filter DAG, so it never depends on a sibling filter. Tokenizer/postprocessor also apply on the row-scan path.
        const bool apply_preprocessor = is_filter_dag && condition.info->index != nullptr && condition.is_index_analyzed && needApplyPreprocessor(function_name) && preprocessor && preprocessor->hasActions();
        const bool apply_tokenizer = needApplyTokenizer(function_name) && tokenizer;
        const bool apply_postprocessor = needApplyPostprocessor(function_name) && has_postprocessor;

        if (!apply_preprocessor && !apply_tokenizer && !apply_postprocessor)
            return &function_node;

        /// Spliced into the postprocessor DAG below: merging two DAGs would unify their `__lambda` nodes by name.
        ASTPtr preprocessor_source_ast;

        if (apply_preprocessor)
        {
            const auto & preprocessor_dag = preprocessor->getOriginalActionsDAG();
            chassert(preprocessor_dag.getOutputs().size() == 1);
            const auto & preprocessor_output = preprocessor_dag.getOutputs().front();
            /// The index was analyzed on the expression under lossless conversions, e.g. `s` in `hasToken(toNullable(s), 'Foo')`.
            const auto * haystack = unwrapLosslessConversion(arg_haystack);
            auto haystack_name = getNameWithoutAliases(haystack);

            /// Check that preprocessor contains current expression as its argument.
            if (hasSubexpression(preprocessor_output, haystack_name))
            {
                /// Keep a `CAST` that drops `Nullable` under the preprocessor, so that the predicate still throws on NULL.
                new_children[0] = unwrapLosslessConversion(arg_haystack, /*allow_drop_nullable=*/ false);

                if (apply_postprocessor)
                {
                    preprocessor_source_ast = preprocessor->getExpressionAST(new_children[0]->result_name);
                }
                else
                {
                    ActionsDAG::NodeRawConstPtrs merged_outputs;
                    actions_dag.mergeNodes(
                        preprocessor->getActionsDAGForColumn(new_children[0]->result_name, new_children[0]->result_type),
                        &merged_outputs);

                    chassert(merged_outputs.size() == 1);
                    new_children[0] = merged_outputs.front();
                }

                /// Needles in array are not processed and passed as is.
                if (needles_field.getType() == Field::Types::String)
                {
                    needles_field = preprocessor->processConstant(needles_field.safeGet<String>());
                    needles_type = std::make_shared<DataTypeString>();
                }
            }
        }

        if (apply_tokenizer)
        {
            const String tokenizer_description = tokenizer->getDescription();

            /// Set the argument with the tokenizer definition. Assign when one is already present, so
            /// the argument count stays the same however often this runs over the same node.
            DataTypePtr arg_type = std::make_shared<DataTypeString>();
            MutableColumnConstPtr arg_column = arg_type->createColumnConst(0, Field(tokenizer_description));
            String name = quoteString(tokenizer_description);
            const ActionsDAG::Node & new_child = actions_dag.addColumn(std::move(arg_column), std::move(arg_type), std::move(name));
            if (new_children.size() == 3)
                new_children[2] = &new_child;
            else
                new_children.push_back(&new_child);

            /// Convert needles to array if they are a string by applying a tokenizer.
            /// For hasPhrase the phrase must stay as a string — tokenization is done inside hasPhrase itself.
            const bool convert_needle_to_array = function_name == "hasAnyTokens" || function_name == "hasAllTokens";
            if (convert_needle_to_array && needles_field.getType() == Field::Types::String)
            {
                VectorWithMemoryTracking<String> needles_array;
                const auto & needles_string = needles_field.safeGet<String>();
                tokenizer->stringToTokens(needles_string.data(), needles_string.size(), needles_array);
                /// Compaction is valid only for hasAllTokens and is unsound before a postprocessor.
                if (function_name == "hasAllTokens" && !apply_postprocessor)
                    needles_array = tokenizer->compactTokens(needles_array);
                needles_field = Array(needles_array.begin(), needles_array.end());
                needles_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
            }
        }

        /// Rewrite the haystack into the postprocessed tokens the index stores, so the row-level
        /// function still matches when the index isn't read directly (direct read off, or unmaterialized
        /// parts). getOriginalActionsDAG yields an Array(String) of postprocessed tokens.
        if (apply_postprocessor)
        {
            /// Name the postprocessor's haystack input after the haystack node's actual result_name so
            /// mergeNodes reuses that node (it matches by result_name). A reconstructed name can diverge for
            /// an ALIAS/expression haystack (e.g. `ifNull(str, 'default')`), leaving a dangling input.
            const auto & haystack_name = new_children[0]->result_name;
            ActionsDAG::NodeRawConstPtrs merged_outputs;
            actions_dag.mergeNodes(
                postprocessor->getOriginalActionsDAG(haystack_name, new_children[0]->result_type, tokenizer->getDescription(), preprocessor_source_ast),
                &merged_outputs);
            chassert(merged_outputs.size() == 1);
            new_children[0] = merged_outputs.front();

            /// new_children[0] is now an Array(String) of postprocessed tokens. Switch the tokenizer argument
            /// to 'array' to match them verbatim, instead of re-splitting tokens the index stores whole.
            if (function_name == "hasAnyTokens" || function_name == "hasAllTokens" || function_name == "hasPhrase")
            {
                chassert(new_children.size() == 3);
                DataTypePtr arg_type = std::make_shared<DataTypeString>();
                const String array_tokenizer_desc = ArrayTokenizer::getName();
                MutableColumnConstPtr arg_column = arg_type->createColumnConst(0, Field(array_tokenizer_desc));
                new_children[2] = &actions_dag.addColumn(std::move(arg_column), arg_type, quoteString(array_tokenizer_desc));
            }

            /// hasToken takes a String haystack, so rejoin the tokens for it to re-tokenize; dropped tokens are
            /// empty elements that collapse into adjacent separators, keeping positions dense. Its index
            /// tokenizer is always splitByNonAlpha, which splits on this space.
            if (function_name == "hasToken")
            {
                DataTypePtr separator_type = std::make_shared<DataTypeString>();
                MutableColumnConstPtr separator_column = separator_type->createColumnConst(0, Field(String(" ")));
                const ActionsDAG::Node & separator = actions_dag.addColumn(std::move(separator_column), separator_type, "' '");
                FunctionOverloadResolverPtr concat = FunctionFactory::instance().get("arrayStringConcat", context);
                new_children[0] = &actions_dag.addFunction(concat, {new_children[0], &separator}, "");
            }

            if (function_name == "hasPhrase" && needles_field.getType() == Field::Types::String)
            {
                /// Compare token sequences: rejoining into strings assumed the index tokenizer splits on the
                /// separator used, which is false for e.g. splitByString(['()']). An emptied phrase never matches.
                const auto & phrase = needles_field.safeGet<String>();
                VectorWithMemoryTracking<String> tokens;
                tokenizer->stringToTokens(phrase.data(), phrase.size(), tokens);
                tokens = postprocessor->processTokens(std::move(tokens));

                needles_field = Array(tokens.begin(), tokens.end());
                needles_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
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
                /// `hasPhrase` ignores an empty element, the set predicates keep it as a token that never matches.
                const bool drop_empty_needles = function_name == "hasPhrase";
                for (const Field & element : src_array)
                {
                    if (element.getType() != Field::Types::String)
                        continue;

                    const auto & element_value = element.safeGet<String>();
                    if (!drop_empty_needles || !element_value.empty())
                        tokens.push_back(element_value);
                }
                /// Compaction is unsound after a postprocessor, and `hasPhrase` needs every duplicate, in order.
                tokens = postprocessor->processTokens(std::move(tokens));
                needles_field = Array(tokens.begin(), tokens.end());
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

        return &actions_dag.addAlias(*new_function_node, function_node.result_name);
    }

    /// Optimizes text-search functions by replacing them with virtual columns.
    const ActionsDAG::Node * replaceFunctionsToVirtualColumns(
        const ActionsDAG::Node & function_node,
        std::vector<SelectedCondition> selected_conditions,
        const ContextPtr & context)
    {
        std::erase_if(selected_conditions, [](const auto & condition) { return condition.virtual_column.name.empty(); });
        if (selected_conditions.empty())
            return &function_node;

        /// Sort conditions to produce stable output for EXPLAIN query.
        std::ranges::sort(selected_conditions, [](const auto & lhs, const auto & rhs)
        {
            return lhs.virtual_column.name < rhs.virtual_column.name;
        });

        bool has_exact_search = false;
        bool has_materialized_index = false;

        for (const auto & condition : selected_conditions)
        {
            has_materialized_index |= condition.info->is_materialized;
            has_exact_search |= condition.virtual_column.search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact;
        }

        /// It doesn't make sense to optimize if index is not materialized in any data part.
        if (!has_materialized_index)
            return &function_node;

        /// Convert up front: without an AST the optimization must be skipped, not registered with a different meaning.
        ASTPtr exact_default_expression;
        if (has_exact_search)
        {
            exact_default_expression = convertNodeToAST(function_node);
            if (!exact_default_expression)
            {
                LOG_TRACE(
                    getLogger("optimizeDirectReadFromTextIndex"),
                    "Cannot use direct reading from text index. Predicate '{}' has no AST representation",
                    function_node.result_name);
                return &function_node;
            }
        }

        auto add_condition_to_input = [&](SelectedCondition & condition)
        {
            auto & column = condition.virtual_column;
            auto [it, inserted] = virtual_column_inputs.try_emplace(column.name);

            if (inserted)
            {
                /// Create a default expression for the virtual column.
                /// It will be executed by merge tree reader when index is not materialized in the data part.
                /// Shared, not cloned: a stored default expression is immutable, `addDefaultRequiredExpressionsRecursively` clones it before use.
                /// Do not execute the default expression for hint mode, because it will be executed anyway in the original predicate.
                if (column.search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact)
                    column.default_expression = exact_default_expression;
                else if (column.search_query->getDirectReadMode() == TextIndexDirectReadMode::Hint)
                    column.default_expression = make_intrusive<ASTLiteral>(Field(1));

                it->second = &actions_dag.addInput(column.name, column.type);
                auto [task_it, task_inserted] = index_read_tasks.try_emplace(condition.index_name);

                if (task_inserted)
                    task_it->second.index = *condition.info->index;

                task_it->second.columns.push_back(std::move(column));
            }

            return it->second;
        };

        /// A scoring predicate of a query computing `bm25()` also gets its score column (`Float32`), filled by
        /// the same reader as the match column. It is registered like the match column and referenced only
        /// by the assembled score, so it is read only when the predicate ends up in the score expression.
        std::optional<ScoringLeaf> scoring_leaf;
        if (bm25_rewrite)
        {
            auto scoring_condition = std::ranges::find_if(selected_conditions, isScoringCondition);
            if (scoring_condition != selected_conditions.end())
            {
                const auto & search_query = scoring_condition->virtual_column.search_query;
                const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*scoring_condition->info->condition);
                String score_column_name = condition_text.getScoreVirtualColumnName(*search_query, scoring_condition->index_name);

                auto [it, inserted] = virtual_column_inputs.try_emplace(score_column_name);
                if (inserted)
                {
                    /// Only a part with the materialized index can score, and `buildBM25State` rejects the others.
                    auto score_type = std::make_shared<DataTypeFloat32>();
                    it->second = &actions_dag.addInput(score_column_name, score_type);
                    auto [task_it, task_inserted] = index_read_tasks.try_emplace(scoring_condition->index_name);

                    if (task_inserted)
                        task_it->second.index = *scoring_condition->info->index;

                    task_it->second.columns.push_back({score_column_name, std::move(score_type), search_query, nullptr});
                }

                scoring_leaf = ScoringLeaf{.score_input = it->second, .search_query = search_query, .index_name = scoring_condition->index_name};
            }
        }

        const ActionsDAG::Node * result_node = nullptr;

        /// If we have only one condition with exact search, we can use
        /// only virtual column and remove the original condition.
        if (selected_conditions.size() == 1 && has_exact_search)
        {
            result_node = add_condition_to_input(selected_conditions.front());
        }
        else /// Otherwise, combine all conditions with the AND function.
        {
            ActionsDAG::NodeRawConstPtrs children;
            auto function_builder = FunctionFactory::instance().get("and", context);

            for (auto & condition : selected_conditions)
                children.push_back(add_condition_to_input(condition));

            if (!has_exact_search)
                children.push_back(&function_node);

            result_node = &actions_dag.addFunction(function_builder, children, "");
        }

        /// If the type of original function does not match the type of replacement,
        /// add a cast to the replacement to match the expected type (e.g. hasAnyTokens('hello world', toNullable('world'))).
        /// It can happen when the original function returns Nullable or LowCardinality type and replacement doesn't.
        if (!function_node.result_type->equals(*result_node->result_type))
            result_node = &actions_dag.addCast(*result_node, function_node.result_type, "", context);

        /// Preserve the original column name so that downstream steps (e.g. ExpressionStep for SELECT)
        /// that reference the predicate by its original name can still find it in the block.
        if (result_node->result_name != function_node.result_name)
            result_node = &actions_dag.addAlias(*result_node, function_node.result_name);

        /// The replaced node (after any cast and alias) is what the rewritten filter refers to.
        if (scoring_leaf)
            scoring_leaves.emplace(result_node, *scoring_leaf);

        return result_node;
    }
};

/// Result of the direct-read rewrite of one filter DAG.
struct TextIndexDAGResult
{
    const ActionsDAG::Node * filter_node = nullptr;
    TextIndexDAGReplacer::BM25Assembly bm25;
};

static TextIndexDAGResult processAndOptimizeTextIndexDAG(
    ReadFromMergeTree & read_from_merge_tree_step,
    ActionsDAG & filter_dag,
    const TextIndexReadInfos & text_index_read_infos,
    const String & filter_column_name,
    bool direct_read_from_text_index,
    bool require_index_analyzed_predicate,
    NameSet * scoring_predicate_indexes,
    const BM25Rewrite * bm25_rewrite,
    size_t bm25_dag)
{
    const BM25Rewrite * dag_bm25_rewrite = direct_read_from_text_index ? bm25_rewrite : nullptr;
    TextIndexDAGReplacer replacer(
        filter_dag, text_index_read_infos, direct_read_from_text_index, /*is_filter_dag=*/ true, require_index_analyzed_predicate,
        scoring_predicate_indexes, dag_bm25_rewrite, dag_bm25_rewrite ? dag_bm25_rewrite->getResultNamesAbove(bm25_dag) : NameOrderedSet{});
    auto result = replacer.replace(read_from_merge_tree_step.getContext(), filter_column_name);
    if (!result)
        return {};

    /// Even when no virtual columns are added (index_read_tasks is empty),
    /// the DAG may have been modified by text index preprocessing
    /// (e.g. applying tokenizer/preprocessor to hasAnyTokens).
    /// In that case, we must return the filter node
    /// so the caller can update the filter column name to match the modified DAG.
    if (result->index_read_tasks.empty())
        return {.filter_node = result->filter_node, .bm25 = {}};

    /// Keep columns the PREWHERE or row-level filter still read, so this filter DAG's removal does not drop them from the shared read set.
    {
        NameSet required_columns_by_readers;
        if (auto prewhere = read_from_merge_tree_step.getPrewhereInfo())
            for (const auto & name : prewhere->prewhere_actions.getRequiredColumnsNames())
                required_columns_by_readers.insert(name);

        if (auto row_level_filter = read_from_merge_tree_step.getRowLevelFilter())
            for (const auto & name : row_level_filter->actions.getRequiredColumnsNames())
                required_columns_by_readers.insert(name);

        const auto & read_header = *read_from_merge_tree_step.getOutputHeader();
        std::erase_if(result->removed_columns, [&](const String & column)
        {
            if (!required_columns_by_readers.contains(column))
                return false;

            /// ActionsDAG::updateHeader appends a header column that is not an input of the DAG,
            /// which would widen this step's output header.
            if (read_header.has(column))
                filter_dag.addInput(read_header.getByName(column));
            return true;
        });
    }

    auto logger = getLogger("processAndOptimizeTextIndexFunctions");
    LOG_DEBUG(logger, "{}", optimizationInfoToString(result->index_read_tasks, result->removed_columns));

    /// Log partially materialized text indexes
    for (const auto & [index_name, info] : text_index_read_infos)
    {
        if (!info.is_fully_materialized)
            LOG_DEBUG(logger, "Text index '{}' is not fully materialized. In some parts, direct read from text index cannot be used.", index_name);
    }

    read_from_merge_tree_step.createReadTasksForTextIndex(std::move(result->index_read_tasks), result->removed_columns);
    return {.filter_node = result->filter_node, .bm25 = std::move(result->bm25)};
}

/// Applies the tokenizer/preprocessor/postprocessor rewrite to text-search functions in an arbitrary DAG,
/// without any direct read from the text index. Returns true if the DAG was modified.
/// `filter_column_name` (with `scoring_predicate_indexes`) is passed for the DAG of a filter step,
/// so scoring predicates that stay row-wise in filters above the scan are still accounted for.
static bool applyTextIndexInject(
    ReadFromMergeTree & read_from_merge_tree_step,
    ActionsDAG & dag,
    const TextIndexReadInfos & text_index_infos,
    NameSet * scoring_predicate_indexes,
    const String & filter_column_name)
{
    TextIndexDAGReplacer replacer(dag, text_index_infos, /*direct_read_from_text_index=*/ false, /*is_filter_dag=*/ false, /*require_index_analyzed_predicate=*/ false, scoring_predicate_indexes);
    return replacer.replace(read_from_merge_tree_step.getContext(), filter_column_name).has_value();
}

static bool processAndOptimizeTextIndexFunctionsInPrewhere(
    ReadFromMergeTree & read_from_merge_tree_step,
    const PrewhereInfoPtr & prewhere_info,
    const TextIndexReadInfos & text_index_read_infos,
    bool direct_read_from_text_index,
    bool require_index_analyzed_predicate,
    NameSet * scoring_predicate_indexes,
    const BM25Rewrite * bm25_rewrite,
    TextIndexDAGReplacer::BM25Assembly & bm25_assembly)
{
    read_from_merge_tree_step.updatePrewhereInfo({});
    auto cloned_prewhere_info = prewhere_info->clone();
    auto result = processAndOptimizeTextIndexDAG(read_from_merge_tree_step, cloned_prewhere_info.prewhere_actions, text_index_read_infos, cloned_prewhere_info.prewhere_column_name, direct_read_from_text_index, require_index_analyzed_predicate, scoring_predicate_indexes, bm25_rewrite, BM25Rewrite::PREWHERE_DAG);

    if (!result.filter_node)
    {
        read_from_merge_tree_step.updatePrewhereInfo(prewhere_info);
        return false;
    }

    if (result.bm25.isAssembled())
        bm25_assembly = std::move(result.bm25);

    cloned_prewhere_info.prewhere_column_name = result.filter_node->result_name;
    auto modified_prewhere_info = std::make_shared<PrewhereInfo>(std::move(cloned_prewhere_info));
    read_from_merge_tree_step.updatePrewhereInfo(modified_prewhere_info);
    return true;
}

/// Steps that keep the indexed column intact (WindowStep only appends columns), so the injection walk can
/// traverse past them to reach a projection/filter lifted above them.
static bool isRowScanPassThroughStep(const IQueryPlanStep * step)
{
    return typeid_cast<const SortingStep *>(step)
        || typeid_cast<const LimitStep *>(step)
        || typeid_cast<const NegativeLimitStep *>(step)
        || typeid_cast<const FractionalLimitStep *>(step)
        || typeid_cast<const OffsetStep *>(step)
        || typeid_cast<const NegativeOffsetStep *>(step)
        || typeid_cast<const FractionalOffsetStep *>(step)
        || typeid_cast<const LimitByStep *>(step)
        || typeid_cast<const NegativeLimitByStep *>(step)
        || typeid_cast<const DistinctStep *>(step)
        || typeid_cast<const WindowStep *>(step);
}

static const ActionsDAG * getStepDAG(const IQueryPlanStep * step)
{
    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        return &filter_step->getExpression();
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        return &expression_step->getExpression();
    return nullptr;
}

/// Collects the `bm25()` calls of the query fragment of the read step: its PREWHERE and the steps of the
/// walk [walk_begin, walk_end). A call beyond the walk (above an aggregation, a join, ...) is rejected.
static std::optional<BM25Rewrite> collectBM25Rewrite(
    const ReadFromMergeTree & read_from_merge_tree_step,
    const Stack & stack,
    Stack::const_reverse_iterator walk_begin,
    Stack::const_reverse_iterator walk_end)
{
    std::optional<BM25Params> params;
    std::vector<NameOrderedSet> result_names_by_dag;
    bool is_in_filter_condition = false;

    auto & prewhere_names = result_names_by_dag.emplace_back();
    if (auto prewhere_info = read_from_merge_tree_step.getPrewhereInfo())
    {
        addBM25Nodes(prewhere_info->prewhere_actions, params, prewhere_names);
        is_in_filter_condition |= !prewhere_names.empty() && filterConditionCallsBM25(prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name);
    }

    for (auto it = walk_begin; it != walk_end; ++it)
    {
        const auto * step = it->node->step.get();
        auto & step_names = result_names_by_dag.emplace_back();
        if (const auto * dag = getStepDAG(step))
            addBM25Nodes(*dag, params, step_names);

        if (const auto * filter_step = typeid_cast<const FilterStep *>(step); filter_step && !step_names.empty())
            is_in_filter_condition |= filterConditionCallsBM25(filter_step->getExpression(), filter_step->getFilterColumnName());
    }

    for (auto it = walk_end; it != stack.rend(); ++it)
    {
        const auto * dag = getStepDAG(it->node->step.get());
        if (dag && !collectBM25Nodes(*dag).empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function bm25 cannot be computed above the '{}' step: the score exists only in the query fragment "
                "reading the table with the text index", walk_end->node->step->getName());
        }
    }

    if (!params)
        return {};

    const auto & settings = read_from_merge_tree_step.getContext()->getSettingsRef();
    if (!settings[Setting::allow_experimental_bm25_scoring])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function bm25 is experimental. Enable it with the setting `allow_experimental_bm25_scoring = 1`");

    return BM25Rewrite{.params = *params, .result_names_by_dag = std::move(result_names_by_dag), .is_in_filter_condition = is_in_filter_condition};
}

/// Turns `dag`, a copy of the score expression of `assembly`, into the actions computing `bm25()` ahead of the PREWHERE:
/// they output the score under each of `result_names` and return the score node. The score columns are consumed;
/// the other inputs (the match columns of the masked conjunctions, the columns of non-text predicates) pass through,
/// so the filter that assembled the score keeps them.
static const ActionsDAG::Node & exposeBM25Score(ActionsDAG & dag, const TextIndexDAGReplacer::BM25Assembly & assembly, const NameOrderedSet & result_names)
{
    const auto * score = dag.getOutputs().front();

    auto & outputs = dag.getOutputs();
    outputs.clear();

    for (const auto & name : result_names)
        outputs.push_back(&dag.addAlias(*score, name));

    for (const auto * input : dag.getInputs())
    {
        if (!assembly.score_columns.contains(input->result_name))
            outputs.push_back(input);
    }

    return *score;
}

/// Builds the dynamic top-k filter of `ORDER BY bm25() LIMIT n` as the PREWHERE of the read step: it computes the score,
/// filters it by the threshold of the sorting (see `tryOptimizeTopK`) and outputs it, so the steps above take it as a column.
static void installBM25TopKFilter(ReadFromMergeTree & read_from_merge_tree_step, const TextIndexDAGReplacer::BM25Assembly & assembly, const BM25Rewrite & rewrite)
{
    ActionsDAG dag = assembly.score_dag->clone();
    const auto & score = exposeBM25Score(dag, assembly, rewrite.getResultNamesAbove(BM25Rewrite::PREWHERE_DAG));

    auto filter_function = createInternalFunctionTopKFilterResolver(read_from_merge_tree_step.getTopKFilterInfo()->threshold_tracker);
    const auto * filter_node = &dag.addFunction(filter_function, {&score}, {});
    dag.getOutputs().push_back(filter_node);

    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = std::move(dag);
    prewhere_info->prewhere_column_name = filter_node->result_name;
    prewhere_info->remove_prewhere_column = true;
    prewhere_info->need_filter = true;
    read_from_merge_tree_step.updatePrewhereInfo(prewhere_info);
}

/// Computes the score ahead of the PREWHERE of the read step, which calls `bm25()` without having a scoring predicate:
/// the PREWHERE and the steps above take the score as a column.
static void addBM25ScoreToPrewhere(ReadFromMergeTree & read_from_merge_tree_step, const TextIndexDAGReplacer::BM25Assembly & assembly, const BM25Rewrite & rewrite)
{
    auto prewhere_info = read_from_merge_tree_step.getPrewhereInfo()->clone();
    replaceBM25NodesWithInputs(prewhere_info.prewhere_actions, rewrite.getResultNamesAbove(BM25Rewrite::PREWHERE_DAG));

    ActionsDAG score_actions = assembly.score_dag->clone();
    auto result_names = rewrite.getResultNamesAbove(BM25Rewrite::PREWHERE_DAG);
    const auto & prewhere_names = rewrite.result_names_by_dag[BM25Rewrite::PREWHERE_DAG];
    result_names.insert(prewhere_names.begin(), prewhere_names.end());
    exposeBM25Score(score_actions, assembly, result_names);

    prewhere_info.prewhere_actions = ActionsDAG::merge(std::move(score_actions), std::move(prewhere_info.prewhere_actions));
    read_from_merge_tree_step.updatePrewhereInfo(std::make_shared<PrewhereInfo>(std::move(prewhere_info)));
}

/// Applies text index optimizations to the query plan.
///
/// Always preprocesses `hasAllTokens`/`hasAnyTokens` arguments with text index metadata
/// (preprocessor wrapping, string-to-array tokenization, tokenizer arguments). Besides the WHERE/PREWHERE
/// filter, this also runs for Expression/Filter steps above the scan (e.g. the SELECT list) and does not
/// require skip-index analysis.
///
/// When `direct_read_from_text_index` is true, also replaces text-search functions
/// with virtual columns for direct index reads (both WHERE and PREWHERE clauses).
///
/// When the fragment calls `bm25()`, the scoring predicates of the direct read also get score columns, the
/// filter step assembles the score from them following its boolean structure (see `TextIndexDAGReplacer::assembleScore`)
/// and outputs it under the call's name, and the steps above take it as an input.
///
/// See TextIndexDAGReplacer class for more details.
void processAndOptimizeTextIndexFunctions(
    const Stack & stack, QueryPlan::Nodes & nodes, bool direct_read_from_text_index, const Optimization::ExtraSettings & settings)
{
    const auto & frame = stack.back();
    ReadFromMergeTree * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree_step)
        return;

    /// Text index conditions used to rewrite text-search functions. Direct-read-eligible entries come from the
    /// analyzed skip indexes; the rest are filled in from the index definitions, so the rewrite also covers the
    /// SELECT list, steps above the scan, and queries that did not run skip-index analysis.
    TextIndexReadInfos text_index_infos;
    collectTextIndexReadInfos(read_from_merge_tree_step, text_index_infos);
    collectTextIndexInjectInfos(read_from_merge_tree_step, text_index_infos);
    if (text_index_infos.empty())
        return;

    /// This step can be visited by the pass more than once, because a Merge child plan is optimized
    /// again after ReadFromMerge pushes a filter down into it (StorageMerge), and because the same
    /// text-index predicate can reach the step from several filter stages. Direct read replaces a
    /// text-search function with a synthetic __text_index_..._has_<hash> column that is registered
    /// into the step's read set exactly once (ReadFromMergeTree::createReadTasksForTextIndex). If the
    /// step already carries index read tasks, direct read has already been applied to it, so a later
    /// visit must not register the column a second time -- doing so throws "already added for reading"
    /// (e.g. an identical predicate in both PREWHERE and WHERE over Merge -> Distributed -> MergeTree).
    /// The tokenizer/preprocessor rewrite below still runs regardless: it is needed for correctness
    /// (row-level evaluation when direct read is off or a part is not fully materialized) and does not
    /// register any read column.
    bool already_has_direct_read = !read_from_merge_tree_step->getIndexReadTasks().empty();

    /// Scoring text indexes with a scoring-eligible predicate in any of the query's filters,
    NameSet scoring_predicate_indexes;

    /// A first-pass optimization can leave an `ExpressionStep` on top of the read step and hide the
    /// filter. Merge it into the filter above so direct read stays possible.
    auto walk_begin = stack.rbegin() + 1;
    if (stack.size() >= 3 && typeid_cast<ExpressionStep *>(walk_begin->node->step.get()))
    {
        QueryPlan::Node * node_above = (stack.rbegin() + 2)->node;
        if (typeid_cast<FilterStep *>(node_above->step.get()) && tryMergeExpressions(node_above, nodes, settings))
            ++walk_begin; /// the merged-away step is detached now, the filter sits directly above the scan
    }

    /// The walk covers the steps above the scan up to the first one that changes the column set (aggregation, join):
    /// filters, expressions and row-preserving pass-throughs (liftUpFunctions may hoist a projection above a sort).
    auto walk_end = walk_begin;
    while (walk_end != stack.rend())
    {
        const auto * step = walk_end->node->step.get();
        if (!typeid_cast<const FilterStep *>(step) && !typeid_cast<const ExpressionStep *>(step) && !isRowScanPassThroughStep(step))
            break;
        ++walk_end;
    }

    /// The `bm25()` calls must be known before the rewrite: the scoring predicates get score columns while they are replaced.
    std::optional<BM25Rewrite> bm25_rewrite = collectBM25Rewrite(*read_from_merge_tree_step, stack, walk_begin, walk_end);

    /// `ORDER BY bm25() LIMIT n`: `tryOptimizeTopK` requested the dynamic top-k filter on the score (see
    /// `ReadFromMergeTree::computesSortColumnForTopK`). Only this pass can build it, as a PREWHERE of its own
    /// that computes the score, so `installTopKDynamicFilter` must not try.
    if (bm25_rewrite && read_from_merge_tree_step->hasPendingTopKDynamicFilter()
        && bm25_rewrite->hasResultName(read_from_merge_tree_step->getTopKFilterInfo()->column_name))
    {
        read_from_merge_tree_step->clearPendingTopKDynamicFilter();
        bm25_rewrite->assemble_in_prewhere = !read_from_merge_tree_step->getPrewhereInfo();
    }

    if (bm25_rewrite && already_has_direct_read)
        bm25_rewrite.reset(); /// the score was assembled by the earlier visit

    /// A plan without index analysis (e.g. the probe plan of the automatic parallel replicas) cannot read the index
    /// directly, so the calls stay as they are; executing them reports the reason.
    if (bm25_rewrite && !read_from_merge_tree_step->getIndexes())
        bm25_rewrite.reset();

    if (bm25_rewrite && !direct_read_from_text_index)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Function bm25 requires the direct read from the text index (the setting `query_plan_direct_read_from_text_index`)");
    }

    TextIndexDAGReplacer::BM25Assembly bm25_assembly;

    /// --- PREWHERE ---
    bool prewhere_optimized = false;
    if (auto prewhere_info = read_from_merge_tree_step->getPrewhereInfo())
    {
        /// A PREWHERE deferred after FINAL never runs during reading and is excluded from index analysis, so it gets neither the direct read nor a sibling predicate's preprocessor.
        bool is_deferred_after_final = read_from_merge_tree_step->isPrewhereDeferredAfterFinal();
        bool direct_read_allowed = direct_read_from_text_index && !already_has_direct_read && !is_deferred_after_final;
        prewhere_optimized = processAndOptimizeTextIndexFunctionsInPrewhere(*read_from_merge_tree_step, prewhere_info, text_index_infos, direct_read_allowed, /*require_index_analyzed_predicate=*/ is_deferred_after_final, &scoring_predicate_indexes, bm25_rewrite ? &*bm25_rewrite : nullptr, bm25_assembly);

        /// The PREWHERE calls `bm25()` but the score is assembled from the WHERE filter: it is computed ahead of the PREWHERE.
        if (bm25_rewrite && !bm25_assembly.isAssembled() && !bm25_rewrite->result_names_by_dag[BM25Rewrite::PREWHERE_DAG].empty())
            bm25_rewrite->assemble_in_prewhere = true;
    }

    for (auto it = walk_begin; it != walk_end; ++it)
    {
        QueryPlan::Node * node = it->node;
        IQueryPlanStep * step = node->step.get();

        auto * filter_step = typeid_cast<FilterStep *>(step);
        auto * expression_step = typeid_cast<ExpressionStep *>(step);

        /// Only a filter directly above the scan can carry the virtual column; otherwise the whole text column is read.
        if (it == walk_begin && !filter_step)
            LOG_TRACE(
                getLogger("optimizeDirectReadFromTextIndex"),
                "Cannot use direct reading from text index. Reason: the parent of ReadFromMergeTree is a '{}' step, not a filter",
                step->getName());

        if (!filter_step && !expression_step)
            continue;

        /// Direct read only for the WHERE filter directly above the scan (its rebuild uses the scan's header).
        if (filter_step && it == walk_begin)
        {
            ActionsDAG & filter_dag = filter_step->getExpression();
            bool direct_read_allowed = direct_read_from_text_index && !prewhere_optimized && !already_has_direct_read;
            auto result = processAndOptimizeTextIndexDAG(
                *read_from_merge_tree_step, filter_dag, text_index_infos, filter_step->getFilterColumnName(), direct_read_allowed,
                /*require_index_analyzed_predicate=*/ false, &scoring_predicate_indexes, bm25_rewrite ? &*bm25_rewrite : nullptr, BM25Rewrite::WHERE_DAG);

            if (!result.filter_node)
                continue;

            if (result.bm25.isAssembled())
            {
                bm25_assembly = std::move(result.bm25);

                /// The filter step is rebuilt on the read step's header, which must already carry the score.
                if (bm25_rewrite->assemble_in_prewhere && read_from_merge_tree_step->getPrewhereInfo())
                    addBM25ScoreToPrewhere(*read_from_merge_tree_step, bm25_assembly, *bm25_rewrite);
                else if (bm25_rewrite->assemble_in_prewhere)
                    installBM25TopKFilter(*read_from_merge_tree_step, bm25_assembly, *bm25_rewrite);
            }

            bool removes_filter_column = filter_step->removesFilterColumn();
            auto new_filter_column_name = result.filter_node->result_name;
            node->step = std::make_unique<FilterStep>(read_from_merge_tree_step->getOutputHeader(), filter_dag.clone(), new_filter_column_name, removes_filter_column);
            continue;
        }

        /// Inject-only rewrite: a merged postprocessor DAG reads only the indexed column, so inputs and output names are unchanged -- reuse the step's input header.
        ActionsDAG & dag = filter_step ? filter_step->getExpression() : expression_step->getExpression();

        bool injected = filter_step
            ? applyTextIndexInject(*read_from_merge_tree_step, dag, text_index_infos, &scoring_predicate_indexes, filter_step->getFilterColumnName())
            : applyTextIndexInject(*read_from_merge_tree_step, dag, text_index_infos, nullptr, {});

        if (!injected)
            continue;

        const SharedHeader & input_header = step->getInputHeaders().front();
        if (filter_step)
            node->step = std::make_unique<FilterStep>(input_header, dag.clone(), filter_step->getFilterColumnName(), filter_step->removesFilterColumn());
        else
            node->step = std::make_unique<ExpressionStep>(input_header, dag.clone());
    }

    if (!bm25_rewrite)
        return;

    if (scoring_predicate_indexes.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function bm25 with predicates on multiple scoring text indexes is not supported");

    if (!bm25_assembly.isAssembled())
    {
        if (!scoring_predicate_indexes.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function bm25 requires the text-search predicate on the scoring text index '{}' "
                "to be evaluated by the direct read from the text index, but it is evaluated row-wise in this query",
                *scoring_predicate_indexes.begin());
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Function bm25 requires a `hasToken`, `hasAnyTokens` or `hasAllTokens` predicate "
            "on a column with a text index created with `scoring = 'bm25'`");
    }

    LOG_TRACE(getLogger("optimizeDirectReadFromTextIndex"),
        "BM25 score: index '{}', scoring predicates: {}, k1: {}, b: {}",
        bm25_assembly.index_name, bm25_assembly.num_predicates, bm25_rewrite->params.k1, bm25_rewrite->params.b);

    read_from_merge_tree_step->attachTextIndexScoring(bm25_assembly.index_name, bm25_rewrite->params);

    /// The score of a row depends on the statistics of all parts the query reads, so the result of a condition on it
    /// cannot be kept per granule of a part in the query condition cache. The rewritten PREWHERE no longer calls
    /// `bm25()`, so nothing else would notice it.
    if (bm25_rewrite->is_in_filter_condition)
        read_from_merge_tree_step->disableQueryConditionCache();

    /// The steps of the walk take `bm25()` as an input of the step computing it (the filter step, or the
    /// PREWHERE of the read step), and their headers follow the read step's.
    QueryPlan::Node * child = frame.node;
    size_t bm25_dag = BM25Rewrite::WHERE_DAG;
    for (auto it = walk_begin; it != walk_end; ++it, ++bm25_dag)
    {
        QueryPlan::Node * node = it->node;
        IQueryPlanStep * step = node->step.get();
        auto child_header = child->step->getOutputHeader();

        auto * filter_step = typeid_cast<FilterStep *>(step);
        auto * expression_step = typeid_cast<ExpressionStep *>(step);

        if ((filter_step || expression_step)
            && replaceBM25NodesWithInputs(filter_step ? filter_step->getExpression() : expression_step->getExpression(), bm25_rewrite->getResultNamesAbove(bm25_dag)))
        {
            if (filter_step)
                node->step = std::make_unique<FilterStep>(child_header, filter_step->getExpression().clone(), filter_step->getFilterColumnName(), filter_step->removesFilterColumn());
            else
                node->step = std::make_unique<ExpressionStep>(child_header, expression_step->getExpression().clone());
        }
        else
        {
            size_t child_idx = std::ranges::find(node->children, child) - node->children.begin();
            if (!blocksHaveEqualStructure(*step->getInputHeaders().at(child_idx), *child_header))
                step->updateInputHeader(child_header, child_idx);
        }

        child = node;
    }

    /// Above the walk, refresh the input headers until the outputs stop changing.
    for (auto it = walk_end; it != stack.rend(); ++it)
    {
        QueryPlan::Node * node = it->node;
        size_t child_idx = std::ranges::find(node->children, child) - node->children.begin();
        auto child_header = child->step->getOutputHeader();

        if (blocksHaveEqualStructure(*node->step->getInputHeaders().at(child_idx), *child_header))
            break;

        auto old_output_header = node->step->getOutputHeader();
        node->step->updateInputHeader(child_header, child_idx);

        if (blocksHaveEqualStructure(*old_output_header, *node->step->getOutputHeader()))
            break;

        child = node;
    }
}

}
