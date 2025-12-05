#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB::Setting
{
    extern const SettingsString ignore_data_skipping_indices;
    extern const SettingsBool use_skip_indexes_if_final;
    extern const SettingsBool use_skip_indexes;
}

namespace DB::QueryPlanOptimizations
{

/// Used when inverted-index functions (e.g. hasAllTokens) require a
/// constant Array(String) argument inside an ActionsDAG.
static ColumnWithTypeAndName makeConstArrayOfTokens(const std::vector<String> & tokens)
{
    Array array_field;
    array_field.reserve(tokens.size());
    for (const auto & token : tokens)
        array_field.emplace_back(token);

    DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    auto column = type->createColumnConst(1, Field(array_field));
    auto name = calculateConstantActionNodeName(Field { array_field });
    return { column, type, name };
}

static void collectLikeFunctionNodeWithIndexes(const ActionsDAG & dag, const ReadFromMergeTree * read_from_merge_tree, std::unordered_map<const ActionsDAG::Node*, IndexDescription> & like_node_with_indexes)
{
    std::unordered_map<const ActionsDAG::Node*, const MergeTreeIndexText*> result;
    auto settings = read_from_merge_tree->getContext()->getSettingsRef();

    bool use_skip_indexes = settings[Setting::use_skip_indexes];
    if (read_from_merge_tree->getQueryInfo().isFinal() && !settings[Setting::use_skip_indexes_if_final])
        use_skip_indexes = false;

    if (!use_skip_indexes)
        return;

    auto metadata_snapshot = read_from_merge_tree->getStorageMetadata();
    const auto &all_indexes = metadata_snapshot->getSecondaryIndices();
    if (all_indexes.empty())
        return;

    std::unordered_set<std::string> ignored_index_names;
    if (settings[Setting::ignore_data_skipping_indices].changed)
    {
        const auto & indices = settings[Setting::ignore_data_skipping_indices].toString();
        ignored_index_names = parseIdentifiersOrStringLiteralsToSet(indices, settings);
    }

    auto find_index = [&] (const String & column_name) -> std::optional<IndexDescription>
    {
        for (auto & index_desc: all_indexes)
        {
            if (ignored_index_names.contains(index_desc.name))
                continue;
            auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
            if (auto *text_index = dynamic_cast<const MergeTreeIndexText *>(index_helper.get()))
            {
                auto names = text_index->index.column_names;
                auto it = std::find_if(names.begin(), names.end(), [&](const auto &name) { return column_name == name; });
                if (it != names.end())
                    return index_desc;
            }
        }
        return {};
    };

    for (const auto & node: dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function->getName() == "like")
        {
            chassert(node.children.size() == 2);
            const auto * text_node = node.children[0]->type == ActionsDAG::ActionType::INPUT ? node.children[0] : node.children[1];
            auto index = find_index(text_node->result_name);
            if (index)
               like_node_with_indexes.insert({&node, *index});
        }
    }
}

/// Optimize a single LIKE expression using a text index.
/// This function rewrites: column LIKE '%prefix,token1,token2,suffix%'
/// into an equivalent: hasAllTokens(column, ['token1','token2']) AND LIKE '%prefix,token1,token2,suffix%'
/// and AND-combines it with the existing result node in the DAG.
static const ActionsDAG::Node * optimizeOneLikeOperatorUsingTextIndex(ActionsDAG & dag, const String & result_column_name, const ActionsDAG::Node * like_node, const MergeTreeIndexText * index, const ContextPtr & context)
{
    chassert(like_node->type == ActionsDAG::ActionType::FUNCTION && like_node->children.size() == 2);

    bool is_input_column = like_node->children[0]->type == ActionsDAG::ActionType::INPUT;
    const auto * text_node = is_input_column ? like_node->children[0] : like_node->children[1];
    const auto * const_node = is_input_column ? like_node->children[1] : like_node->children[0];

    const auto * col_const = assert_cast<const ColumnConst *>(const_node->column.get());
    const IColumn & data = col_const->getDataColumn();
    String value = data.getDataAt(0).toString();

    std::vector<String> tokens;
    index->token_extractor->stringLikeToTokens(value.data(), value.size(), tokens);
    if (tokens.empty())
        index->token_extractor->stringToTokens(value.data(), value.size(), tokens);

    auto const_text = makeConstArrayOfTokens(tokens);
    auto & const_text_node = dag.addColumn(const_text);
    auto func_has_all_tokens =  FunctionFactory::instance().get("hasAllTokens", context);

    auto & result_node = dag.findInOutputs(result_column_name);
    ActionsDAG::NodeRawConstPtrs has_tokens_args = { text_node, &const_text_node };
    auto & text_function_node = dag.addFunction(func_has_all_tokens, has_tokens_args, {});

    FunctionOverloadResolverPtr func_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    ActionsDAG::NodeRawConstPtrs and_args = { &result_node, &text_function_node };
    auto & and_node = dag.addFunction(func_and, and_args, result_column_name);

    auto & outputs = dag.getOutputs();
    for (auto & out : outputs)
        if (out == &result_node)
            out = &and_node;

    return &and_node;
}

/// Optimize LIKE operators in the filter step by rewriting them into text-index
/// predicates (hasAllTokens), when an applicable inverted text index exists.
/// This enables fast LIKE queries (e.g. `WHERE col LIKE '%abc%'`) using the
/// inverted text index, significantly reducing full-scan costs.
static void optimizeLikeOperatorsUsingTextIndex(SourceStepWithFilterBase * source_step_with_filter, FilterStep * filter_step)
{
    auto *read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(source_step_with_filter);
    if (!read_from_merge_tree)
        return;

    std::unordered_map<const ActionsDAG::Node*, IndexDescription> like_node_with_indexes;
    auto filter_dag = filter_step->getExpression().clone();
    collectLikeFunctionNodeWithIndexes(filter_dag, read_from_merge_tree, like_node_with_indexes);
    if (like_node_with_indexes.empty())
        return;

    bool updated = false;
    const String & filter_column_name = filter_step->getFilterColumnName();
    for (auto [like_node, index_desc] : like_node_with_indexes)
    {
        auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
        auto * text_index = dynamic_cast<const MergeTreeIndexText *>(index_helper.get());
        chassert(text_index);
        auto result_node = optimizeOneLikeOperatorUsingTextIndex(filter_dag, filter_column_name, like_node, text_index, read_from_merge_tree->getContext());
        if (result_node)
            updated = true;
    }
    if (updated)
        filter_step->getExpression() = std::move(filter_dag);
}

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();
    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            optimizeLikeOperatorsUsingTextIndex(source_step_with_filter, filter_step);
            source_step_with_filter->addFilter(filter_step->getExpression().clone(), filter_step->getFilterColumnName());
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// Note: actually, plan optimizations merge Filter and Expression steps.
            /// Ideally, chain should look like (Expression -> ...) -> (Filter -> ...) -> ReadFromStorage,
            /// So this is likely not needed.
            continue;
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
