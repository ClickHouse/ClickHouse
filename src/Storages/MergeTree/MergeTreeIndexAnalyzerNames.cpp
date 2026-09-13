#include <Storages/MergeTree/MergeTreeIndexAnalyzerNames.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Core/LogsLevel.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageDummy.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace
{

/// Reproduce the rewrites of the query analyzer (the query tree passes) on the key expressions.
ActionsDAG buildRewrittenExpressionWithAnalyzer(
    const ASTPtr & expression_list_ast, const ExpressionActionsPtr & expression, const ContextMutablePtr & context)
{
    /// Resolve the key expressions against a dummy storage with the key's source columns,
    /// running the same query tree passes (with the same settings) the query itself goes through.
    auto source_columns = expression->getRequiredColumnsWithTypes();
    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(source_columns));
    auto table_expression = std::make_shared<TableNode>(storage, context);

    auto query_node = std::make_shared<QueryNode>(context);
    /// The description is shared through the metadata snapshot, so do not let the resolution
    /// touch its AST.
    query_node->getProjectionNode() = buildQueryTree(expression_list_ast->clone(), context);
    query_node->getJoinTreeNode() = table_expression;

    QueryTreeNodePtr query_tree = query_node;
    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);
    query_tree_pass_manager.run(query_tree);

    auto projection_node = query_tree->as<QueryNode &>().getProjectionNode();

    GlobalPlannerContextPtr global_planner_context
        = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(projection_node, planner_context, /*keep_alias_columns=*/ false);

    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    auto [dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
        projection_node,
        /*input_columns=*/ {},
        planner_context,
        empty_correlated_columns_set,
        /*use_column_identifier_as_action_node_name=*/ false);
    correlated_subtrees.assertEmpty("in an index or key expression");

    return std::move(dag);
}

/// Reproduce the rewrites of the legacy analyzer (the AST optimizations of `TreeRewriter`, e.g.
/// `TreeOptimizer::optimizeIf`, which rewrites `multiIf` with a single condition to `if`) on the
/// key expressions. They are applied to a `SELECT` query, so the key expressions are analyzed
/// as the projection of a synthetic one, the same way the key expressions themselves are
/// analyzed in `IndexDescription::initExpressionInfo` and `KeyDescription::getSortingKeyFromAST`.
ActionsDAG buildRewrittenExpressionWithLegacyAnalyzer(
    const ASTPtr & expression_list_ast, const ExpressionActionsPtr & expression, const ContextMutablePtr & context)
{
    auto source_columns = expression->getRequiredColumnsWithTypes();

    auto select_query = make_intrusive<ASTSelectQuery>();
    /// The description is shared through the metadata snapshot, so do not let the rewrite touch its AST.
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, expression_list_ast->clone());

    ASTPtr query = select_query;
    auto syntax_result = TreeRewriter(context).analyzeSelect(query, TreeRewriterResult(source_columns));

    auto expression_list = select_query->select();
    auto actions = ExpressionAnalyzer(expression_list, syntax_result, context).getActions(true);
    return actions->getActionsDAG().clone();
}

/// The generic computation of the alternative (rewritten) form of a list of key expressions: it is
/// the same for a skip index, a primary key and a partition key.
AlternativeKeyExpressionPtr getAlternativeExpression(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & column_names,
    const String & description_for_log,
    const ContextPtr & context)
{
    if (!expression_list_ast || !expression)
        return nullptr;

    /// A key on plain columns cannot be named differently by the rewrites.
    bool all_columns_are_identifiers = true;
    for (const auto & child : expression_list_ast->children)
    {
        if (!child || !child->as<ASTIdentifier>())
        {
            all_columns_are_identifiers = false;
            break;
        }
    }
    if (all_columns_are_identifiers)
        return nullptr;

    /// The alternative names are a best-effort improvement of index analysis: on any failure
    /// fall back to matching by the original names only.
    try
    {
        auto execution_context = Context::createCopy(context);

        auto dag = execution_context->getSettingsRef()[Setting::allow_experimental_analyzer]
            ? buildRewrittenExpressionWithAnalyzer(expression_list_ast, expression, execution_context)
            : buildRewrittenExpressionWithLegacyAnalyzer(expression_list_ast, expression, execution_context);

        const auto & outputs = dag.getOutputs();
        if (outputs.size() != column_names.size())
            return nullptr;

        /// Re-clone the rewritten DAG with the same canonicalization index analysis (RPNBuilder over
        /// `ActionsDAGWithInversionPushDown`) applies to the query filter expressions (e.g. of constant
        /// names), so that every (sub)expression node is named the way the matching filter node is.
        auto canonical_dag = cloneDAGForIndexAnalysisNames(outputs, context);
        const auto & canonical_outputs = canonical_dag.getOutputs();

        Names result(canonical_outputs.size());
        RPNBuilderTreeContext tree_context(context);
        bool has_difference = false;
        for (size_t i = 0; i < canonical_outputs.size(); ++i)
        {
            result[i] = RPNBuilderTreeNode(canonical_outputs[i], tree_context).getColumnName();
            has_difference |= (result[i] != column_names[i]);
        }

        if (!has_difference)
            return nullptr;

        auto alternative_key = std::make_shared<AlternativeKeyExpression>();
        alternative_key->column_names = std::move(result);
        /// The expression carries the rewritten form of the key expressions; it is only searched
        /// by index analysis, never executed.
        alternative_key->expression = std::make_shared<ExpressionActions>(std::move(canonical_dag), ExpressionActionsSettings(execution_context));
        return alternative_key;
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MergeTreeIndexAnalyzerNames"),
            fmt::format("Cannot compute the alternative form of {}, matching by the original names only", description_for_log),
            LogsLevel::debug);
        return nullptr;
    }
}

}

AlternativeKeyExpressionPtr getAlternativeIndexExpression(const IndexDescription & index, const ContextPtr & context)
{
    return getAlternativeExpression(
        index.expression_list_ast, index.expression, index.column_names, fmt::format("the expression of index {}", index.name), context);
}

AlternativeKeyExpressionPtr getAlternativeKeyExpression(const KeyDescription & key, const ContextPtr & context)
{
    return getAlternativeExpression(key.expression_list_ast, key.expression, key.column_names, "the key expression", context);
}

AlternativeKeyExpressionPtr LazyAlternativeKeyExpression::get(const KeyDescription & key, const ContextPtr & context) const
{
    std::call_once(initialized, [&] { alternative_key = getAlternativeKeyExpression(key, context); });
    return alternative_key;
}

RewriteAwareIndexConditionFactory::RewriteAwareIndexConditionFactory(MergeTreeIndexPtr index_helper_)
    : index_helper(std::move(index_helper_))
    , minmax_index(typeid_cast<const MergeTreeIndexMinMax *>(index_helper.get()))
{
}

MergeTreeIndexConditionPtr RewriteAwareIndexConditionFactory::create(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    if (!predicate)
        return nullptr;

    if (minmax_index)
        std::call_once(alternative_key_initialized, [&] { alternative_key = getAlternativeIndexExpression(index_helper->index, context); });

    if (alternative_key)
        return minmax_index->createIndexConditionWithAlternativeKey(predicate, context, alternative_key);
    return index_helper->createIndexCondition(predicate, context);
}

}
