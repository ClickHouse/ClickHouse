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
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTIdentifier.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
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

AlternativeKeyExpressionPtr getAlternativeIndexExpressionForAnalyzer(const IndexDescription & index, const ContextPtr & context)
{
    /// The rewrites we reproduce here are applied by the analyzer's query tree passes only.
    if (!context->getSettingsRef()[Setting::allow_experimental_analyzer])
        return nullptr;

    if (!index.expression_list_ast || !index.expression)
        return nullptr;

    /// An index on plain columns cannot be named differently by the rewrites.
    bool all_columns_are_identifiers = true;
    for (const auto & child : index.expression_list_ast->children)
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

        /// Resolve the index expressions against a dummy storage with the index source columns,
        /// running the same query tree passes (with the same settings) the query itself goes through.
        auto source_columns = index.expression->getRequiredColumnsWithTypes();
        auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(source_columns));
        auto table_expression = std::make_shared<TableNode>(storage, execution_context);

        auto query_node = std::make_shared<QueryNode>(execution_context);
        /// The description is shared through the metadata snapshot, so do not let the resolution
        /// touch its AST.
        query_node->getProjectionNode() = buildQueryTree(index.expression_list_ast->clone(), execution_context);
        query_node->getJoinTreeNode() = table_expression;

        QueryTreeNodePtr query_tree = query_node;
        QueryTreePassManager query_tree_pass_manager(execution_context);
        addQueryTreePasses(query_tree_pass_manager);
        query_tree_pass_manager.run(query_tree);

        auto projection_node = query_tree->as<QueryNode &>().getProjectionNode();

        GlobalPlannerContextPtr global_planner_context
            = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
        auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});
        collectSetsAndSourceColumns(projection_node, planner_context, /*keep_alias_columns=*/ false);

        ColumnNodePtrWithHashSet empty_correlated_columns_set;
        auto [dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
            projection_node,
            /*input_columns=*/ {},
            planner_context,
            empty_correlated_columns_set,
            /*use_column_identifier_as_action_node_name=*/ false);
        correlated_subtrees.assertEmpty("in a skip index expression");

        const auto & outputs = dag.getOutputs();
        if (outputs.size() != index.column_names.size())
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
            has_difference |= (result[i] != index.column_names[i]);
        }

        if (!has_difference)
            return nullptr;

        auto alternative_key = std::make_shared<AlternativeKeyExpression>();
        alternative_key->column_names = std::move(result);
        /// The expression carries the rewritten form of the index expressions; it is only searched
        /// by index analysis, never executed.
        alternative_key->expression = std::make_shared<ExpressionActions>(std::move(canonical_dag), ExpressionActionsSettings(execution_context));
        return alternative_key;
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MergeTreeIndexAnalyzerNames"),
            fmt::format("Cannot compute the alternative form of the expression of index {}, matching by the original names only", index.name),
            LogsLevel::debug);
        return nullptr;
    }
}

}
