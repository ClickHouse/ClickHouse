#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(
    TableExpressionNodePtr table_expression_, bool only_analyze_, StoragePtr replaced_storage_, bool view_source_in_subqueries_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
    , replaced_storage(std::move(replaced_storage_))
    , view_source_in_subqueries(view_source_in_subqueries_)
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_, StoragePtr replaced_storage_, bool view_source_in_subqueries_)
    : only_analyze(only_analyze_)
    , replaced_storage(std::move(replaced_storage_))
    , view_source_in_subqueries(view_source_in_subqueries_)
{}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    QueryAnalyzer analyzer(only_analyze, replaced_storage, view_source_in_subqueries);
    analyzer.resolve(query_tree_node, table_expression, context);
    createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
