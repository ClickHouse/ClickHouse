#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
    , used_column_names()
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_, UsedColumns used_column_names_)
    : only_analyze(only_analyze_)
    , used_column_names(std::move(used_column_names_))
{}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    QueryAnalyzer analyzer(only_analyze, std::move(used_column_names));
    analyzer.resolve(query_tree_node, table_expression, context);
    createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
