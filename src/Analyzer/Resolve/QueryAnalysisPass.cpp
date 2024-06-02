#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueTableAliases.h>

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_) : only_analyze(only_analyze_) {}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, table_expression, context);
    createUniqueTableAliases(query_tree_node, table_expression, context);
}

}
