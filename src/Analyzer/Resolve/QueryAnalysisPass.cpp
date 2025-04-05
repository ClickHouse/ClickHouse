#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_, bool no_aliasing_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
    , no_aliasing(no_aliasing_)
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_, bool no_aliasing_)
    : only_analyze(only_analyze_)
    , no_aliasing(no_aliasing_)
{}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, table_expression, context);
    if (!no_aliasing)
        createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
