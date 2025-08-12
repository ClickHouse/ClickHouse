#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_) : only_analyze(only_analyze_) {}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    LOG_DEBUG(getLogger("QueryAnalysisPass"),
        "Running query analysis pass on query tree node: {}, only analyze: {} context query kind: {}",
        query_tree_node->formatConvertedASTForErrorMessage(), only_analyze, toString(context->getClientInfo().query_kind));

    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, table_expression, context);
    createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
