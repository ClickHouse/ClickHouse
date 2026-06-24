#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <optional>

namespace ProfileEvents
{
    extern const Event QueryAnalysisMicroseconds;
}

namespace DB
{

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_, bool only_analyze_, bool is_top_level_)
    : table_expression(std::move(table_expression_))
    , only_analyze(only_analyze_)
    , is_top_level(is_top_level_)
{}

QueryAnalysisPass::QueryAnalysisPass(bool only_analyze_, bool is_top_level_)
    : only_analyze(only_analyze_)
    , is_top_level(is_top_level_)
{}

void QueryAnalysisPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// For nested (re-entrant) analysis is_top_level is false, so this is not double-counted;
    /// its time is folded into the still-live outer QueryAnalysisMicroseconds timer instead.
    std::optional<ProfileEventTimeIncrement<Microseconds>> watch;
    if (is_top_level)
        watch.emplace(ProfileEvents::QueryAnalysisMicroseconds);

    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, table_expression, context);
    createUniqueAliasesIfNecessary(query_tree_node, context);
}

}
