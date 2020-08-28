#include <Parsers/ParserExplainQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool ParserExplainQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTExplainQuery::ExplainKind kind;
    bool old_syntax = false;

    ParserKeyword s_ast("AST");
    ParserKeyword s_analyze("ANALYZE");
    ParserKeyword s_explain("EXPLAIN");
    ParserKeyword s_syntax("SYNTAX");
    ParserKeyword s_pipeline("PIPELINE");
    ParserKeyword s_plan("PLAN");

    if (enable_debug_queries && s_ast.ignore(pos, expected))
    {
        old_syntax = true;
        kind = ASTExplainQuery::ExplainKind::ParsedAST;
    }
    else if (enable_debug_queries && s_analyze.ignore(pos, expected))
    {
        old_syntax = true;
        kind = ASTExplainQuery::ExplainKind::AnalyzedSyntax;
    }
    else if (s_explain.ignore(pos, expected))
    {
        kind = ASTExplainQuery::QueryPlan;

        if (s_ast.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::ParsedAST;
        else if (s_syntax.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::AnalyzedSyntax;
        else if (s_pipeline.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPipeline;
        else if (s_plan.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPlan;
    }
    else
        return false;

    auto explain_query = std::make_shared<ASTExplainQuery>(kind, old_syntax);

    {
        ASTPtr settings;
        ParserSetQuery parser_settings(true);

        auto begin = pos;
        if (parser_settings.parse(pos, settings, expected))
            explain_query->setSettings(std::move(settings));
        else
            pos = begin;
    }

    ParserSelectWithUnionQuery select_p;
    ASTPtr query;
    if (!select_p.parse(pos, query, expected))
        return false;

    explain_query->setExplainedQuery(std::move(query));

    node = std::move(explain_query);
    return true;
}

}
