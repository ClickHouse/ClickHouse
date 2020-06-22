#include <Parsers/ParserExplainQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>

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
    }
    else
        return false;

    auto explain_query = std::make_shared<ASTExplainQuery>(kind, old_syntax);

    ParserSelectWithUnionQuery select_p;
    if (!select_p.parse(pos, explain_query->getExplainedQuery(), expected))
        return false;

    node = std::move(explain_query);
    return true;
}

}
