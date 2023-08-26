#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>

namespace DB
{

bool ParserKQLStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);
    ParserSetQuery set_p;

    bool res = query_with_output_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected);

    return res;
}

bool ParserKQLWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery KQL_p;

    ASTPtr query;
    bool parsed = KQL_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    node = std::move(query);
    return true;
}

bool ParserKQLWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // will support union next phase
    ASTPtr KQLQuery;

    if (!ParserKQLQuery().parse(pos, KQLQuery, expected))
        return false;

    if (KQLQuery->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(KQLQuery);
        return true;
    }

    auto list_node = std::make_shared<ASTExpressionList>();
    list_node->children.push_back(KQLQuery);

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    node = select_with_union_query;
    select_with_union_query->list_of_selects = list_node;
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    return true;
}

}
