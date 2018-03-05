#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;

    ParserList parser(std::make_unique<ParserSelectQuery>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
    if (!parser.parse(pos, list_node, expected))
        return false;

    auto res = std::make_shared<ASTSelectWithUnionQuery>();

    res->list_of_selects = std::move(list_node);
    res->children.push_back(res->list_of_selects);

    node = res;
    return true;
}

}
