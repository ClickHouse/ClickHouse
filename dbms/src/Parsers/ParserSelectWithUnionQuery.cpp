#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ASTPtr list_node;

    ParserList parser(std::make_unique<ParserSelectQuery>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
    if (!parser.parse(pos, end, list_node, max_parsed_pos, expected))
        return false;

    node = std::make_shared<ASTSelectWithUnionQuery>(list_node->range);
    node->children = list_node->children;

    return true;
}

}
