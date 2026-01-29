#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto old_pos = pos;

    if (ParserSubquery().parse(pos, node, expected))
    {
        if (const auto * ast_subquery = node->as<ASTSubquery>())
            node = ast_subquery->children.at(0);
        return true;
    }

    pos = old_pos;

    if (ParserSelectQuery().parse(pos, node, expected))
        return true;

    pos = old_pos;

    return false;
}

}
