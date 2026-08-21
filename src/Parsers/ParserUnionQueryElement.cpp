#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserSubquery().parse(pos, node, expected))
    {
        if (const auto * ast_subquery = node->as<ASTSubquery>())
            node = ast_subquery->children.at(0);

        return true;
    }

    if (ParserSelectQuery().parse(pos, node, expected))
        return true;

    return false;
}

}
