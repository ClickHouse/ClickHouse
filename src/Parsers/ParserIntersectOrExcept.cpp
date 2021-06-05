#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserIntersectOrExcept.h>
#include <Parsers/ParserSelectWithUnionQuery.h>

namespace DB
{
bool ParserIntersectOrExcept::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword intersect_keyword("INTERSECT");
    ParserKeyword except_keyword("EXCEPT");
    ASTPtr left_node;
    ASTPtr right_node;

    auto ast = std::make_shared<ASTIntersectOrExcept>();
    ast->is_except = false;

    if (!ParserSelectWithUnionQuery().parse(pos, left_node, expected) && !ParserSubquery().parse(pos, left_node, expected))
        return false;

    if (!intersect_keyword.ignore(pos))
    {
        if (!except_keyword.ignore(pos))
        {
            return false;
        }
        else
        {
            ast->is_except = true;
        }
    }

    if (!ParserSelectWithUnionQuery().parse(pos, right_node, expected) && !ParserSubquery().parse(pos, right_node, expected))
        return false;

    ast->children.push_back(left_node);
    ast->children.push_back(right_node);

    node = ast;
    return true;
}

}
