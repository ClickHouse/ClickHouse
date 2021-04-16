#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserIntersectOrExcept.h>
#include <Parsers/ParserSelectQuery.h>

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

    if (!ParserSelectQuery().parse(pos, left_node, expected) && !ParserSubquery().parse(pos, left_node, expected))
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

    if (!ParserSelectQuery().parse(pos, right_node, expected) && !ParserSubquery().parse(pos, right_node, expected))
        return false;

    if (const auto * ast_subquery = left_node->as<ASTSubquery>())
        left_node = ast_subquery->children.at(0);
    if (const auto * ast_subquery = right_node->as<ASTSubquery>())
        right_node = ast_subquery->children.at(0);

    ast->children.push_back(left_node);
    ast->children.push_back(right_node);

    node = ast;
    return true;
}

}
