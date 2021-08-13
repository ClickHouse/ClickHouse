#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectIntersectExceptQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

bool ParserSelectIntersectExceptQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword intersect_keyword("INTERSECT");

    ASTs elements;
    ASTSelectIntersectExceptQuery::Operators operators;

    auto parse_element = [&]() -> bool
    {
        ASTPtr element;
        if (!ParserSelectQuery().parse(pos, element, expected) && !ParserSubquery().parse(pos, element, expected))
            return false;

        elements.push_back(element);
        return true;
    };

    auto parse_separator = [&]() -> bool
    {
        if (!intersect_keyword.ignore(pos))
            return false;

        operators.emplace_back(ASTSelectIntersectExceptQuery::Operator::INTERSECT);
        return true;
    };

    if (!ParserUnionList::parseUtil(pos, parse_element, parse_separator))
        return false;

    if (operators.empty() || elements.empty())
        return false;

    if (operators.size() + 1 != elements.size())
        return false;

    auto list_node = std::make_shared<ASTExpressionList>();
    list_node->children = std::move(elements);

    auto intersect_or_except_ast = std::make_shared<ASTSelectIntersectExceptQuery>();

    node = intersect_or_except_ast;
    intersect_or_except_ast->list_of_selects = list_node;
    intersect_or_except_ast->children.push_back(intersect_or_except_ast->list_of_selects);
    intersect_or_except_ast->list_of_operators = operators;

    return true;
}

}
