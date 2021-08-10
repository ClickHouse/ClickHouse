#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserIntersectOrExcept.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

bool ParserIntersectOrExcept::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword intersect_keyword("INTERSECT");
    ParserKeyword except_keyword("EXCEPT");

    ASTs elements;
    ASTIntersectOrExcept::Modes modes;

    auto parse_element = [&]() -> bool
    {
        ASTPtr element;
        if (!ParserSelectWithUnionQuery().parse(pos, element, expected) && !ParserSubquery().parse(pos, element, expected))
            return false;

        elements.push_back(element);
        return true;
    };

    auto parse_separator = [&]() -> bool
    {
        if (!intersect_keyword.ignore(pos))
        {
            if (!except_keyword.ignore(pos))
                return false;

            modes.emplace_back(ASTIntersectOrExcept::Mode::EXCEPT);
            return true;
        }

        modes.emplace_back(ASTIntersectOrExcept::Mode::INTERSECT);
        return true;
    };

    if (!ParserUnionList::parseUtil(pos, parse_element, parse_separator))
        return false;

    if (modes.empty())
        return false;

    auto list_node = std::make_shared<ASTExpressionList>();
    list_node->children = std::move(elements);

    auto intersect_or_except_ast = std::make_shared<ASTIntersectOrExcept>();

    node = intersect_or_except_ast;
    intersect_or_except_ast->list_of_selects = list_node;
    intersect_or_except_ast->children.push_back(intersect_or_except_ast->list_of_selects);
    intersect_or_except_ast->list_of_modes = modes;

    return true;
}

}
