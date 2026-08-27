#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;
    ParserUnionList parser;

    if (!parser.parse(pos, list_node, expected))
        return false;

    /// NOTE: We can't simply flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`

    /// If we got only one child which is ASTSelectWithUnionQuery, just lift it up
    auto & expr_list = list_node->as<ASTExpressionList &>();
    if (expr_list.children.size() == 1)
    {
        if (expr_list.children.at(0)->as<ASTSelectWithUnionQuery>())
        {
            node = std::move(expr_list.children.at(0));
            return true;
        }
    }

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    node = select_with_union_query;
    select_with_union_query->list_of_selects = list_node;
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    select_with_union_query->list_of_modes = parser.getUnionModes();

    return true;
}

}
