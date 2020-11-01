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

    ParserUnionList parser(
        std::make_unique<ParserUnionQueryElement>(),
        std::make_unique<ParserKeyword>("UNION"),
        std::make_unique<ParserKeyword>("ALL"),
        std::make_unique<ParserKeyword>("DISTINCT"));

    if (!parser.parse(pos, list_node, expected))
        return false;

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    node = select_with_union_query;
    select_with_union_query->list_of_selects = list_node;
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    select_with_union_query->union_modes = parser.getUnionModes();

    /// NOTE: We cann't flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELETC 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`
    return true;
}

}
