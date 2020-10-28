#include <list>
#include <memory>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>


namespace DB
{
static void getSelectsFromUnionListNode(ASTPtr & ast_select, ASTs & selects)
{
    if (auto * inner_union = ast_select->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : inner_union->list_of_selects->children)
        {
            getSelectsFromUnionListNode(child, selects);
        }

        return;
    }

    selects.push_back(std::move(ast_select));
}

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::cout << "\n\n in ParserSelectWithUnionQuery\n\n";
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
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    select_with_union_query->list_of_selects->children.insert(
        select_with_union_query->list_of_selects->children.begin(), list_node->children.begin(), list_node->children.end());
    select_with_union_query->union_modes = parser.getUnionModes();

    /// NOTE: We cann't simply flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELETC 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`
    /// We can use a non-flatten AST to help build QueryPlan in InterpreterSelectWithUnionQuery

    select_with_union_query->flatten_nodes_list = std::make_shared<ASTExpressionList>();

    for (auto & child : list_node->children)
    {
        getSelectsFromUnionListNode(child, select_with_union_query->flatten_nodes_list->children);
    }
    std::cout << "\n\n after ParserSelectWithUnionQuery\n\n";
    std::cout << "\n\n flatten_nodes.size =" << select_with_union_query->flatten_nodes_list->children.size() << "\n\n";

    return true;
}

}
