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
    if (ASTSelectWithUnionQuery * inner_union = typeid_cast<ASTSelectWithUnionQuery *>(ast_select.get()))
    {
        for (auto & child : inner_union->list_of_selects->children)
            getSelectsFromUnionListNode(child, selects);

        return;
    }

    selects.push_back(std::move(ast_select));
}


bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;

    ParserList parser(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
    if (!parser.parse(pos, list_node, expected))
        return false;

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    node = select_with_union_query;
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    // flatten inner union query
    for (auto & child : list_node->children)
        getSelectsFromUnionListNode(child, select_with_union_query->list_of_selects->children);

    return true;
}

}
