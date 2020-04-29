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
            getSelectsFromUnionListNode(child, selects);

        return;
    }

    selects.push_back(std::move(ast_select));
}


bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;

    ParserList parser_all(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
    ParserList parser_distinct(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION DISTINCT"), false);
    if (parser_all.parse(pos, list_node, expected)) {
        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

        node = select_with_union_query;
        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
        select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

        // flatten inner union query
        for (auto & child : list_node->children)
            getSelectsFromUnionListNode(child, select_with_union_query->list_of_selects->children);
    } else if (parser_all.parse(pos, list_node, expected)) {
        // distinct parser
    }
    else
        return false;

    return true;
}

}
