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

    ParserList parser_union(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION"), false);
	ParserList parser_union_all(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
	ParserList parser_union_distinct(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION DISTINCT"), false);

    auto begin = pos;
    auto current_expected = expected;
    ASTSelectWithUnionQuery::Mode union_mode = ASTSelectWithUnionQuery::Mode::ALL;

    /// Parser SELECT lists and UNION type, must have UNION
    auto union_parser = [&](auto & parser, auto mode) {
        if (!parser.parse(pos, list_node, expected))
        {
            pos = begin;
            expected = current_expected;
            return false;
        }
        /// number of SELECT lists should not less than 2
        if (list_node->children.size() < 2)
        {
            pos = begin;
            expected = current_expected;
            return false;
        }
        union_mode = mode;
        return true;
    };

    /// We first parse: SELECT ... UNION SELECT ...
    ///                 SELECT ... UNION ALL SELECT ...
    ///                 SELECT ... UNION DISTINCT SELECT ...
    if (!union_parser(parser_union, ASTSelectWithUnionQuery::Mode::Unspecified)
        && !union_parser(parser_union_all, ASTSelectWithUnionQuery::Mode::ALL)
        && !union_parser(parser_union_distinct, ASTSelectWithUnionQuery::Mode::DISTINCT))
    {
        /// If above parse failed, we back to parse SELECT without UNION
        if (!parser_union.parse(pos, list_node, expected))
            return false;
    }

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    node = select_with_union_query;
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    select_with_union_query->mode = union_mode;

    // flatten inner union query
    for (auto & child : list_node->children)
        getSelectsFromUnionListNode(child, select_with_union_query->list_of_selects->children);

    return true;
}

}
