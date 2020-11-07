#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{
static void getSelectsFromUnionListNode(ASTPtr & ast_select, ASTs & selects)
{
    if (auto * inner_union = ast_select->as<ASTSelectWithUnionQuery>())
    {
        /// We need flatten from last to first
        for (auto child = inner_union->list_of_selects->children.rbegin(); child != inner_union->list_of_selects->children.rend(); ++child)
            getSelectsFromUnionListNode(*child, selects);

        return;
    }

    selects.push_back(std::move(ast_select));
}

void normalizeSelectList(ASTs & select_list, const ASTSelectWithUnionQuery::UnionModes & union_modes, ASTs & selects)
{
    int i;
    for (i = union_modes.size() - 1; i >= 0; --i)
    {
        if (union_modes[i] == ASTSelectWithUnionQuery::Mode::ALL)
        {
            if (auto * inner_union = select_list[i + 1]->as<ASTSelectWithUnionQuery>())
            {
                /// If inner_union is an UNION ALL list, just lift up
                if (inner_union->union_mode == ASTSelectWithUnionQuery::Mode::ALL)
                {
                    for (auto child = inner_union->list_of_selects->children.rbegin();
                         child != inner_union->list_of_selects->children.rend();
                         ++child)
                        selects.push_back(std::move(*child));
                }
                /// inner_union is an UNION DISTINCT list,
                // we cann't lift up
                else
                    selects.push_back(std::move(select_list[i + 1]));
            }
            else
                selects.push_back(std::move(select_list[i + 1]));
        }
        /// flatten all left nodes and current node to a UNION DISTINCT list
        else if (union_modes[i] == ASTSelectWithUnionQuery::Mode::DISTINCT)
        {
            auto distinct_list = std::make_shared<ASTSelectWithUnionQuery>();
            distinct_list->list_of_selects = std::make_shared<ASTExpressionList>();
            distinct_list->children.push_back(distinct_list->list_of_selects);
            for (int j = i + 1; j >= 0; j--)
            {
                getSelectsFromUnionListNode(select_list[j], distinct_list->list_of_selects->children);
            }
            distinct_list->union_mode = ASTSelectWithUnionQuery::Mode::DISTINCT;
            // Reverse children list
			std::reverse(distinct_list->list_of_selects->children.begin(), distinct_list->list_of_selects->children.end());
            selects.push_back(std::move(distinct_list));
            return;
        }
    }
    /// No UNION DISTINCT or only one SELECT in select_list
    if (i == -1)
    {
        if (auto * inner_union = select_list[0]->as<ASTSelectWithUnionQuery>())
        {
            /// If inner_union is an UNION ALL list, just lift it up
            if (inner_union->union_mode == ASTSelectWithUnionQuery::Mode::ALL)
            {
                    for (auto child = inner_union->list_of_selects->children.rbegin();
                         child != inner_union->list_of_selects->children.rend();
                         ++child)
                        selects.push_back(std::move(*child));
            }
            /// inner_union is an UNION DISTINCT list,
            // we cann't lift it up
            else
                selects.push_back(std::move(select_list[i + 1]));
        }
        else
            selects.push_back(std::move(select_list[0]));
    }
}

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

    /// NOTE: We cann't simply flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELETC 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`

    /// Before normalize, if we got only one child which is ASTSelectWithUnionQuery, just lift it up
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
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    auto union_modes = parser.getUnionModes();

    normalizeSelectList(expr_list.children, union_modes, select_with_union_query->list_of_selects->children);
    /// We need reverse children list
    std::reverse(select_with_union_query->list_of_selects->children.begin(), select_with_union_query->list_of_selects->children.end());

    select_with_union_query->union_mode = ASTSelectWithUnionQuery::Mode::ALL;

    /// After normalize, if we only have one ASTSelectWithUnionQuery child, lift if up
    if (select_with_union_query->list_of_selects->children.size() == 1)
    {
        if (select_with_union_query->list_of_selects->children.at(0)->as<ASTSelectWithUnionQuery>())
        {
            node = std::move(select_with_union_query->list_of_selects->children.at(0));
        }
    }

    return true;
}

}
