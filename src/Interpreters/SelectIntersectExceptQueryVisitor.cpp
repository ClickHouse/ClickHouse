#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>


namespace DB
{

/*
 * Note: there is a difference between intersect and except behaviour.
 * `intersect` is supposed to be a part of last SelectQuery, i.e. the sequence with no parenthesis:
 * select 1 union all select 2 except select 1 intersect 2 except select 2 union distinct select 5;
 * is interpreted as:
 * select 1 union all select 2 except (select 1 intersect 2) except select 2 union distinct select 5;
 * Whereas `except` is applied to all union part like:
 * (((select 1 union all select 2) except (select 1 intersect 2)) except select 2) union distinct select 5;
**/

void SelectIntersectExceptQueryMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_intersect_except = ast->as<ASTSelectIntersectExceptQuery>())
    {
        if (select_intersect_except->final_operator != ASTSelectIntersectExceptQuery::Operator::UNKNOWN)
            return;

        data.initialize(select_intersect_except);
        visit(*select_intersect_except, data);
    }
    else if (auto * select_union = ast->as<ASTSelectWithUnionQuery>())
    {
        visit(*select_union, data);
    }
}

void SelectIntersectExceptQueryMatcher::visit(ASTSelectIntersectExceptQuery & ast, Data & data)
{
    /* Example: select 1 intersect select 1 intsect select 1 intersect select 1 intersect select 1;
     *
     * --SelectIntersectExceptQuery                --SelectIntersectExceptQuery
     * ---ExpressionList                           ---ExpressionList
     * ----SelectQuery                             ----SelectIntersectExceptQuery
     * ----SelectQuery                             ------ExpressionList
     * ----SelectQuery                --->         -------SelectIntersectExceptQuery
     * ----SelectQuery                             --------ExpressionList
     *                                             ---------SelectQuery
     *                                             ---------SelectQuery
     *                                             -------SelectQuery
     *                                             ----SelectQuery
    **/

    auto & selects = data.reversed_list_of_selects;

    if (selects.empty())
        return;

    const auto left = selects.back();
    selects.pop_back();
    const auto right = selects.back();
    selects.pop_back();

    auto & operators = data.reversed_list_of_operators;
    const auto current_operator = operators.back();
    operators.pop_back();

    auto list_node = std::make_shared<ASTExpressionList>();
    list_node->children = {left, right};

    if (selects.empty())
    {
        ast.final_operator = current_operator;
        ast.children = {std::move(list_node)};
    }
    else
    {
        auto select_intersect_except = std::make_shared<ASTSelectIntersectExceptQuery>();
        select_intersect_except->final_operator = {current_operator};
        select_intersect_except->list_of_selects = std::move(list_node);
        select_intersect_except->children.push_back(select_intersect_except->list_of_selects);

        selects.emplace_back(std::move(select_intersect_except));
    }

    visit(ast, data);
}

void SelectIntersectExceptQueryMatcher::visit(ASTSelectWithUnionQuery & ast, Data &)
{
    /* Example: select 1 union all select 2 except select 1 except select 2 union distinct select 5;
     *
     * --SelectWithUnionQuery                      --SelectIntersectExceptQuery
     * ---ExpressionList                           ---ExpressionList
     * ----SelectQuery                             ----SelectIntersectExceptQuery
     * ----SelectQuery                             -----ExpressionList
     * ----SelectQuery (except)        --->        ------SelectIntersectExceptQuery
     * ----SelectQuery (except)                    -------ExpressionList
     * ----SelectQuery                             --------SelectWithUnionQuery (select 1 union all select 2)
     *                                             --------SelectQuery (select 1)
     *                                             ------SelectQuery (select 2)
     *                                             -----SelectQuery (select 5)
    **/

    auto & union_modes = ast.list_of_modes;

    if (union_modes.empty())
        return;

    auto selects = std::move(ast.list_of_selects->children);

    if (union_modes.size() + 1 != selects.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Incorrect ASTSelectWithUnionQuery (modes: {}, selects: {})",
                        union_modes.size(), selects.size());

    std::reverse(selects.begin(), selects.end());

    ASTs children = {selects.back()};
    selects.pop_back();
    ASTSelectWithUnionQuery::UnionModes modes;

    for (const auto & mode : union_modes)
    {
        /// Flatten all previous selects into ASTSelectIntersectQuery
        if (mode == ASTSelectWithUnionQuery::Mode::EXCEPT)
        {
            auto left = std::make_shared<ASTSelectWithUnionQuery>();
            left->union_mode = ASTSelectWithUnionQuery::Mode::ALL;

            left->list_of_selects = std::make_shared<ASTExpressionList>();
            left->children.push_back(left->list_of_selects);
            left->list_of_selects->children = std::move(children);

            left->list_of_modes = std::move(modes);
            modes = {};

            auto right = selects.back();
            selects.pop_back();

            auto list_node = std::make_shared<ASTExpressionList>();
            list_node->children = {left, right};

            auto select_intersect_except = std::make_shared<ASTSelectIntersectExceptQuery>();
            select_intersect_except->final_operator = {ASTSelectIntersectExceptQuery::Operator::EXCEPT};
            select_intersect_except->children.emplace_back(std::move(list_node));
            select_intersect_except->list_of_selects = std::make_shared<ASTExpressionList>();
            select_intersect_except->list_of_selects->children.push_back(select_intersect_except->children[0]);

            children = {select_intersect_except};
        }
        else if (!selects.empty())
        {
            auto right = selects.back();
            selects.pop_back();
            children.emplace_back(std::move(right));
            modes.push_back(mode);
        }
    }

    if (!selects.empty())
    {
        auto right = selects.back();
        selects.pop_back();
        children.emplace_back(std::move(right));
    }

    ast.union_mode = ASTSelectWithUnionQuery::Mode::Unspecified;
    ast.list_of_selects->children = std::move(children);
    ast.list_of_modes = std::move(modes);
}

}
