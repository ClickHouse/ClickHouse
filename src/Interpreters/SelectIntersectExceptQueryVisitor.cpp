#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>


namespace DB
{

void SelectIntersectExceptQueryMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_intersect_except = ast->as<ASTSelectIntersectExceptQuery>())
    {
        std::cerr << "\n\nSelectIntersectExceptVisitor BEFORE:\n" << ast->dumpTree() << std::endl;
        data.initialize(select_intersect_except);
        visit(*select_intersect_except, data);
        std::cerr << "\n\nSelectIntersectExceptVisitor AFTER:\n" << ast->dumpTree() << std::endl;
    }
}

void SelectIntersectExceptQueryMatcher::visit(ASTSelectIntersectExceptQuery & ast, Data & data)
{
    /* Example: select 1 intersect select 1 intsect select 1 intersect select 1 intersect select 1;
     *
     * --SelectIntersectExceptQuery                --SelectIntersectExceptQuery
     * ---expressionlist                           ---ExpressionList
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
        select_intersect_except->children.emplace_back(std::move(list_node));

        selects.emplace_back(std::move(select_intersect_except));
    }

    visit(ast, data);
}

// void SelectIntersectExceptQueryVisitor::visit(ASTSelectWithUnionQuery & ast, Data & data)
// {
//     auto & union_modes = ast.list_of_modes;
//     ASTs selects;
//     auto & select_list = ast.list_of_selects->children;
//
//
//     // reverse children list
//     std::reverse(selects.begin(), selects.end());
//
//     ast.is_normalized = true;
//     ast.union_mode = ASTSelectWithUnionQuery::Mode::ALL;
//
//     ast.list_of_selects->children = std::move(selects);
// }
}
