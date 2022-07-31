#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/*
 * Note: there is a difference between intersect and except behaviour.
 * `intersect` is supposed to be a part of the last SelectQuery, i.e. the sequence with no parenthesis:
 * select 1 union all select 2 except select 1 intersect 2 except select 2 union distinct select 5;
 * is interpreted as:
 * select 1 union all select 2 except (select 1 intersect 2) except select 2 union distinct select 5;
 * Whereas `except` is applied to all left union part like:
 * (((select 1 union all select 2) except (select 1 intersect 2)) except select 2) union distinct select 5;
**/

void SelectIntersectExceptQueryMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_union = ast->as<ASTSelectWithUnionQuery>())
        visit(*select_union, data);
}

void SelectIntersectExceptQueryMatcher::visit(ASTSelectWithUnionQuery & ast, Data &)
{
    const auto & union_modes = ast.list_of_modes;

    if (union_modes.empty())
        return;

    auto selects = std::move(ast.list_of_selects->children);

    if (union_modes.size() + 1 != selects.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect ASTSelectWithUnionQuery (modes: {}, selects: {})",
                        union_modes.size(), selects.size());

    std::reverse(selects.begin(), selects.end());

    ASTList children = {selects.back()};
    selects.pop_back();
    SelectUnionModes modes;

    for (const auto & mode : union_modes)
    {
        switch (mode)
        {
            case SelectUnionMode::EXCEPT:
            {
                auto left = std::make_shared<ASTSelectWithUnionQuery>();
                left->union_mode = SelectUnionMode::ALL;

                left->list_of_selects = std::make_shared<ASTExpressionList>();
                left->children.push_back(left->list_of_selects);
                left->list_of_selects->children = std::move(children);

                left->list_of_modes = std::move(modes);
                modes = {};

                auto right = selects.back();
                selects.pop_back();

                auto except_node = std::make_shared<ASTSelectIntersectExceptQuery>();
                except_node->final_operator = ASTSelectIntersectExceptQuery::Operator::EXCEPT;
                except_node->children = {left, right};

                children = {except_node};
                break;
            }
            case SelectUnionMode::INTERSECT:
            {
                bool from_except = false;
                const auto * except_ast = typeid_cast<const ASTSelectIntersectExceptQuery *>(children.back().get());
                if (except_ast && (except_ast->final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT))
                    from_except = true;

                ASTPtr left;
                if (from_except)
                {
                    left = std::move(children.back()->children.back());
                }
                else
                {
                    left = children.back();
                    children.pop_back();
                }

                auto right = selects.back();
                selects.pop_back();

                auto intersect_node = std::make_shared<ASTSelectIntersectExceptQuery>();
                intersect_node->final_operator = ASTSelectIntersectExceptQuery::Operator::INTERSECT;
                intersect_node->children = {left, right};

                if (from_except)
                    children.back()->children.back() = std::move(intersect_node);
                else
                    children.push_back(std::move(intersect_node));

                break;
            }
            default:
            {
                auto right = selects.back();
                selects.pop_back();
                children.emplace_back(std::move(right));
                modes.push_back(mode);
                break;
            }
        }
    }

    if (!selects.empty())
    {
        auto right = selects.back();
        selects.pop_back();
        children.emplace_back(std::move(right));
    }

    ast.union_mode = SelectUnionMode::Unspecified;
    ast.list_of_selects->children = std::move(children);
    ast.list_of_modes = std::move(modes);
}

}
