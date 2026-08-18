#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/typeid_cast.h>
#include <Core/SettingsEnums.h>
#include <Parsers/SelectUnionMode.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXPECTED_ALL_OR_DISTINCT;
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

void SelectIntersectExceptQueryMatcher::visit(ASTSelectWithUnionQuery & ast, Data & data)
{
    auto union_modes = ast.list_of_modes;

    if (union_modes.empty())
        return;

    auto selects = ast.list_of_selects->children;

    if (union_modes.size() + 1 != selects.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect ASTSelectWithUnionQuery (modes: {}, selects: {})",
                        union_modes.size(), selects.size());

    std::reverse(selects.begin(), selects.end());

    ASTs children = {selects.back()};
    selects.pop_back();
    SelectUnionModes modes;

    for (auto & mode : union_modes)
    {
        /// Rewrite intersect / except mode
        if (mode == SelectUnionMode::EXCEPT_DEFAULT)
        {
            if (data.except_default_mode == SetOperationMode::ALL)
                mode = SelectUnionMode::EXCEPT_ALL;
            else if (data.except_default_mode == SetOperationMode::DISTINCT)
                mode = SelectUnionMode::EXCEPT_DISTINCT;
            else
                throw Exception(ErrorCodes::EXPECTED_ALL_OR_DISTINCT,
                    "Expected ALL or DISTINCT in EXCEPT query, because setting (except_default_mode) is empty");
        }
        else if (mode == SelectUnionMode::INTERSECT_DEFAULT)
        {
            if (data.intersect_default_mode == SetOperationMode::ALL)
                mode = SelectUnionMode::INTERSECT_ALL;
            else if (data.intersect_default_mode == SetOperationMode::DISTINCT)
                mode = SelectUnionMode::INTERSECT_DISTINCT;
            else
                throw Exception(ErrorCodes::EXPECTED_ALL_OR_DISTINCT,
                    "Expected ALL or DISTINCT in INTERSECT query, because setting (intersect_default_mode) is empty");
        }

        switch (mode)
        {
            case SelectUnionMode::EXCEPT_ALL:
            case SelectUnionMode::EXCEPT_DISTINCT:
            {
                auto left = std::make_shared<ASTSelectWithUnionQuery>();
                left->union_mode = mode == SelectUnionMode::EXCEPT_ALL ? SelectUnionMode::UNION_ALL : SelectUnionMode::UNION_DISTINCT;

                left->list_of_selects = std::make_shared<ASTExpressionList>();
                left->children.push_back(left->list_of_selects);
                left->list_of_selects->children = std::move(children);

                left->list_of_modes = std::move(modes);
                modes = {};

                auto right = selects.back();
                selects.pop_back();

                auto except_node = std::make_shared<ASTSelectIntersectExceptQuery>();
                except_node->final_operator = mode == SelectUnionMode::EXCEPT_ALL
                    ? ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL
                    : ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT;
                except_node->children = {left, right};

                children = {except_node};
                break;
            }
            case SelectUnionMode::INTERSECT_ALL:
            case SelectUnionMode::INTERSECT_DISTINCT:
            {
                bool from_except = false;
                const auto * except_ast = typeid_cast<const ASTSelectIntersectExceptQuery *>(children.back().get());
                if (except_ast
                    && (except_ast->final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL
                        || except_ast->final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT))
                    from_except = true;

                ASTPtr left;
                if (from_except)
                {
                    left = std::move(children.back()->children[1]);
                }
                else
                {
                    left = children.back();
                    children.pop_back();
                }

                auto right = selects.back();
                selects.pop_back();

                auto intersect_node = std::make_shared<ASTSelectIntersectExceptQuery>();
                intersect_node->final_operator = mode == SelectUnionMode::INTERSECT_ALL
                    ? ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL
                    : ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT;
                intersect_node->children = {left, right};

                if (from_except)
                    children.back()->children[1] = std::move(intersect_node);
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

    ast.list_of_selects->children = std::move(children);
    ast.list_of_modes = std::move(modes);
}

}
