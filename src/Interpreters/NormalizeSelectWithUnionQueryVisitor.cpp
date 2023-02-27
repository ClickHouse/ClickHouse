#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXPECTED_ALL_OR_DISTINCT;
    extern const int LOGICAL_ERROR;
}

void NormalizeSelectWithUnionQueryMatcher::getSelectsFromUnionListNode(ASTPtr ast_select, ASTs & selects)
{
    if (const auto * inner_union = ast_select->as<ASTSelectWithUnionQuery>())
    {
        for (const auto & child : inner_union->list_of_selects->children)
            getSelectsFromUnionListNode(child, selects);

        return;
    }

    selects.push_back(ast_select);
}

void NormalizeSelectWithUnionQueryMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_union = ast->as<ASTSelectWithUnionQuery>())
        visit(*select_union, data);
}

void NormalizeSelectWithUnionQueryMatcher::visit(ASTSelectWithUnionQuery & ast, Data & data)
{
    auto & union_modes = ast.list_of_modes;
    ASTs selects;
    const auto & select_list = ast.list_of_selects->children;

    if (select_list.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty list of selects for ASTSelectWithUnionQuery");

    /// Since nodes are traversed from bottom to top, we can also collect union modes from children up to parents.
    SelectUnionModesSet current_set_of_modes;
    bool distinct_found = false;

    int i;
    for (i = union_modes.size() - 1; i >= 0; --i)
    {
        current_set_of_modes.insert(union_modes[i]);
        if (const auto * union_ast = typeid_cast<const ASTSelectWithUnionQuery *>(select_list[i + 1].get()))
        {
            const auto & current_select_modes = union_ast->set_of_modes;
            current_set_of_modes.insert(current_select_modes.begin(), current_select_modes.end());
        }

        if (distinct_found)
            continue;

        /// Rewrite UNION Mode
        if (union_modes[i] == SelectUnionMode::Unspecified)
        {
            if (data.union_default_mode == UnionMode::ALL)
                union_modes[i] = SelectUnionMode::ALL;
            else if (data.union_default_mode == UnionMode::DISTINCT)
                union_modes[i] = SelectUnionMode::DISTINCT;
            else
                throw Exception(
                    "Expected ALL or DISTINCT in SelectWithUnion query, because setting (union_default_mode) is empty",
                    DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT);
        }

        if (union_modes[i] == SelectUnionMode::ALL)
        {
            if (auto * inner_union = select_list[i + 1]->as<ASTSelectWithUnionQuery>();
                inner_union && inner_union->union_mode == SelectUnionMode::ALL)
            {
                /// Inner_union is an UNION ALL list, just lift up
                for (auto child = inner_union->list_of_selects->children.rbegin(); child != inner_union->list_of_selects->children.rend();
                     ++child)
                    selects.push_back(*child);
            }
            else
                selects.push_back(select_list[i + 1]);
        }
        /// flatten all left nodes and current node to a UNION DISTINCT list
        else if (union_modes[i] == SelectUnionMode::DISTINCT)
        {
            auto distinct_list = std::make_shared<ASTSelectWithUnionQuery>();
            distinct_list->list_of_selects = std::make_shared<ASTExpressionList>();
            distinct_list->children.push_back(distinct_list->list_of_selects);

            for (int j = 0; j <= i + 1; ++j)
            {
                getSelectsFromUnionListNode(select_list[j], distinct_list->list_of_selects->children);
            }

            distinct_list->union_mode = SelectUnionMode::DISTINCT;
            distinct_list->is_normalized = true;
            selects.push_back(std::move(distinct_list));
            distinct_found = true;
        }
    }

    if (const auto * union_ast = typeid_cast<const ASTSelectWithUnionQuery *>(select_list[0].get()))
    {
        const auto & current_select_modes = union_ast->set_of_modes;
        current_set_of_modes.insert(current_select_modes.begin(), current_select_modes.end());
    }

    /// No UNION DISTINCT or only one child in select_list
    if (!distinct_found)
    {
        if (auto * inner_union = select_list[0]->as<ASTSelectWithUnionQuery>();
            inner_union && inner_union->union_mode == SelectUnionMode::ALL)
        {
            /// Inner_union is an UNION ALL list, just lift it up
            for (auto child = inner_union->list_of_selects->children.rbegin(); child != inner_union->list_of_selects->children.rend();
                 ++child)
                selects.push_back(*child);
        }
        else
            selects.push_back(select_list[0]);
    }

    /// Just one union type child, lift it up
    if (selects.size() == 1 && selects[0]->as<ASTSelectWithUnionQuery>())
    {
        ast = *(selects[0]->as<ASTSelectWithUnionQuery>());
        ast.set_of_modes = std::move(current_set_of_modes);
        return;
    }

    // reverse children list
    std::reverse(selects.begin(), selects.end());

    ast.is_normalized = true;
    ast.union_mode = SelectUnionMode::ALL;
    ast.set_of_modes = std::move(current_set_of_modes);

    ast.list_of_selects->children = std::move(selects);
}
}
