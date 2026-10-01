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
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

void addUnionModesFromChild(const ASTPtr & child, SelectUnionModesSet & modes)
{
    if (const auto * union_ast = child->as<ASTSelectWithUnionQuery>())
    {
        modes.insert(union_ast->union_mode);
        modes.insert(union_ast->set_of_modes.begin(), union_ast->set_of_modes.end());
    }
}

ASTPtr makeNormalizedUnion(
    const ASTPtr & left,
    const ASTPtr & right,
    SelectUnionMode mode,
    SetOperationColumnMatchMode column_match_mode)
{
    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    result->list_of_selects = make_intrusive<ASTExpressionList>();
    result->children.push_back(result->list_of_selects);
    result->union_mode = mode;
    result->column_match_mode = column_match_mode;
    result->is_normalized = true;
    result->set_of_modes.insert(mode);

    if (const auto * left_union = left->as<ASTSelectWithUnionQuery>();
        left_union && left_union->is_normalized
        && left_union->union_mode == mode
        && left_union->column_match_mode == column_match_mode)
    {
        for (const auto & child : left_union->list_of_selects->children)
            result->list_of_selects->children.push_back(child);
        result->set_of_modes.insert(left_union->set_of_modes.begin(), left_union->set_of_modes.end());
    }
    else
    {
        result->list_of_selects->children.push_back(left);
        addUnionModesFromChild(left, result->set_of_modes);
    }

    result->list_of_selects->children.push_back(right);
    addUnionModesFromChild(right, result->set_of_modes);

    return result;
}

void normalizeUnionWithColumnMatchingByName(ASTSelectWithUnionQuery & ast, NormalizeSelectWithUnionQueryMatcher::Data & data)
{
    auto operations = ast.getSetOperations();
    const auto & select_list = ast.list_of_selects->children;

    if (operations.empty())
        return;

    if (operations.size() + 1 != select_list.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect ASTSelectWithUnionQuery (operations: {}, selects: {})",
                        operations.size(), select_list.size());

    ASTPtr current = select_list.front();
    for (size_t i = 0; i < operations.size(); ++i)
    {
        auto & operation = operations[i];

        if (operation.mode == SelectUnionMode::UNION_DEFAULT)
        {
            if (data.union_default_mode == SetOperationMode::ALL)
                operation.mode = SelectUnionMode::UNION_ALL;
            else if (data.union_default_mode == SetOperationMode::DISTINCT)
                operation.mode = SelectUnionMode::UNION_DISTINCT;
            else
                throw Exception(DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT,
                    "Expected ALL or DISTINCT after UNION. Write `UNION ALL` to keep duplicate rows or "
                    "`UNION DISTINCT` to remove them, or set `union_default_mode` to choose what a bare "
                    "UNION means");
        }

        if (operation.column_match_mode == SetOperationColumnMatchMode::Name
            && operation.mode != SelectUnionMode::UNION_ALL)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "BY NAME is supported only with UNION ALL");

        current = makeNormalizedUnion(current, select_list[i + 1], operation.mode, operation.column_match_mode);
    }

    const auto & normalized_union = current->as<ASTSelectWithUnionQuery &>();
    ast.union_mode = normalized_union.union_mode;
    ast.column_match_mode = normalized_union.column_match_mode;
    ast.is_normalized = true;
    ast.set_of_modes = normalized_union.set_of_modes;
    ast.list_of_modes.clear();
    ast.list_of_column_match_modes.clear();
    ast.list_of_selects->children = normalized_union.list_of_selects->children;
}

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
    if (auto * select_union = ast->as<ASTSelectWithUnionQuery>(); select_union && !select_union->is_normalized)
    {
        /// The rewrite of ASTSelectWithUnionQuery may strip the format info, so
        /// we need to keep and restore it.
        auto format = select_union->format_ast;
        if (select_union->hasByNameSetOperation())
            normalizeUnionWithColumnMatchingByName(*select_union, data);
        else
            visit(*select_union, data);
        select_union->reset(select_union->format_ast);
        select_union->set(select_union->format_ast, std::move(format));
    }
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

    for (Int64 i = union_modes.size() - 1; i >= 0; --i)
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
        if (union_modes[i] == SelectUnionMode::UNION_DEFAULT)
        {
            if (data.union_default_mode == SetOperationMode::ALL)
                union_modes[i] = SelectUnionMode::UNION_ALL;
            else if (data.union_default_mode == SetOperationMode::DISTINCT)
                union_modes[i] = SelectUnionMode::UNION_DISTINCT;
            else
                /// Say what to write: this is the wall that everyone coming from a database where a bare
                /// UNION means UNION DISTINCT runs into, and `SelectWithUnion` is the name of an AST class
                /// rather than of anything the query says.
                throw Exception(DB::ErrorCodes::EXPECTED_ALL_OR_DISTINCT,
                    "Expected ALL or DISTINCT after UNION. Write `UNION ALL` to keep duplicate rows or "
                    "`UNION DISTINCT` to remove them, or set `union_default_mode` to choose what a bare "
                    "UNION means");
        }

        if (union_modes[i] == SelectUnionMode::UNION_ALL)
        {
            if (auto * inner_union = select_list[i + 1]->as<ASTSelectWithUnionQuery>();
                inner_union && inner_union->union_mode == SelectUnionMode::UNION_ALL)
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
        else if (union_modes[i] == SelectUnionMode::UNION_DISTINCT)
        {
            auto distinct_list = make_intrusive<ASTSelectWithUnionQuery>();
            distinct_list->list_of_selects = make_intrusive<ASTExpressionList>();
            distinct_list->children.push_back(distinct_list->list_of_selects);

            for (int j = 0; j <= i + 1; ++j)
            {
                getSelectsFromUnionListNode(select_list[j], distinct_list->list_of_selects->children);
            }

            distinct_list->union_mode = SelectUnionMode::UNION_DISTINCT;
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
            inner_union && inner_union->union_mode == SelectUnionMode::UNION_ALL)
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
    ast.union_mode = SelectUnionMode::UNION_ALL;
    ast.set_of_modes = std::move(current_set_of_modes);

    ast.list_of_selects->children = std::move(selects);
}
}
