#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{

void ApplyWithGlobalVisitor::visit(ASTSelectQuery & select, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list)
{
    auto with = select.with();
    if (with)
    {
        std::set<String> current_names;
        for (const auto & child : with->children)
        {
            if (const auto * ast_with_alias = dynamic_cast<const ASTWithAlias *>(child.get()))
                current_names.insert(ast_with_alias->alias);
        }
        for (const auto & with_alias : exprs)
        {
            if (!current_names.contains(with_alias.first))
                with->children.push_back(with_alias.second->clone());
        }
    }
    else
        select.setExpression(ASTSelectQuery::Expression::WITH, with_expression_list->clone());
}

void ApplyWithGlobalVisitor::visit(
    ASTSelectWithUnionQuery & selects, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list)
{
    for (auto & select : selects.list_of_selects->children)
    {
        if (ASTSelectWithUnionQuery * node_union = select->as<ASTSelectWithUnionQuery>())
        {
            visit(*node_union, exprs, with_expression_list);
        }
        else if (ASTSelectQuery * node_select = select->as<ASTSelectQuery>())
        {
            visit(*node_select, exprs, with_expression_list);
        }
        else if (ASTSelectIntersectExceptQuery * node_intersect_except = select->as<ASTSelectIntersectExceptQuery>())
        {
            visit(*node_intersect_except, exprs, with_expression_list);
        }
    }
}

void ApplyWithGlobalVisitor::visit(
    ASTSelectIntersectExceptQuery & selects, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list)
{
    auto selects_list = selects.getListOfSelects();
    for (auto & select : selects_list)
    {
        if (ASTSelectWithUnionQuery * node_union = select->as<ASTSelectWithUnionQuery>())
        {
            visit(*node_union, exprs, with_expression_list);
        }
        else if (ASTSelectQuery * node_select = select->as<ASTSelectQuery>())
        {
            visit(*node_select, exprs, with_expression_list);
        }
        else if (ASTSelectIntersectExceptQuery * node_intersect_except = select->as<ASTSelectIntersectExceptQuery>())
        {
            visit(*node_intersect_except, exprs, with_expression_list);
        }
    }
}

void ApplyWithGlobalVisitor::visit(ASTPtr & ast)
{
    if (ASTSelectWithUnionQuery * node_union = ast->as<ASTSelectWithUnionQuery>())
    {
        if (auto * first_select = typeid_cast<ASTSelectQuery *>(node_union->list_of_selects->children[0].get()))
        {
            ASTPtr with_expression_list = first_select->with();
            if (with_expression_list)
            {
                std::map<String, ASTPtr> exprs;
                for (auto & child : with_expression_list->children)
                {
                    if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(child.get()))
                        exprs[ast_with_alias->alias] = child;
                }
                for (auto it = node_union->list_of_selects->children.begin() + 1; it != node_union->list_of_selects->children.end(); ++it)
                {
                    if (auto * union_child = (*it)->as<ASTSelectWithUnionQuery>())
                        visit(*union_child, exprs, with_expression_list);
                    else if (auto * select_child = (*it)->as<ASTSelectQuery>())
                        visit(*select_child, exprs, with_expression_list);
                    else if (auto * intersect_except_child = (*it)->as<ASTSelectIntersectExceptQuery>())
                        visit(*intersect_except_child, exprs, with_expression_list);
                }
            }
        }
    }
    else
    {
        // Other non-SELECT queries that contains SELECT children, such as EXPLAIN or INSERT
        for (auto & child : ast->children)
            visit(child);
    }
}

}
