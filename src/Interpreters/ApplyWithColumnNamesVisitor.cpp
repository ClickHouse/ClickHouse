#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/ApplyWithColumnNamesVisitor.h>

namespace DB
{
void ApplyWithColumnNamesVisitor::visit(ASTPtr & ast)
{
    if (auto * node_select = ast->as<ASTSelectQuery>())
        visit(*node_select);
    else
        for (auto & child : ast->children)
            visit(child);
}

void ApplyWithColumnNamesVisitor::visit(ASTSelectQuery & ast)
{
    if (auto with = ast.with())
    {
        for (auto & child : with->children)
        {
            if (auto * with_element = child->as<ASTWithElement>())
            {
                auto * subquery = with_element->subquery->as<ASTSubquery>();
                auto column_names = with_element->column_names;
                auto ast_query = subquery->children.at(0);

                if (auto * select = ast_query->as<ASTSelectQuery>())
                    select->setExpression(ASTSelectQuery::Expression::SELECT_ALIASES, column_names->clone());
                else if (auto * select_union = ast_query->as<ASTSelectWithUnionQuery>())
                    select_union->list_of_selects->children.at(0)->as<ASTSelectQuery>()->setExpression(
                        ASTSelectQuery::Expression::SELECT_ALIASES, column_names->clone());
            }
        }
    }
}

}
