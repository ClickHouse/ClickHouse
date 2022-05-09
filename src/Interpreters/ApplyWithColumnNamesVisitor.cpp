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
                if (auto * column_names = with_element->column_names->as<ASTExpressionList>())
                {
                    if (auto * subquery = with_element->subquery->as<ASTSubquery>())
                    {
                        if (auto * select_union = subquery->children.at(0)->as<ASTSelectWithUnionQuery>())
                        {
                            for (auto & child_select : select_union->list_of_selects->children)
                            {
                                if (auto * select = child_select->as<ASTSelectQuery>())
                                {
                                    if (auto columns = select->select())
                                    {
                                        if (column_names->children.size() != columns->children.size())
                                            throw Exception("The number of identifiers is not equal to the number of expressions", ErrorCodes::LOGICAL_ERROR);

                                        for (size_t i = 0; i != columns->children.size(); ++i)
                                            if (auto * column_name = column_names->children[i]->as<ASTIdentifier>())
                                                columns->children[i]->setAlias(column_name->name());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

}
