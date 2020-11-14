#include <Interpreters/ColumnAliasesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

bool ColumnAliasesMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !(node->as<ASTTableExpression>()
            || node->as<ASTSubquery>()
            || node->as<ASTArrayJoin>()
            || node->as<ASTSelectWithUnionQuery>());
}

void ColumnAliasesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * node = ast->as<ASTIdentifier>())
    {
        if (auto column_name = IdentifierSemantic::getColumnName(*node))
        {
            if (const auto column_default = data.columns.getDefault(column_name.value()))
            {
                if (column_default.value().kind == ColumnDefaultKind::Alias)
                {
                    const auto alias_columns = data.columns.getAliases();
                    for (const auto & alias_column : alias_columns)
                    {
                        if (alias_column.name == column_name.value())
                        {
                            ast = addTypeConversionToAST(column_default.value().expression->clone(), alias_column.type->getName());
                            //revisit ast to track recursive alias columns
                            Visitor(data).visit(ast);
                            break;
                        }
                    }
                }
            }
        }
    }
}

}
