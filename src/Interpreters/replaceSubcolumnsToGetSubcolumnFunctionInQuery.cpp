#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (columns.contains(identifier->getColumnName()))
            return;

        auto [column_name, subcolumn_name] = Nested::splitName(identifier->getColumnName());
        auto column = columns.tryGetByName(column_name);
        if (!column || !column->type->hasSubcolumn(subcolumn_name))
            return;

        ast = makeASTFunction("getSubcolumn", make_intrusive<ASTIdentifier>(column_name), make_intrusive<ASTLiteral>(subcolumn_name));
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        if (node->arguments)
        {
            for (auto & child : node->arguments->children)
                replaceSubcolumnsToGetSubcolumnFunctionInQuery(child, columns);
        }
    }
    else
    {
        for (auto & child : ast->children)
            replaceSubcolumnsToGetSubcolumnFunctionInQuery(child, columns);
    }
}

Names getRequiredColumnsWithSubcolumnsReplaced(
    const ASTPtr & expression_ast, const NamesAndTypesList & all_columns, const ContextPtr & context)
{
    auto ast = expression_ast->clone();
    replaceSubcolumnsToGetSubcolumnFunctionInQuery(ast, all_columns);
    return TreeRewriter(context).analyze(ast, all_columns)->requiredSourceColumns();
}

}
