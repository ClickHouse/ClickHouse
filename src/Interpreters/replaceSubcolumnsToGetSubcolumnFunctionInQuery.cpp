#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
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

        ast = makeASTFunction("getSubcolumn", std::make_shared<ASTIdentifier>(column_name), std::make_shared<ASTLiteral>(subcolumn_name));
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

}
