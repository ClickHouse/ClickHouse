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

        /// A column name can itself contain dots (e.g. a Nested column `a.b`), so try every split
        /// point of the identifier and take the first whose left part is a stored column that has
        /// the remainder as a subcolumn, like Block::findSubcolumnByName.
        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(identifier->getColumnName()))
        {
            auto column = columns.tryGetByName(String(column_name));
            if (!column || !column->type->hasSubcolumn(subcolumn_name))
                continue;

            ast = makeASTFunction(
                "getSubcolumn",
                make_intrusive<ASTIdentifier>(String(column_name)),
                make_intrusive<ASTLiteral>(String(subcolumn_name)));
            return;
        }
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
