#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionNull.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto structure = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context)->as<ASTLiteral>()->value.safeGet<String>();
        ColumnsDescription columns = parseColumnsListFromString(structure, context);

        auto res = StorageNull::create(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription());
        res->startup();
        return res;
    }
    throw Exception("Table function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void registerTableFunctionNull(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNull>();
}
}
