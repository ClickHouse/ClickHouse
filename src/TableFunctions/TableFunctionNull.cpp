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

void TableFunctionNull::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception("Table function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto & arguments = function->arguments->children;
    if (arguments.size() != 1)
        throw Exception("Table function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    structure = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context)->as<ASTLiteral>()->value.safeGet<String>();
}

ColumnsDescription TableFunctionNull::getActualTableStructure(const Context & context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = StorageNull::create(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription());
    res->startup();
    return res;
}

void registerTableFunctionNull(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNull>();
}
}
