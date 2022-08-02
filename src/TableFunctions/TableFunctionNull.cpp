#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
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

void TableFunctionNull::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception("Table function '" + getName() + "' requires 'structure'", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto & arguments = function->arguments->children;
    if (!arguments.empty() && arguments.size() != 1)
        throw Exception(
            "Table function '" + getName() + "' requires 'structure' argument or empty argument",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!arguments.empty())
        structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments.front(), context), "structure");
}

ColumnsDescription TableFunctionNull::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    ColumnsDescription columns;
    if (structure != "auto")
        columns = getActualTableStructure(context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    auto res = std::make_shared<StorageNull>(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription(), String{});
    res->startup();
    return res;
}

void registerTableFunctionNull(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNull>();
}
}
