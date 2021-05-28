#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionInput.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageInput.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    auto args = function->arguments->children;

    if (args.size() != 1)
        throw Exception("Table function '" + getName() + "' requires exactly 1 argument: structure",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context)->as<ASTLiteral &>().value.safeGet<String>();
    auto columns = parseColumnsListFromString(structure, context);
    StoragePtr storage = StorageInput::create(StorageID(getDatabaseName(), table_name), columns);

    storage->startup();

    return storage;
}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
