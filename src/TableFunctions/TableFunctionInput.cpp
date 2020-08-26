#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionInput.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageInput.h>
#include <Storages/StorageTableFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionInput::parseArguments(const ASTPtr & ast_function, const Context & context) const
{
    if (!structure.empty())
        return;

    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    auto args = function->arguments->children;

    if (args.size() != 1)
        throw Exception("Table function '" + getName() + "' requires exactly 1 argument: structure",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context)->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionInput::getActualTableStructure(const ASTPtr & ast_function, const Context & context) const
{
    parseArguments(ast_function, context);
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    parseArguments(ast_function, context);
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(ast_function, context);

    auto get_structure = [=, tf = shared_from_this()]()
    {
        return tf->getActualTableStructure(ast_function, context);
    };

    StoragePtr storage = std::make_shared<StorageTableFunction<StorageInput>>(get_structure, StorageID(getDatabaseName(), table_name), cached_columns);

    storage->startup();

    return storage;
}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
