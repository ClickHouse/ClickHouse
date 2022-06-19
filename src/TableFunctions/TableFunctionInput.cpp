#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionInput.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Storages/StorageInput.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

void TableFunctionInput::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    auto args = function->arguments->children;

    if (args.empty())
    {
        structure = "auto";
        return;
    }

    if (args.size() != 1)
        throw Exception("Table function '" + getName() + "' requires exactly 1 argument: structure",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context)->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionInput::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
    {
        if (structure_hint.empty())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Table function '{}' was used without structure argument but structure could not be determined automatically. Please, "
                "provide structure manually",
                getName());
        return structure_hint;
    }
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage = std::make_shared<StorageInput>(StorageID(getDatabaseName(), table_name), getActualTableStructure(context));
    storage->startup();
    return storage;
}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
