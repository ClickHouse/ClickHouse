#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionExecutable.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageExecutable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/interpretSubquery.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionExecutable::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    auto args = function->arguments->children;

    if (!(args.size() == 3 || args.size() == 4))
        throw Exception("Table function '" + getName() + "' requires exactly 3 or 4 arguments: path, format, structure, [input_query]",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i <= 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    file_path = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context)->as<ASTLiteral &>().value.safeGet<String>();
    if (args.size() == 4) {
        if (!(args[3]->as<ASTSubquery>()))
            throw Exception("Table function '" + getName() + "' 4th argument is invalid input query", ErrorCodes::LOGICAL_ERROR);
        
        input = interpretSubquery(args[3], context, {}, {})->execute().getInputStream();
    }
}

ColumnsDescription TableFunctionExecutable::getActualTableStructure(const Context & context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionExecutable::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage = StorageExecutable::create(StorageID(getDatabaseName(), table_name), file_path, format, input, getActualTableStructure(context), ConstraintsDescription{}, context);
    storage->startup();
    return storage;
}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}

}
