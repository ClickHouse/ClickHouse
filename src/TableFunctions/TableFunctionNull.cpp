#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionNull.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * literal = arguments[0]->as<ASTLiteral>();
        if (!literal)
            throw Exception("Table function " + getName() + " requested literal argument.", ErrorCodes::LOGICAL_ERROR);
        auto structure = literal->value.safeGet<String>();
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
