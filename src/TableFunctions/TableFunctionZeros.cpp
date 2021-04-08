#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionZeros.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Storages/System/StorageSystemZeros.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <bool multithreaded>
StoragePtr TableFunctionZeros<multithreaded>::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        UInt64 length = evaluateArgument(context, arguments[0]);

        auto res = StorageSystemZeros::create(StorageID(getDatabaseName(), table_name), multithreaded, length);
        res->startup();
        return res;
    }
    throw Exception("Table function '" + getName() + "' requires 'limit'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void registerTableFunctionZeros(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionZeros<true>>();
    factory.registerFunction<TableFunctionZeros<false>>();
}

template <bool multithreaded>
UInt64 TableFunctionZeros<multithreaded>::evaluateArgument(const Context & context, ASTPtr & argument) const
{
    return evaluateConstantExpressionOrIdentifierAsLiteral(argument, context)->as<ASTLiteral &>().value.safeGet<UInt64>();
}

}
