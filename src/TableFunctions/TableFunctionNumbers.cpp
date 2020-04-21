#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionNumbers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Storages/System/StorageSystemNumbers.h>
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
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Table function '" + getName() + "' requires 'length' or 'offset, length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        UInt64 offset = arguments.size() == 2 ? evaluateArgument(context, arguments[0]) : 0;
        UInt64 length = arguments.size() == 2 ? evaluateArgument(context, arguments[1]) : evaluateArgument(context, arguments[0]);

        auto res = StorageSystemNumbers::create(StorageID(getDatabaseName(), table_name), multithreaded, length, offset, false);
        res->startup();
        return res;
    }
    throw Exception("Table function '" + getName() + "' requires 'limit' or 'offset, limit'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers<true>>();
    factory.registerFunction<TableFunctionNumbers<false>>();
}

template <bool multithreaded>
UInt64 TableFunctionNumbers<multithreaded>::evaluateArgument(const Context & context, ASTPtr & argument) const
{
    return evaluateConstantExpressionOrIdentifierAsLiteral(argument, context)->as<ASTLiteral &>().value.safeGet<UInt64>();
}

}
