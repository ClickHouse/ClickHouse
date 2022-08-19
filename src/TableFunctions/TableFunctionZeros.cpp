#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionZeros.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/StorageSystemZeros.h>
#include <DataTypes/DataTypesNumber.h>
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
ColumnsDescription TableFunctionZeros<multithreaded>::getActualTableStructure(ContextPtr /*context*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"zero", std::make_shared<DataTypeUInt8>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionZeros<multithreaded>::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        UInt64 length = evaluateArgument(context, arguments[0]);

        auto res = std::make_shared<StorageSystemZeros>(StorageID(getDatabaseName(), table_name), multithreaded, length);
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
UInt64 TableFunctionZeros<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    return evaluateConstantExpressionOrIdentifierAsLiteral(argument, context)->as<ASTLiteral &>().value.safeGet<UInt64>();
}

}
