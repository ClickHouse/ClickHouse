#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionZeros.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/StorageSystemZeros.h>
#include <Storages/StorageTableFunction.h>
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
void TableFunctionZeros<multithreaded>::parseArguments(const ASTPtr & ast_function, const Context & context) const
{


    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1)
            throw Exception("Table function '" + getName() + "' requires 'length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        length = evaluateArgument(context, arguments[0]);
    }
    else
        throw Exception("Table function '" + getName() + "' requires 'limit'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

template <bool multithreaded>
ColumnsDescription TableFunctionZeros<multithreaded>::getActualTableStructure(const ASTPtr & /*ast_function*/, const Context & /*context*/) const
{
    return ColumnsDescription({{"zero", std::make_shared<DataTypeUInt8>()}});
}

template <bool multithreaded>
StoragePtr TableFunctionZeros<multithreaded>::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    parseArguments(ast_function, context);

    if (cached_columns.empty())
        cached_columns = getActualTableStructure(ast_function, context);

    auto get_structure = [=, tf = shared_from_this()]()
    {
        return tf->getActualTableStructure(ast_function, context);
    };

    auto res = std::make_shared<StorageTableFunction<StorageSystemZeros>>(std::move(get_structure), StorageID(getDatabaseName(), table_name), multithreaded, length);
    res->startup();
    return res;
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
