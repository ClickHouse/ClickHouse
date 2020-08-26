#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionNumbers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/StorageTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <bool multithreaded>
void TableFunctionNumbers<multithreaded>::parseArguments(const ASTPtr & ast_function, const Context & context) const
{


    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Table function '" + getName() + "' requires 'length' or 'offset, length'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
            length = evaluateArgument(context, arguments[0]);
        else
        {
            offset = evaluateArgument(context, arguments[0]);
            length = evaluateArgument(context, arguments[1]);
        }
    }
    else
        throw Exception("Table function '" + getName() + "' requires 'limit' or 'offset, limit'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

template <bool multithreaded>
ColumnsDescription TableFunctionNumbers<multithreaded>::getActualTableStructure(const ASTPtr & /*ast_function*/, const Context & /*context*/) const
{
    return ColumnsDescription({{"number", std::make_shared<DataTypeUInt64>()}});
}

template <bool multithreaded>
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    parseArguments(ast_function, context);

    if (cached_columns.empty())
        cached_columns = getActualTableStructure(ast_function, context);

    auto get_structure = [=, tf = shared_from_this()]()
    {
        return tf->getActualTableStructure(ast_function, context);
    };

    auto res = std::make_shared<StorageTableFunction<StorageSystemNumbers>>(get_structure, StorageID(getDatabaseName(), table_name), multithreaded, length, offset, false);
    res->startup();
    return res;
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
