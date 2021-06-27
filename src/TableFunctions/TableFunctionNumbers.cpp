#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionNumbers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <bool multithreaded>
ColumnsDescription TableFunctionNumbers<multithreaded>::getActualTableStructure(ContextPtr /*context*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"number", std::make_shared<DataTypeUInt64>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
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
UInt64 TableFunctionNumbers<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The value {} is not representable as UInt64", applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

}
