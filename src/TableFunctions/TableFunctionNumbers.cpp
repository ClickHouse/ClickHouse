#include <optional>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include "base/types.h"
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

namespace
{

/* numbers(limit), numbers_mt(limit)
 * - the same as SELECT number FROM system.numbers LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionNumbers : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "numbers_mt" : "numbers";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "SystemNumbers"; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

template <bool multithreaded>
ColumnsDescription TableFunctionNumbers<multithreaded>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"number", std::make_shared<DataTypeUInt64>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionNumbers<multithreaded>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() >= 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' cannot have more than three params", getName());
        if (!arguments.empty())
        {
            UInt64 offset = arguments.size() >= 2 ? evaluateArgument(context, arguments[0]) : 0;
            UInt64 length = arguments.size() >= 2 ? evaluateArgument(context, arguments[1]) : evaluateArgument(context, arguments[0]);
            UInt64 step = arguments.size() == 3 ? evaluateArgument(context, arguments[2]) : 1;

            if (!step)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function {} requires step to be a positive number", getName());

            auto res = std::make_shared<StorageSystemNumbers>(
                StorageID(getDatabaseName(), table_name), multithreaded, std::string{"number"}, length, offset, step);
            res->startup();
            return res;
        }

        auto res = std::make_shared<StorageSystemNumbers>(StorageID(getDatabaseName(), table_name), multithreaded, std::string{"number"});
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

template <bool multithreaded>
UInt64 TableFunctionNumbers<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The value {} is not representable as UInt64",
            applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

}

void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers<true>>({.documentation = {}, .allow_readonly = true});
    factory.registerFunction<TableFunctionNumbers<false>>({.documentation = {}, .allow_readonly = true});
}

}
