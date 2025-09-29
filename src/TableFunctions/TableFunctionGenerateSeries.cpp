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
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_SETTING_VALUE;
}

namespace
{

constexpr std::array<const char *, 2> names = {"generate_series", "generateSeries"};

template <size_t alias_num>
class TableFunctionGenerateSeries : public ITableFunction
{
public:
    static_assert(alias_num < names.size());
    static constexpr auto name = names[alias_num];
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

template <size_t alias_num>
ColumnsDescription TableFunctionGenerateSeries<alias_num>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"generate_series", std::make_shared<DataTypeUInt64>()}}};
}

template <size_t alias_num>
StoragePtr TableFunctionGenerateSeries<alias_num>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'length' or 'offset, length'.", getName());

        UInt64 start = evaluateArgument(context, arguments[0]);
        UInt64 stop = evaluateArgument(context, arguments[1]);
        UInt64 step = (arguments.size() == 3) ? evaluateArgument(context, arguments[2]) : UInt64{1};
        if (step == UInt64{0})
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Table function '{}' requires step to be a positive number", getName());
        auto res = (start > stop)
            ? std::make_shared<StorageSystemNumbers>(
                StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"}, 0, 0, 1)
            : std::make_shared<StorageSystemNumbers>(
                StorageID(getDatabaseName(), table_name), false, std::string{"generate_series"}, (stop - start) + 1, start, step);
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

template <size_t alias_num>
UInt64 TableFunctionGenerateSeries<alias_num>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
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

void registerTableFunctionGenerateSeries(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateSeries<0>>({.documentation = {}, .allow_readonly = true});
    factory.registerFunction<TableFunctionGenerateSeries<1>>({.documentation = {}, .allow_readonly = true});
}

}
