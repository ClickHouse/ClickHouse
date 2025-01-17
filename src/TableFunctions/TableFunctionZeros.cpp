#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
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

namespace
{

/* zeros(limit), zeros_mt(limit)
 * - the same as SELECT zero FROM system.zeros LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionZeros : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "zeros_mt" : "zeros";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "SystemZeros"; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};


template <bool multithreaded>
ColumnsDescription TableFunctionZeros<multithreaded>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"zero", std::make_shared<DataTypeUInt8>()}}};
}

template <bool multithreaded>
StoragePtr TableFunctionZeros<multithreaded>::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' cannot have more than one params", getName());

        if (!arguments.empty())
        {
            UInt64 length = evaluateArgument(context, arguments[0]);

            auto res = std::make_shared<StorageSystemZeros>(StorageID(getDatabaseName(), table_name), multithreaded, length);
            res->startup();
            return res;
        }

        /// zero-argument, the same as system.zeros
        auto res = std::make_shared<StorageSystemZeros>(StorageID(getDatabaseName(), table_name), multithreaded);
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit'.", getName());
}

template <bool multithreaded>
UInt64 TableFunctionZeros<multithreaded>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    return checkAndGetLiteralArgument<UInt64>(evaluateConstantExpressionOrIdentifierAsLiteral(argument, context), "length");
}

}

void registerTableFunctionZeros(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionZeros<true>>({.documentation = {
            .description=R"(
                Generates a stream of zeros (a table with one column 'zero' of type 'UInt8') of specified size.
                This table function is used in performance tests, where you want to spend as little time as possible to data generation while testing some other parts of queries.
                In contrast to the `zeros_mt`, this table function is using single thread for data generation.
                Example:
                [example:1]
                This query will test the speed of `randomPrintableASCII` function using single thread.
                See also the `system.zeros` table.)",
            .examples={{"1", "SELECT count() FROM zeros(100000000) WHERE NOT ignore(randomPrintableASCII(10))", ""}}
    }});

    factory.registerFunction<TableFunctionZeros<false>>({.documentation = {
            .description=R"(
                Generates a stream of zeros (a table with one column 'zero' of type 'UInt8') of specified size.
                This table function is used in performance tests, where you want to spend as little time as possible to data generation while testing some other parts of queries.
                In contrast to the `zeros`, this table function is using multiple threads for data generation, according to the `max_threads` setting.
                Example:
                [example:1]
                This query will test the speed of `randomPrintableASCII` function using multiple threads.
                See also the `system.zeros` table.
                )",
            .examples={{"1", "SELECT count() FROM zeros_mt(1000000000) WHERE NOT ignore(randomPrintableASCII(10))", ""}}
    }});
}

}
