#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemPrimes.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>

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

/** primes(limit)
  * primes(offset, length)
  * primes(offset, length, step)
  *
  * offset/length/step are in prime-index space:
  *   offset: how many primes to skip (0-based)
  *   length: how many primes to return
  *   step: take every step-th prime (1 means every prime)
  *
  * In text, after skipping `offset` primes, take every `step`-th prime until `limit` primes are taken.
  */
class TableFunctionPrimes : public ITableFunction
{
public:
    static constexpr auto name = "primes";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return ""; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

ColumnsDescription TableFunctionPrimes::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription{{{"prime", std::make_shared<DataTypeUInt64>()}}};
}

UInt64 TableFunctionPrimes::evaluateArgument(ContextPtr context, ASTPtr & argument) const
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

StoragePtr TableFunctionPrimes::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments ? function->arguments->children : ASTs{};

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

            auto res = std::make_shared<StorageSystemPrimes>(
                StorageID(getDatabaseName(), table_name), std::string{"prime"}, length, offset, step);

            res->startup();
            return res;
        }

        auto res = std::make_shared<StorageSystemPrimes>(StorageID(getDatabaseName(), table_name), std::string{"prime"});

        res->startup();
        return res;
    }

    throw Exception(
        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Table function '{}' requires 'limit' or 'offset, length', or 'offset, length, step'.",
        getName());
}

}

void registerTableFunctionPrimes(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPrimes>({.documentation = {
        .description = R"(Returns tables with a single UInt64 column `prime` containing prime numbers in ascending order, starting from 2.)",
        .syntax = "primes() | primes(N) | primes(N, M) | primes(N, M, S)",
        .arguments = {
            {"N", "If used as primes(N): number of primes to return. If used as primes(N, M[, S]): starting prime index (0-based).", {"UInt64"}},
            {"M", "Number of primes to return (only for primes(N, M) and primes(N, M, S)).", {"UInt64"}},
            {"S", "Step by prime index (S >= 1), only for primes(N, M, S).", {"UInt64"}},
        },
        .returned_value = {"A table with a single UInt64 column `prime`.", {"UInt64"}},
        .examples = {
            {"The first 10 primes", "SELECT * FROM primes(10);", ""},
            {"The first 10 primes using LIMIT", "SELECT prime FROM primes() LIMIT 10;", ""},
            {"Skip the first 10 primes and then return next 10", "SELECT prime FROM primes() LIMIT 10 OFFSET 10;", ""},
            {"The first prime after 1e15", "SELECT prime FROM primes() WHERE prime > toUInt64(1e15) LIMIT 1;", ""},
            {"The first 7 Mersenne primes", "SELECT prime FROM primes() WHERE bitAnd(prime, prime + 1) = 0 LIMIT 7;", ""},
        },
        .introduced_in = {26, 1},
        .category = FunctionDocumentation::Category::TableFunction,
    }, .allow_readonly = true});
}

}
