#include "config.h"

#if USE_SQIDS

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

#include <sqids/sqids.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// sqid(number1, ...)
class FunctionSqid : public IFunction
{
public:
    static constexpr auto name = "sqid";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSqid>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!checkDataTypes<
                    DataTypeUInt8,
                    DataTypeUInt16,
                    DataTypeUInt32,
                    DataTypeUInt64>(arguments[i].get()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} for function {} must have datatype UInt*, given type: {}.",
                    i, getName(), arguments[i]->getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        const size_t num_args = arguments.size();
        std::vector<UInt64> numbers(num_args);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_args; ++j)
            {
                const ColumnWithTypeAndName & arg = arguments[j];
                ColumnPtr current_column = arg.column;
                numbers[j] = current_column->getUInt(i);
            }
            auto id = sqids.encode(numbers);
            col_res->insert(id);
        }
        return col_res;
    }

private:
    sqidscxx::Sqids<> sqids;
};

REGISTER_FUNCTION(Sqid)
{
    factory.registerFunction<FunctionSqid>(FunctionDocumentation{
        .description=R"(
Transforms numbers into a [Sqid](https://sqids.org/) which is a Youtube-like ID string.)",
        .syntax="sqid(number1, ...)",
        .arguments={{"number1, ...", "Arbitrarily many UInt8, UInt16, UInt32 or UInt64 arguments"}},
        .returned_value="A hash id [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT sqid(1, 2, 3, 4, 5);",
            R"(
┌─sqid(1, 2, 3, 4, 5)─┐
│ gXHfJ1C6dN          │
└─────────────────────┘
            )"
            }}
    });
}

}

#endif
