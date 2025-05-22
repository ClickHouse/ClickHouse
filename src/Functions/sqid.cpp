#include "config.h"

#if USE_SQIDS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <sqids/sqids.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// sqidEncode(number1, ...)
class FunctionSqidEncode : public IFunction
{
public:
    static constexpr auto name = "sqidEncode";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSqidEncode>(); }

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

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
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

/// sqidDecode(number1, ...)
class FunctionSqidDecode : public IFunction
{
public:
    static constexpr auto name = "sqidDecode";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSqidDecode>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"sqid", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res_nested = ColumnUInt64::create();
        auto & res_nested_data = col_res_nested->getData();

        auto col_res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = col_res_offsets->getData();
        res_offsets_data.reserve(input_rows_count);

        const auto & src = arguments[0];
        const auto & src_column = *src.column;

        if (const auto * col_non_const = typeid_cast<const ColumnString *>(&src_column))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view sqid = col_non_const->getDataAt(i).toView();
                std::vector<UInt64> integers = sqids.decode(String(sqid));
                if (!integers.empty())
                    res_nested_data.insert(integers.begin(), integers.end());
                res_offsets_data.push_back(res_offsets_data.back() + integers.size());
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        return ColumnArray::create(std::move(col_res_nested), std::move(col_res_offsets));
    }

private:
    sqidscxx::Sqids<> sqids;
};

REGISTER_FUNCTION(Sqid)
{
    factory.registerFunction<FunctionSqidEncode>(FunctionDocumentation{
        .description=R"(
Transforms numbers into a [Sqid](https://sqids.org/) which is a Youtube-like ID string.)",
        .syntax="sqidEncode(number1, ...)",
        .arguments={{"number1, ...", "Arbitrarily many UInt8, UInt16, UInt32 or UInt64 arguments"}},
        .returned_value="A hash id [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT sqidEncode(1, 2, 3, 4, 5);",
            R"(
┌─sqidEncode(1, 2, 3, 4, 5)─┐
│ gXHfJ1C6dN                │
└───────────────────────────┘
            )"
            }}
    });
    factory.registerAlias("sqid", FunctionSqidEncode::name);

    factory.registerFunction<FunctionSqidDecode>(FunctionDocumentation{
        .description=R"(
Transforms a [Sqid](https://sqids.org/) back into an array of numbers.)",
        .syntax="sqidDecode(number1, ...)",
        .arguments={{"sqid", "A sqid"}},
        .returned_value="An array of [UInt64](/docs/en/sql-reference/data-types/int-uint.md).",
        .examples={
            {"simple",
            "SELECT sqidDecode('gXHfJ1C6dN');",
            R"(
┌─sqidDecode('gXHfJ1C6dN')─┐
│ [1,2,3,4,5]              │
└──────────────────────────┘
            )"
            }}
    });
}

}

#endif
