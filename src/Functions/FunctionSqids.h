#pragma once

#include <sqids/blocklist.hpp>
#include <sqids/sqids.hpp>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <functional>
#include <initializer_list>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// sqids(numbers, alphabet, minLength, blocklist)
class FunctionSqids : public IFunction
{
public:
    static constexpr auto name = "sqids";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef().allow_experimental_hash_functions)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Hashing function '{}' is experimental. Set `allow_experimental_hash_functions` setting to enable it",
                name);

        return std::make_shared<FunctionSqids>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

        for (auto i : collections::range(0, arguments.size()))
        {
            if (!checkDataTypes<
                    DataTypeUInt8,
                    DataTypeUInt16,
                    DataTypeUInt32,
                    DataTypeUInt64>(arguments[i].get()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} for function {} do not support datatype {}.",
                    i,
                    getName(),
                    arguments[i]->getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_args = arguments.size();
        auto col_res = ColumnString::create();

        sqidscxx::Sqids<> sqids;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::vector<UInt64> numbers(num_args);
            for (size_t j = 0; j < num_args; ++j)
            {
                const ColumnWithTypeAndName & arg = arguments[j];
                ColumnPtr current_column = arg.column;
                numbers[j] = current_column->getUInt(i);
            }
            col_res->insert(sqids.encode(numbers));
        }
        return col_res;
    }
};
}
