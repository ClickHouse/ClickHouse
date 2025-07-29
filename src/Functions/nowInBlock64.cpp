#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/nowSubsecond.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionNowInBlock64 : public IFunction
{
public:
    static constexpr auto name = "nowInBlock64";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNowInBlock64>(context); }
    explicit FunctionNowInBlock64(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Optional timezone argument.
    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty()) 
            return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

        if (arguments.size() > 2) 
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 1 or 2", getName());
        

        if (!isInteger(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be Integer", getName());

        const auto scale = static_cast<UInt32>(arguments[0].column->get64(0));

        if (arguments.size() == 1)
            return std::make_shared<DataTypeDateTime64>(scale);

        if (!isStringOrFixedString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be String or FixedString", getName());

        return std::make_shared<DataTypeDateTime64>(
            scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 1, allow_nonconst_timezone_arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & type, size_t input_rows_count) const override
    {
        const auto * dataType64 = checkAndGetDataType<DataTypeDateTime64>(type.get());

        if (!dataType64) 
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected DataTypeDateTime64, got {}", type->getName());

        auto column_pointer = ColumnDateTime64::create(input_rows_count, dataType64->getScale());
        auto & vec_res = column_pointer->getData();
        const auto now_decimal = nowSubsecond(dataType64->getScale()).safeGet<Decimal64>();

        for (size_t i = 0; i < input_rows_count; ++i)
            vec_res[i] = now_decimal.getValue();

        return column_pointer;
    }

private:
    const bool allow_nonconst_timezone_arguments;
};

}

REGISTER_FUNCTION(NowInBlock64)
{
    FunctionDocumentation::Description description = R"(
Returns the current date and time at the moment of processing of each block of data in miliseconds. In contrast to the function [now64](#now64), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running INSERT SELECT queries.
    )";
    FunctionDocumentation::Syntax syntax = R"(
nowInBlock([scale[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"scale", "Optional. Tick size (precision): 10^-precision seconds. Valid range: [0 : 9]. Typically, are used - 3 (default) (milliseconds), 6 (microseconds), 9 (nanoseconds).", {"UInt8"}},
        {"timezone", "Optional. Timezone name for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current date and time at the moment of processing of each block of data with sub-second precision.", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Difference with the now64() function", R"(
SELECT
    now64(),
    nowInBlock64(),
    sleep(1)
FROM numbers(3)
SETTINGS max_block_size = 1
FORMAT PrettyCompactMonoBlock
        )",
        R"(
┌─────────────────now64()─┬──────────nowInBlock64()─┬─sleep(1)─┐
│ 2025-07-29 17:07:29.526 │ 2025-07-29 17:07:29.534 │        0 │
│ 2025-07-29 17:07:29.526 │ 2025-07-29 17:07:30.535 │        0 │
│ 2025-07-29 17:07:29.526 │ 2025-07-29 17:07:31.535 │        0 │
└─────────────────────────┴─────────────────────────┴──────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNowInBlock64>(documentation);
}

}
