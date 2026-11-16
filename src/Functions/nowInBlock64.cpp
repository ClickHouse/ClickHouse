#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/nowSubsecond.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Returns current time at calculation of every block.
* In contrast to 'now64' function, it's not a constant expression and is not a subject of constant folding.
*/
class FunctionNowInBlock64 : public IFunction
{
public:
    static constexpr auto name = "nowInBlock64";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionNowInBlock64>(); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; } /// Optional timezone argument.
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
        };
        FunctionArgumentDescriptors optional_args{
            {"scale", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), isColumnConst, "const (U)Int*"},
            {"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        if (arguments.empty())
            return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

        auto scale = static_cast<UInt32>(arguments[0].column->get64(0));

        if (arguments.size() == 1)
            return std::make_shared<DataTypeDateTime64>(scale);

        return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 1, false));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & type, size_t input_rows_count) const override
    {
        const auto * data_type_datetime64 = checkAndGetDataType<DataTypeDateTime64>(type.get());

        if (!data_type_datetime64)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected a DateTime64, got {}", type->getName());

        auto col_res = ColumnDateTime64::create(input_rows_count, data_type_datetime64->getScale());
        auto & vec_res = col_res->getData();

        auto now = nowSubsecond(data_type_datetime64->getScale()).safeGet<Decimal64>();
        for (size_t i = 0; i < input_rows_count; ++i)
            vec_res[i] = now.getValue();

        return col_res;
    }
};

}

REGISTER_FUNCTION(NowInBlock64)
{
    FunctionDocumentation::Description description = R"(
Returns the current date and time at the moment of processing of each block of data in milliseconds. In contrast to the function [now64](#now64), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running INSERT SELECT queries.
    )";
    FunctionDocumentation::Syntax syntax = R"(
nowInBlock64([scale[, timezone]])
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
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNowInBlock64>(documentation);
}

}
