#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace
{

/** Returns current time at calculation of every block.
  * In contrast to 'now' function, it's not a constant expression and is not a subject of constant folding.
  */
class FunctionNowInBlock final : public IFunction
{
public:
    static constexpr auto name = "nowInBlock";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionNowInBlock>(context);
    }
    explicit FunctionNowInBlock(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {}

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; } /// Optional timezone argument.
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    String getSignatureString() const override
    {
        if (allow_nonconst_timezone_arguments)
            return
                "(const tz String) -> DateTime(tz)"
                " OR ([StringOrFixedString]) -> DateTime";
        return
            "(const tz String) -> DateTime(tz)"
            " OR () -> DateTime";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnDateTime::create(input_rows_count, static_cast<UInt32>(time(nullptr)));
    }

private:
    const bool allow_nonconst_timezone_arguments;
};

}

REGISTER_FUNCTION(NowInBlock)
{
    FunctionDocumentation::Description description = R"(
Returns the current date and time at the moment of processing of each block of data. In contrast to the function [`now`](#now), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running `INSERT SELECT` queries.
    )";
    FunctionDocumentation::Syntax syntax = R"(
nowInBlock([timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timezone", "Optional. Timezone name for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current date and time at the moment of processing of each block of data.", {"DateTime"}};
    FunctionDocumentation::Examples examples = {
        {"Difference with the now() function", R"(
SELECT
    now(),
    nowInBlock(),
    sleep(1)
FROM numbers(3)
SETTINGS max_block_size = 1
FORMAT PrettyCompactMonoBlock
        )",
        R"(
┌───────────────now()─┬────────nowInBlock()─┬─sleep(1)─┐
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:19 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:20 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:21 │        0 │
└─────────────────────┴─────────────────────┴──────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNowInBlock>(documentation);
}

}
