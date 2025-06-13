#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Returns current time at calculation of every block.
  * In contrast to 'now' function, it's not a constant expression and is not a subject of constant folding.
  */
class FunctionNowInBlock : public IFunction
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

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    /// Optional timezone argument.
    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Arguments size of function {} should be 0 or 1", getName());
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be String or FixedString",
                getName());
        }
        if (arguments.size() == 1)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0, allow_nonconst_timezone_arguments));
        }
        return std::make_shared<DataTypeDateTime>();
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
    FunctionDocumentation::Description description_nowInBlock = R"(
Returns the current date and time at the moment of processing of each block of data. In contrast to the function [`now`](#now), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running `INSERT SELECT` queries.
    )";
    FunctionDocumentation::Syntax syntax_nowInBlock = R"(
nowInBlock([timezone])
    )";
    FunctionDocumentation::Arguments arguments_nowInBlock = {
        {"timezone", "Optional. Timezone name for the returned value. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_nowInBlock = "Returns the current date and time at the moment of processing of each block of data. [`DateTime`](../data-types/datetime.md).";
    FunctionDocumentation::Examples examples_nowInBlock = {
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
    FunctionDocumentation::IntroducedIn introduced_in_nowInBlock = {22, 8};
    FunctionDocumentation::Category category_nowInBlock = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_nowInBlock = {
        description_nowInBlock,
        syntax_nowInBlock,
        arguments_nowInBlock,
        returned_value_nowInBlock,
        examples_nowInBlock,
        introduced_in_nowInBlock,
        category_nowInBlock
    };

    factory.registerFunction<FunctionNowInBlock>(documentation_nowInBlock);
}

}
