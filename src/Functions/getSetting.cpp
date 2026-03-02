#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>
#include <Core/Settings.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

enum class ErrorHandlingMode : uint8_t
{
    Exception,  /// Raise exception if setting not found (getSetting())
    Default,  /// Return default value if setting not found (getSettingOrDefault())
};

/// Get the value of a setting.
class FunctionGetSetting : public IFunction, WithContext
{
public:
    static FunctionPtr create(ContextPtr context_, ErrorHandlingMode mode_)
    {
        return std::make_shared<FunctionGetSetting>(context_, mode_);
    }

    FunctionGetSetting(ContextPtr context_, ErrorHandlingMode mode_)
        : WithContext(context_), mode(mode_)
        , function_name(mode == ErrorHandlingMode::Exception ? "getSetting" : "getSettingOrDefault")
    {}

    String getName() const override { return function_name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return (mode == ErrorHandlingMode::Default) ? 2 : 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto value = getValue(arguments);
        return applyVisitor(FieldToDataType{}, value);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto value = getValue(arguments);
        return result_type->createColumnConst(input_rows_count, convertFieldToType(value, *result_type));
    }

private:
    Field getValue(const ColumnsWithTypeAndName & arguments) const
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument of function {} should be a constant string with the name of a setting",
                            function_name);
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The argument of function {} should be a constant string with the name of a setting",
                            function_name);

        std::string_view setting_name{column->getDataAt(0)};
        Field setting_value;
        if (mode == ErrorHandlingMode::Exception)
            setting_value = getContext()->getSettingsRef().get(setting_name);
        else
        {
            const auto * default_value_column = arguments[1].column.get();
            if (!default_value_column || !(isColumnConst(*default_value_column)))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The 2nd argument of function {} should be a constant with the default value of a setting", function_name);
            }
            if (!getContext()->getSettingsRef().tryGet(setting_name, setting_value))
                setting_value = (*default_value_column)[0];
        }
        return setting_value;
    }

    ErrorHandlingMode mode;
    String function_name;
};

}

REGISTER_FUNCTION(GetSetting)
{
    FunctionDocumentation::Description description_getSetting = R"(
Returns the current value of a setting.
)";
    FunctionDocumentation::Syntax syntax_getSetting = "getSetting(setting_name)";
    FunctionDocumentation::Arguments arguments_getSetting = {
        {"setting_Name", "The setting name.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_getSetting = {"Returns the setting's current value.", {"Any"}};
    FunctionDocumentation::Examples examples_getSetting = {
    {
        "Usage example",
        R"(
SELECT getSetting('enable_analyzer');
SET enable_analyzer = false;
SELECT getSetting('enable_analyzer');
        )",
        R"(
┌─getSetting('⋯_analyzer')─┐
│ true                     │
└──────────────────────────┘
┌─getSetting('⋯_analyzer')─┐
│ false                    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_getSetting = {20, 7};
    FunctionDocumentation::Category category_getSetting = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_getSetting = {description_getSetting, syntax_getSetting, arguments_getSetting, {}, returned_value_getSetting, examples_getSetting, introduced_in_getSetting, category_getSetting};

    factory.registerFunction("getSetting", [](ContextPtr context){ return FunctionGetSetting::create(context, ErrorHandlingMode::Exception); }, documentation_getSetting);

    FunctionDocumentation::Description description_getSettingOrDefault = R"(
Returns the current value of a setting or returns the default value specified in the second argument if the setting is not set in the current profile.
)";
    FunctionDocumentation::Syntax syntax_getSettingOrDefault = "getSettingOrDefault(setting_name, default_value)";
    FunctionDocumentation::Arguments arguments_getSettingOrDefault = {
        {"setting_name", "The setting name.", {"String"}},
        {"default_value", "Value to return if custom_setting is not set. Value may be of any data type or Null."}
    };
    FunctionDocumentation::ReturnedValue returned_value_getSettingOrDefault = {"Returns the current value of the specified setting or `default_value` if the setting is not set."};
    FunctionDocumentation::Examples examples_getSettingOrDefault = {
    {
        "Usage example",
        R"(
SELECT getSettingOrDefault('custom_undef1', 'my_value');
SELECT getSettingOrDefault('custom_undef2', 100);
SELECT getSettingOrDefault('custom_undef3', NULL);
        )",
        R"(
my_value
100
NULL
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_getSettingOrDefault = {24, 10};
    FunctionDocumentation::Category category_getSettingOrDefault = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_getSettingOrDefault = {description_getSettingOrDefault, syntax_getSettingOrDefault, arguments_getSettingOrDefault, {}, returned_value_getSettingOrDefault, examples_getSettingOrDefault, introduced_in_getSettingOrDefault, category_getSettingOrDefault};

    factory.registerFunction("getSettingOrDefault", [](ContextPtr context){ return FunctionGetSetting::create(context, ErrorHandlingMode::Default); }, documentation_getSettingOrDefault);
}

}
