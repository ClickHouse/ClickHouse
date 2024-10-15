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
template <ErrorHandlingMode mode>
class FunctionGetSetting : public IFunction, WithContext
{
public:
    static constexpr auto name = (mode == ErrorHandlingMode::Exception) ? "getSetting" : "getSettingOrDefault";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetSetting>(context_); }
    explicit FunctionGetSetting(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return (mode == ErrorHandlingMode::Default) ? 2 : 1 ; }
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
                            String{name});
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The argument of function {} should be a constant string with the name of a setting",
                            String{name});

        std::string_view setting_name{column->getDataAt(0).toView()};
        Field setting_value;
        if constexpr (mode == ErrorHandlingMode::Exception)
            setting_value = getContext()->getSettingsRef().get(setting_name);
        else
        {
            const auto * default_value_column = arguments[1].column.get();
            if (!default_value_column || !(isColumnConst(*default_value_column)))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The 2nd argument of function {} should be a constant with the default value of a setting", String{name});
            }
            if (!getContext()->getSettingsRef().tryGet(setting_name, setting_value))
                setting_value = (*default_value_column)[0];
        }
        return setting_value;
    }
};

}

REGISTER_FUNCTION(GetSetting)
{
    factory.registerFunction<FunctionGetSetting<ErrorHandlingMode::Exception>>(FunctionDocumentation{
        .description = R"(
Returns the current value of a custom setting.
)",
        .syntax = "getSetting('custom_setting')",
        .arguments = {
            {"custom_setting", "The setting name. Type: String."}
        },
        .returned_value = "The setting's current value.",
        .examples = {
            {"getSetting", "SET custom_a = 123; SELECT getSetting('custom_a');", "123"},
        },
        .categories{"Other"}}, FunctionFactory::Case::Sensitive);
    factory.registerFunction<FunctionGetSetting<ErrorHandlingMode::Default>>(FunctionDocumentation{
        .description = R"(
Returns the current value of a custom setting or returns the default value specified in the 2nd argument if the custom setting is not set in the current profile.
)",
        .syntax = "getSettingOrDefault('custom_setting', default_value)",
        .arguments = {
            {"custom_setting", "The setting name. Type: String."},
            {"default_value", "Value to return if custom_setting is not set. Value may be of any data type or Null."},
        },
        .returned_value = "The setting's current value or the default_value if setting is not set.",
        .examples = {
            {"getSettingOrDefault", "SELECT getSettingOrDefault('custom_undef1', 'my_value');", "my_value"},
            {"getSettingOrDefault", "SELECT getSettingOrDefault('custom_undef1', 100);", "100"},
            {"getSettingOrDefault", "SELECT getSettingOrDefault('custom_undef1', NULL);", "NULL"},
        },
        .categories{"Other"}}, FunctionFactory::Case::Sensitive);
}

}
