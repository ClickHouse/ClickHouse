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

struct NameGetSetting{ static constexpr auto name = "getSetting"; };

struct NameGetSettingOrDefault{ static constexpr auto name = "getSettingOrDefault"; };

/// Get the value of a setting.
template <typename Name, ErrorHandlingMode mode>
class FunctionGetSetting : public IFunction, WithContext
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetSetting>(context_); }
    explicit FunctionGetSetting(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return ( mode == ErrorHandlingMode::Default ? 2 : 1 ); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0,1}; }

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
        if (mode == ErrorHandlingMode::Default)
        {
            const auto * default_value_column = arguments[1].column.get();
            if (!default_value_column || default_value_column->size() != 1)
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The 2nd argument of function {} should be a constant", String{name});
            }
        }

        std::string_view setting_name{column->getDataAt(0).toView()};
        Field setting_value;
        try
        {
            setting_value = getContext()->getSettingsRef().get(setting_name);
        }
        catch(...)
        {
            if (mode == ErrorHandlingMode::Exception)
                throw;
            else if (mode == ErrorHandlingMode::Default)
            {
                auto default_value_column = arguments[1].column;

                setting_value = (*default_value_column)[0];
            }
        }
        return setting_value;
    }
};

}

using FunctionGetSettingWithException = FunctionGetSetting<NameGetSetting, ErrorHandlingMode::Exception>;
using FunctionGetSettingWithDefault = FunctionGetSetting<NameGetSettingOrDefault, ErrorHandlingMode::Default>;

REGISTER_FUNCTION(GetSetting)
{
    factory.registerFunction<FunctionGetSettingWithException>();
}


REGISTER_FUNCTION(GetSettingOrDefault)
{
    factory.registerFunction<FunctionGetSettingWithDefault>();
}
}
