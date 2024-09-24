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

enum class ErrorHandling : uint8_t
{
    Exception,
    Default,
};

/// Get the value of a setting.
class FunctionGetSetting : public IFunction, WithContext
{
public:
    explicit FunctionGetSetting(ContextPtr context_, String name_, ErrorHandling error_handling_) : WithContext(context_), name(name_), error_handling(error_handling_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return ( name == "getSettingOrDefault" ? 2 : 1 ); }
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
        if (arguments.size() == 2)
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
            if (error_handling == ErrorHandling::Exception)
                throw;
            else if (error_handling == ErrorHandling::Default)
            {
                auto default_value_column = arguments[1].column;

                setting_value = (*default_value_column)[0];
            }
        }
        return setting_value;
    }
    String name;
    ErrorHandling error_handling;
};

}

REGISTER_FUNCTION(GetSetting)
{
    factory.registerFunction("getSetting",
        [](ContextPtr context){ return std::make_shared<FunctionGetSetting>(context, "getSetting", ErrorHandling::Exception); });
}

REGISTER_FUNCTION(GetSettingOrDefault)
{
    factory.registerFunction("getSettingOrDefault",
        [](ContextPtr context){ return std::make_shared<FunctionGetSetting>(context, "getSettingOrDefault", ErrorHandling::Default); });
}

}
