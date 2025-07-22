#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/nowSubsecond.h>
#include <Interpreters/Context.h>

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

class FunctionNow64InBlock : public IFunction
{
public:
    static constexpr auto name = "now64InBlock";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNow64InBlock>(context); }
    explicit FunctionNow64InBlock(ContextPtr context)
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
        if (arguments.empty() || arguments.size() > 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 1 or 2", getName());
        }

        if (!isInteger(arguments[0].type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be Integer", getName());
        }

        const auto scale = static_cast<UInt32>(arguments[0].column->get64(0));

        if (arguments.size() == 1)
        {
            return std::make_shared<DataTypeDateTime64>(scale);
        }

        if (!isStringOrFixedString(arguments[1].type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be String or FixedString", getName());
        }
        return std::make_shared<DataTypeDateTime64>(
            scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 1, allow_nonconst_timezone_arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto scale = static_cast<UInt32>(arguments[0].column->get64(0));

        auto column_pointer = ColumnDateTime64::create(input_rows_count, scale);
        auto & vec_res = column_pointer->getData();
        const auto now_decimal = nowSubsecond(scale).safeGet<Decimal64>();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            vec_res[i] = now_decimal.getValue();
        }

        return column_pointer;
    }

private:
    const bool allow_nonconst_timezone_arguments;
};

}

REGISTER_FUNCTION(Now64InBlock)
{
    factory.registerFunction<FunctionNow64InBlock>();
}

}
