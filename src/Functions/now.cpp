#include <ctime>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nonconst_timezone_arguments;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionNow : public IExecutableFunction
{
public:
    explicit ExecutableFunctionNow(time_t time_) : time_value(time_) {}

    String getName() const override { return "now"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(time_value));
    }

private:
    time_t time_value;
};

class FunctionBaseNow : public IFunctionBase
{
public:
    explicit FunctionBaseNow(time_t time_, DataTypes argument_types_, DataTypePtr return_type_)
        : time_value(time_), argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    String getName() const override { return "now"; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionNow>(time_value);
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

private:
    time_t time_value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class NowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "now";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<NowOverloadResolver>(context); }
    explicit NowOverloadResolver(ContextPtr context)
        : allow_nonconst_timezone_arguments(context->getSettingsRef()[Setting::allow_nonconst_timezone_arguments])
    {}

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0 or 1", getName());
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

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0 or 1", getName());
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be String or FixedString",
                getName());
        }
        if (arguments.size() == 1)
            return std::make_unique<FunctionBaseNow>(
                time(nullptr), DataTypes{arguments.front().type},
                std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0, allow_nonconst_timezone_arguments)));

        return std::make_unique<FunctionBaseNow>(time(nullptr), DataTypes(), std::make_shared<DataTypeDateTime>());
    }
private:
    const bool allow_nonconst_timezone_arguments;
};

}

REGISTER_FUNCTION(Now)
{
    factory.registerFunction<NowOverloadResolver>({}, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_timestamp", NowOverloadResolver::name, FunctionFactory::Case::Insensitive);
}

}
