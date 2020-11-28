#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunctionImpl.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>

#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <time.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionNow : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionNow(time_t time_) : time_value(time_) {}

    String getName() const override { return "now"; }

    ColumnPtr execute(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(time_value));
    }

private:
    time_t time_value;
};

class FunctionBaseNow : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseNow(time_t time_, DataTypePtr return_type_) : time_value(time_), return_type(return_type_) {}

    String getName() const override { return "now"; }

    const DataTypes & getArgumentTypes() const override
    {
        static const DataTypes argument_types;
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionNow>(time_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    time_t time_value;
    DataTypePtr return_type;
};

class NowOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "now";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverImplPtr create(const Context &) { return std::make_unique<NowOverloadResolver>(); }

    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0 or 1", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(
                "Arguments of function " + getName() + " should be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (arguments.size() == 1)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0));
        }
        return std::make_shared<DataTypeDateTime>();
    }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0 or 1", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
        {
            throw Exception(
                "Arguments of function " + getName() + " should be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (arguments.size() == 1)
            return std::make_unique<FunctionBaseNow>(
                time(nullptr), std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0)));
        return std::make_unique<FunctionBaseNow>(time(nullptr), std::make_shared<DataTypeDateTime>());
    }
};

}

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<NowOverloadResolver>(FunctionFactory::CaseInsensitive);
}

}
