#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>

#include <time.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Get the UTC time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionUTCTimestamp : public IExecutableFunction
{
public:
    explicit ExecutableFunctionUTCTimestamp(time_t time_) : time_value(time_) {}

    String getName() const override { return "UTC_timestamp"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(time_value));
    }

private:
    time_t time_value;
};

class FunctionBaseUTCTimestamp : public IFunctionBase
{
public:
    explicit FunctionBaseUTCTimestamp(time_t time_, DataTypes argument_types_, DataTypePtr return_type_)
        : time_value(time_), argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    String getName() const override { return "UTC_timestamp"; }

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
        return std::make_unique<ExecutableFunctionUTCTimestamp>(time_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    time_t time_value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class UTCTimestampOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "UTC_timestamp";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<UTCTimestampOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 0)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        return std::make_shared<DataTypeDateTime>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() > 0)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        return std::make_unique<FunctionBaseUTCTimestamp>(time(nullptr), DataTypes(), std::make_shared<DataTypeDateTime>("UTC"));
    }
};

}

void registerFunctionUTCTimestamp(FunctionFactory & factory)
{
    factory.registerFunction<UTCTimestampOverloadResolver>(FunctionFactory::CaseInsensitive);
}

}
