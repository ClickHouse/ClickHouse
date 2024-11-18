#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>


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

    String getName() const override { return "UTCTimestamp"; }

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

    String getName() const override { return "UTCTimestamp"; }

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
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    time_t time_value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class UTCTimestampOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "UTCTimestamp";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<UTCTimestampOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0", getName());
        }

        return std::make_shared<DataTypeDateTime>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (!arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0", getName());
        }

        return std::make_unique<FunctionBaseUTCTimestamp>(time(nullptr), DataTypes(), std::make_shared<DataTypeDateTime>("UTC"));
    }
};

}

/// UTC_timestamp for MySQL interface support
REGISTER_FUNCTION(UTCTimestamp)
{
    factory.registerFunction<UTCTimestampOverloadResolver>(FunctionDocumentation{
        .description=R"(
Returns the current date and time at the moment of query analysis. The function is a constant expression.
Same as `now('UTC')`. Was added only for MySQL support. `now` is preferred.

Example:
[example:typical]
)",
    .examples{
        {"typical", "SELECT UTCTimestamp();", ""}},
    .categories{"Dates and Times"}}, FunctionFactory::Case::Insensitive);
    factory.registerAlias("UTC_timestamp", UTCTimestampOverloadResolver::name, FunctionFactory::Case::Insensitive);
}

}
