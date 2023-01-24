#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class ExecutableFunctionToTimeZone : public IExecutableFunction
{
public:
    explicit ExecutableFunctionToTimeZone() = default;

    String getName() const override { return "toTimezone"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        return arguments[0].column;
    }
};

class FunctionBaseToTimeZone : public IFunctionBase
{
public:
    FunctionBaseToTimeZone(
        bool is_constant_timezone_,
        DataTypes argument_types_,
        DataTypePtr return_type_)
        : is_constant_timezone(is_constant_timezone_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_)) {}

    String getName() const override { return "toTimezone"; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<ExecutableFunctionToTimeZone>();
    }

    bool hasInformationAboutMonotonicity() const override { return is_constant_timezone; }

    Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const override
    {
        const bool b = is_constant_timezone;
        return { .is_monotonic = b, .is_positive = b, .is_always_monotonic = b };
    }

private:
    bool is_constant_timezone;
    DataTypes argument_types;
    DataTypePtr return_type;
};

/// Just changes time zone information for data type. The calculation is free.
class ToTimeZoneOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "toTimezone";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<ToTimeZoneOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto which_type = WhichDataType(arguments[0].type);
        if (!which_type.isDateTime() && !which_type.isDateTime64())
            throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be DateTime or DateTime64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        if (which_type.isDateTime())
            return std::make_shared<DataTypeDateTime>(time_zone_name);

        const auto * date_time64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
        return std::make_shared<DataTypeDateTime64>(date_time64->getScale(), time_zone_name);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        bool is_constant_timezone = false;
        if (arguments[1].column)
            is_constant_timezone = isColumnConst(*arguments[1].column);

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<FunctionBaseToTimeZone>(is_constant_timezone, data_types, result_type);
    }
};

}

void registerFunctionToTimeZone(FunctionFactory & factory)
{
    factory.registerFunction<ToTimeZoneOverloadResolver>();
    factory.registerAlias("toTimeZone", "toTimezone");
}

}
