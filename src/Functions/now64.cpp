#include <DataTypes/DataTypeDateTime64.h>

#include <Core/DecimalFunctions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeNullable.h>

#include <Common/assert_cast.h>

#include <time.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_CLOCK_GETTIME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

Field nowSubsecond(UInt32 scale)
{
    static constexpr Int32 fractional_scale = 9;

    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);

    DecimalUtils::DecimalComponents<DateTime64> components{spec.tv_sec, spec.tv_nsec};

    // clock_gettime produces subsecond part in nanoseconds, but decimalFromComponents fractional is scale-dependent.
    // Andjust fractional to scale, e.g. for 123456789 nanoseconds:
    //   if scale is  6 (miscoseconds) => divide by 9 - 6 = 3 to get 123456 microseconds
    //   if scale is 12 (picoseconds)  => multiply by abs(9 - 12) = 3 to get 123456789000 picoseconds
    const auto adjust_scale = fractional_scale - static_cast<Int32>(scale);
    if (adjust_scale < 0)
        components.fractional *= intExp10(std::abs(adjust_scale));
    else if (adjust_scale > 0)
        components.fractional /= intExp10(adjust_scale);

    return DecimalField(DecimalUtils::decimalFromComponents<DateTime64>(components, scale),
                        scale);
}

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class ExecutableFunctionNow64 : public IExecutableFunction
{
public:
    explicit ExecutableFunctionNow64(Field time_) : time_value(time_) {}

    String getName() const override { return "now64"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, time_value);
    }

private:
    Field time_value;
};

class FunctionBaseNow64 : public IFunctionBase
{
public:
    explicit FunctionBaseNow64(Field time_, DataTypes argument_types_, DataTypePtr return_type_)
        : time_value(time_), argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    String getName() const override { return "now64"; }

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
        return std::make_unique<ExecutableFunctionNow64>(time_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    Field time_value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class Now64OverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "now64";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<Now64OverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        UInt32 scale = DataTypeDateTime64::default_scale;
        String timezone_name;

        if (arguments.size() > 2)
        {
            throw Exception("Arguments size of function " + getName() + " should be 0, or 1, or 2", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (!arguments.empty())
        {
            const auto & argument = arguments[0];
            if (!isInteger(argument.type) || !argument.column || !isColumnConst(*argument.column))
                throw Exception("Illegal type " + argument.type->getName() +
                                " of 0" +
                                " argument of function " + getName() +
                                ". Expected const integer.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            scale = argument.column->get64(0);
        }
        if (arguments.size() == 2)
        {
            timezone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        }

        return std::make_shared<DataTypeDateTime64>(scale, timezone_name);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        UInt32 scale = DataTypeDateTime64::default_scale;
        auto res_type = removeNullable(result_type);
        if (const auto * type = typeid_cast<const DataTypeDateTime64 *>(res_type.get()))
            scale = type->getScale();

        DataTypes arg_types;
        arg_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            arg_types.push_back(arg.type);

        return std::make_unique<FunctionBaseNow64>(nowSubsecond(scale), std::move(arg_types), result_type);
    }
};

}

void registerFunctionNow64(FunctionFactory & factory)
{
    factory.registerFunction<Now64OverloadResolver>(FunctionFactory::CaseInsensitive);
}

}
