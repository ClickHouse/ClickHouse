#include <DataTypes/DataTypeDateTime64.h>

#include <Core/DecimalFunctions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <Common/assert_cast.h>

#include <time.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_CLOCK_GETTIME;
}

namespace
{

Field nowSubsecond(UInt32 scale)
{
    static constexpr Int32 fractional_scale = 9;

    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);

    DecimalUtils::DecimalComponents<DateTime64::NativeType> components{spec.tv_sec, spec.tv_nsec};

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

class FunctionNow64 : public IFunction
{
public:
    static constexpr auto name = "now64";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow64>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return ColumnNumbers{0}; }
    bool isDeterministic() const override { return false; }

    // Return type depends on argument value.
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        UInt32 scale = DataTypeDateTime64::default_scale;

        // Type check is similar to the validateArgumentType, trying to keep error codes and messages as close to the said function as possible.
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

        return std::make_shared<DataTypeDateTime64>(scale);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const UInt32 scale = assert_cast<const DataTypeDateTime64 *>(result_type.get())->getScale();
        return result_type->createColumnConst(input_rows_count, nowSubsecond(scale));
    }
};

}

void registerFunctionNow64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNow64>(FunctionFactory::CaseInsensitive);
}

}
