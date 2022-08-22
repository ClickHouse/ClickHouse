#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Transform>
class FunctionDateOrDateTimeToDateOrDate32 : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeToDateOrDate32>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (arguments.size() == 1)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!isString(arguments[1].type))
                throw Exception(
                    "Function " + getName() + " supports 1 or 2 arguments. The 1st argument "
                          "must be of type Date or DateTime. The 2nd argument (optional) must be "
                          "a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if ((isDate(arguments[0].type) || isDate32(arguments[0].type)))
                throw Exception(
                    "The timezone argument of function " + getName() + " is allowed only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 1 or 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// If the time zone is specified but empty, throw an exception.
        if (which.isDateTime() || which.isDateTime64())
        {
            std::string time_zone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
            /// only validate the time_zone part if the number of arguments is 2.
            if (arguments.size() == 2 && time_zone.empty())
                throw Exception(
                    "Function " + getName() + " supports a 2nd argument (optional) that must be non-empty and be a valid time zone",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (which.isDateTime64())
                return std::make_shared<DataTypeDate32>();
            else
                return std::make_shared<DataTypeDate>();
        }
        if (which.isDate32())
            return std::make_shared<DataTypeDate32>();
        else
            return std::make_shared<DataTypeDate>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
            return DateTimeTransformImpl<DataTypeDate32, DataTypeDate32, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime64())
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
        }
        else
            throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        if constexpr (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
            return { .is_monotonic = true, .is_always_monotonic = true };

        const IFunction::Monotonicity is_monotonic = { .is_monotonic = true };
        const IFunction::Monotonicity is_not_monotonic;

        const DateLUTImpl * date_lut = &DateLUT::instance();
        if (const auto * timezone = dynamic_cast<const TimezoneMixin *>(&type))
            date_lut = &timezone->getTimeZone();

        if (left.isNull() || right.isNull())
            return is_not_monotonic;

        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

        if (checkAndGetDataType<DataTypeDate>(&type))
        {
            return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), *date_lut)
                == Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), *date_lut)
                ? is_monotonic : is_not_monotonic;
        }
        else if (checkAndGetDataType<DataTypeDate32>(&type))
        {
            return Transform::FactorTransform::execute(Int32(left.get<UInt64>()), *date_lut)
                   == Transform::FactorTransform::execute(Int32(right.get<UInt64>()), *date_lut)
                   ? is_monotonic : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), *date_lut)
                == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), *date_lut)
                ? is_monotonic : is_not_monotonic;
        }
    }
};

}
