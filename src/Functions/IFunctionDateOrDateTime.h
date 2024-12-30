#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionDateOrDateTimeBase : public IFunction
{
    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

protected:
    void checkArguments(const ColumnsWithTypeAndName & arguments, bool is_result_type_date_or_date32) const
    {
        if (arguments.size() == 1)
        {
            if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
                    arguments[0].type->getName(), getName());
        }
        else if (arguments.size() == 2)
        {
            if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
                    arguments[0].type->getName(), getName());
            if (!isString(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports 1 or 2 arguments. The optional 2nd argument must be "
                    "a constant string with a timezone name",
                    getName());
            if (isDateOrDate32(arguments[0].type) && is_result_type_date_or_date32)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} is allowed only when the 1st argument has the type DateTime or DateTime64",
                    getName());
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
                getName(), arguments.size());
    }
};

template <typename Transform>
class IFunctionDateOrDateTime : public FunctionDateOrDateTimeBase
{
public:
    static constexpr auto name = Transform::name;
    String getName() const override { return name; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        if constexpr (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
            return { .is_monotonic = true, .is_always_monotonic = true };
        else
        {
            const IFunction::Monotonicity is_monotonic = { .is_monotonic = true };
            const IFunction::Monotonicity is_not_monotonic;

            const DateLUTImpl * date_lut = &DateLUT::instance();
            if (const auto * timezone = dynamic_cast<const TimezoneMixin *>(&type))
                date_lut = &timezone->getTimeZone();

            if (left.isNull() || right.isNull())
                return is_not_monotonic;

            const auto * type_ptr = &type;

            if (const auto * lc_type = checkAndGetDataType<DataTypeLowCardinality>(type_ptr))
                type_ptr = lc_type->getDictionaryType().get();

            if (const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(type_ptr))
                type_ptr = nullable_type->getNestedType().get();

            /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

            if (checkAndGetDataType<DataTypeDate>(type_ptr))
            {
                return Transform::FactorTransform::execute(UInt16(left.safeGet<UInt64>()), *date_lut)
                    == Transform::FactorTransform::execute(UInt16(right.safeGet<UInt64>()), *date_lut)
                    ? is_monotonic : is_not_monotonic;
            }
            if (checkAndGetDataType<DataTypeDate32>(type_ptr))
            {
                return Transform::FactorTransform::execute(Int32(left.safeGet<UInt64>()), *date_lut)
                        == Transform::FactorTransform::execute(Int32(right.safeGet<UInt64>()), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
            }
            if (checkAndGetDataType<DataTypeDateTime>(type_ptr))
            {
                return Transform::FactorTransform::execute(UInt32(left.safeGet<UInt64>()), *date_lut)
                        == Transform::FactorTransform::execute(UInt32(right.safeGet<UInt64>()), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
            }

            assert(checkAndGetDataType<DataTypeDateTime64>(type_ptr));

            const auto & left_date_time = left.safeGet<DateTime64>();
            TransformDateTime64<typename Transform::FactorTransform> transformer_left(left_date_time.getScale());

            const auto & right_date_time = right.safeGet<DateTime64>();
            TransformDateTime64<typename Transform::FactorTransform> transformer_right(right_date_time.getScale());

            return transformer_left.execute(left_date_time.getValue(), *date_lut)
                    == transformer_right.execute(right_date_time.getValue(), *date_lut)
                ? is_monotonic
                : is_not_monotonic;
        }
    }
};

}
