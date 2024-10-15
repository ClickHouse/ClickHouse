#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/IFunction.h>
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

template <typename Transform>
class IFunctionCustomWeek : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        if constexpr (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
            return {.is_monotonic = true, .is_always_monotonic = true};

        const IFunction::Monotonicity is_monotonic = {.is_monotonic = true};
        const IFunction::Monotonicity is_not_monotonic;

        /// This method is called only if the function has one argument. Therefore, we do not care about the non-local time zone.
        const DateLUTImpl & date_lut = DateLUT::instance();

        if (left.isNull() || right.isNull())
            return {};

        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

        if (checkAndGetDataType<DataTypeDate>(&type))
        {
            return Transform::FactorTransform::execute(UInt16(left.safeGet<UInt64>()), date_lut)
                    == Transform::FactorTransform::execute(UInt16(right.safeGet<UInt64>()), date_lut)
                ? is_monotonic
                : is_not_monotonic;
        }

        return Transform::FactorTransform::execute(UInt32(left.safeGet<UInt64>()), date_lut)
                == Transform::FactorTransform::execute(UInt32(right.safeGet<UInt64>()), date_lut)
            ? is_monotonic
            : is_not_monotonic;
    }

protected:
    void checkArguments(const ColumnsWithTypeAndName & arguments, bool is_result_type_date_or_date32, bool value_may_be_string) const
    {
        if (arguments.size() == 1)
        {
            auto type0 = arguments[0].type;
            if (!isDate(type0) && !isDate32(type0) && !isDateTime(type0) && !isDateTime64(type0) && !(value_may_be_string && isString(type0)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Must be Date, Date32, DateTime or DateTime64.",
                    type0->getName(), getName());
        }
        else if (arguments.size() == 2)
        {
            auto type0 = arguments[0].type;
            auto type1 = arguments[1].type;
            if (!isDate(type0) && !isDate32(type0) && !isDateTime(type0) && !isDateTime64(type0) && !(value_may_be_string && isString(type0)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 1st argument of function {}. Must be Date, Date32, DateTime or DateTime64.",
                    type0->getName(), getName());
            if (!isUInt8(type1))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd (optional) argument of function {}. Must be constant UInt8 (week mode).",
                    type1->getName(), getName());
        }
        else if (arguments.size() == 3)
        {
            auto type0 = arguments[0].type;
            auto type1 = arguments[1].type;
            auto type2 = arguments[2].type;
            if (!isDate(type0) && !isDate32(type0) && !isDateTime(type0) && !isDateTime64(type0) && !(value_may_be_string && isString(type0)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Must be Date, Date32, DateTime or DateTime64",
                    type0->getName(), getName());
            if (!isUInt8(type1))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd (optional) argument of function {}. Must be constant UInt8 (week mode).",
                    type1->getName(), getName());
            if (!isString(type2))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 3rd (optional) argument of function {}. Must be constant string (timezone name).",
                    type2->getName(), getName());
            if (is_result_type_date_or_date32 && (isDate(type0) || isDate32(type0)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} is allowed only when the 1st argument is DateTime or DateTime64.",
                    getName());
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1, 2 or 3.",
                getName(), arguments.size());
    }

};

}
