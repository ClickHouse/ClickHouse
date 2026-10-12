#pragma once
#include <Common/IntervalKind.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>

#include <Functions/TransformTime64.h>


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
    /// True iff this function is a calendar-field extractor (`toYear`, `toMonth`,
    /// `toDayOfMonth`, ...) that supports PostgreSQL-style
    /// `EXTRACT(<unit> FROM INTERVAL ...)` on a matching-kind interval. Other
    /// `FunctionDateOrDateTimeToSomething`-shaped transforms (e.g.
    /// `toStartOfDay`, `toUnixTimestamp`) keep rejecting `Interval`.
    bool acceptsIntervalArgument() const
    {
        IntervalKind unused;
        return IntervalKind::tryParseFromNameOfFunctionExtractTimePart(getName(), unused);
    }

    static bool isAcceptableFirstArgument(const DataTypePtr & type, bool accept_interval)
    {
        return isDateOrDate32OrDateTimeOrDateTime64(type) || (accept_interval && isInterval(type));
    }

    void checkArguments(const ColumnsWithTypeAndName & arguments, bool is_result_type_date_or_date32) const
    {
        const bool accept_interval = acceptsIntervalArgument();
        const char * expected = accept_interval
            ? "Date, Date32, DateTime, DateTime64 or Interval"
            : "Date, Date32, DateTime or DateTime64";
        if (arguments.size() == 1)
        {
            if (!isAcceptableFirstArgument(arguments[0].type, accept_interval))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Should be {}",
                    arguments[0].type->getName(), getName(), expected);
        }
        else if (arguments.size() == 2)
        {
            if (!isAcceptableFirstArgument(arguments[0].type, accept_interval))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Should be {}",
                    arguments[0].type->getName(), getName(), expected);
            if (!isString(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports 1 or 2 arguments. The optional 2nd argument must be "
                    "a constant string with a timezone name",
                    getName());
            if (isInterval(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} is not allowed when the 1st argument is an Interval",
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
        if constexpr (requires { Transform::is_monotonic_only_on_each_side_of_epoch; })
        {
            return getMonotonicityOnEachSideOfEpoch(type, left, right);
        }
        else if constexpr (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
        {
            return { .is_monotonic = true, .is_always_monotonic = true };
        }
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
                return extendedFactorForMonotonicity<typename Transform::FactorTransform>(Int32(left.safeGet<UInt64>()), *date_lut)
                        == extendedFactorForMonotonicity<typename Transform::FactorTransform>(Int32(right.safeGet<UInt64>()), *date_lut)
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
            if (checkAndGetDataType<DataTypeTime>(type_ptr))
            {
                return Transform::FactorTransform::execute(UInt32(left.safeGet<UInt64>()), *date_lut)
                        == Transform::FactorTransform::execute(UInt32(right.safeGet<UInt64>()), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
            }
            if (checkAndGetDataType<DataTypeTime64>(type_ptr))
            {
                const auto & left_time = left.safeGet<Time64>();
                TransformTime64<typename Transform::FactorTransform> transformer_left(left_time.getScale());

                const auto & right_time = right.safeGet<Time64>();
                TransformTime64<typename Transform::FactorTransform> transformer_right(right_time.getScale());

                return transformer_left.execute(left_time.getValue(), *date_lut)
                        == transformer_right.execute(right_time.getValue(), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
            }

            if (!checkAndGetDataType<DataTypeDateTime64>(type_ptr))
                return is_not_monotonic;

            const auto & left_date_time = left.safeGet<DateTime64>();
            TransformDateTime64<typename Transform::FactorTransform> transformer_left(left_date_time.getScale());

            const auto & right_date_time = right.safeGet<DateTime64>();
            TransformDateTime64<typename Transform::FactorTransform> transformer_right(right_date_time.getScale());

            /// Use the unclamped extended result so pre-epoch values keep distinct, order-preserving
            /// factors (see extendedFactorForMonotonicity).
            if constexpr (requires { Transform::FactorTransform::executeExtendedResult(Int64{}, *date_lut); })
                return transformer_left.executeExtendedResult(left_date_time.getValue(), *date_lut)
                        == transformer_right.executeExtendedResult(right_date_time.getValue(), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
            else
                return transformer_left.execute(left_date_time.getValue(), *date_lut)
                        == transformer_right.execute(right_date_time.getValue(), *date_lut)
                    ? is_monotonic
                    : is_not_monotonic;
        }
    }

private:
    /// For a transform that is monotonic before the Unix epoch and after it, but not necessarily across it:
    /// `toRelativeHourNum` in a time zone with a whole-hour offset counts hours from the epoch for a time after it, and
    /// with a bias of a day for a time before it, so `1969-12-31 23:00:00` UTC is `23` and `1970-01-01 00:00:00` is `0`.
    /// The values are kept as they are, because they are stored in the partition IDs and the primary keys of existing
    /// parts.
    ///
    /// Whether the values decrease across the split point depends on the time zone, so the transform is evaluated just
    /// before and just after it, in the time zone that it is executed in. If the value does not decrease, the function
    /// is monotonic everywhere: in `Asia/Kolkata` the same formula is used on both sides. Otherwise only a range that is
    /// entirely on one side of the split point is monotonic.
    ///
    /// A `DateTime64` range is split at the epoch. A negative value with a fraction is rounded down to the second before
    /// the epoch, so it is on that side whatever the scale is. A `Date32` range is split at `1970-01-01`: in a time zone
    /// west of UTC the midnight of `1969-12-31` is before the epoch and the one of `1970-01-01` is after it. A `Date`
    /// range is split after `1970-01-01`, because the value of that day is not clamped for `Date` and wraps around in a
    /// time zone with an offset of more than 12 hours, such as `Pacific/Tongatapu` or `Pacific/Chatham`. A `DateTime`
    /// is never before the epoch. A `NULL` bound of a range is an infinite one.
    static Monotonicity getMonotonicityOnEachSideOfEpoch(const IDataType & type, const Field & left, const Field & right)
    {
        const IFunction::Monotonicity is_always_monotonic = { .is_monotonic = true, .is_always_monotonic = true };
        const IFunction::Monotonicity is_monotonic = { .is_monotonic = true };
        const IFunction::Monotonicity is_not_monotonic;

        const auto * type_ptr = &type;
        if (const auto * lc_type = checkAndGetDataType<DataTypeLowCardinality>(type_ptr))
            type_ptr = lc_type->getDictionaryType().get();
        if (const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(type_ptr))
            type_ptr = nullable_type->getNestedType().get();

        /// The same time zone as `extractTimeZoneFromFunctionArguments` picks. A time zone argument is attached to the
        /// type by `KeyCondition` for `DateTime64`, and a `Date` or `Date32` with one is not considered monotonic there.
        const DateLUTImpl * date_lut = &DateLUT::instance();
        if (const auto * timezone = dynamic_cast<const TimezoneMixin *>(type_ptr))
            date_lut = &timezone->getTimeZone();

        bool (*is_before_split)(const Field &) = nullptr;
        if (const auto * date_time64_type = checkAndGetDataType<DataTypeDateTime64>(type_ptr))
        {
            const TransformDateTime64<Transform> transform(date_time64_type->getScale());
            if (transform.execute(DateTime64(-1), *date_lut) <= transform.execute(DateTime64(0), *date_lut))
                return is_always_monotonic;
            is_before_split = [](const Field & value) { return value.safeGet<DateTime64>().getValue().value < 0; };
        }
        else if (checkAndGetDataType<DataTypeDate32>(type_ptr))
        {
            if (Transform::execute(Int32(-1), *date_lut) <= Transform::execute(Int32(0), *date_lut))
                return is_always_monotonic;
            is_before_split = [](const Field & value) { return value.safeGet<Int64>() < 0; };
        }
        else if (checkAndGetDataType<DataTypeDate>(type_ptr))
        {
            if (Transform::execute(UInt16(0), *date_lut) <= Transform::execute(UInt16(1), *date_lut))
                return is_always_monotonic;
            is_before_split = [](const Field & value) { return value.safeGet<UInt64>() == 0; };
        }
        else
            return is_always_monotonic;

        const bool left_is_before_split = left.isNull() ? !left.isPositiveInfinity() : is_before_split(left);
        const bool right_is_before_split = right.isNull() ? right.isNegativeInfinity() : is_before_split(right);
        return left_is_before_split == right_is_before_split ? is_monotonic : is_not_monotonic;
    }

public:
    bool hasInformationAboutPreimage() const override
    {
        if constexpr (requires { Transform::hasPreimage(); })
            return Transform::hasPreimage();

        return false;
    }

    FieldIntervalPtr getPreimage(const IDataType & type, const Field & point) const override
    {
        if constexpr (requires { Transform::hasPreimage(); })
        {
            if constexpr (Transform::hasPreimage())
                return Transform::getPreimage(type, point);
        }

        return IFunction::getPreimage(type, point);
    }
};

}
