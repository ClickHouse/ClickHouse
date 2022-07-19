#pragma once
#include <type_traits>
#include <Core/AccurateComparison.h>
#include <Common/DateLUTImpl.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Columns/ColumnsNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/TransformDateTime64.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Type of first argument of 'execute' function overload defines what INPUT DataType it is used for.
/// Return type defines what is the OUTPUT (return) type of the CH function.
/// Corresponding types:
///  - UInt16     => DataTypeDate
///  - UInt32     => DataTypeDateTime
///  - DateTime64 => DataTypeDateTime64
/// Please note that INPUT and OUTPUT types may differ, e.g.:
///  - 'AddSecondsImpl::execute(UInt32, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(DateTime, ...) -> DateTime'
///  - 'AddSecondsImpl::execute(UInt16, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(Date, ...) -> DateTime'

struct AddNanosecondsImpl
{
    static constexpr auto name = "addNanoseconds";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 scale = DataTypeDateTime64::default_scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(9 - scale);
        auto division = std::div(t.fractional * multiplier + delta, static_cast<Int64>(1000000000));
        return {t.whole * multiplier + division.quot, t.fractional * multiplier + delta};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(9 - scale);
        return t * multiplier + delta;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(9);
        return t * multiplier + delta;
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(UInt16, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addNanoSeconds() cannot be used with Date", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(Int32, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addNanoSeconds() cannot be used with Date32", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct AddMicrosecondsImpl
{
    static constexpr auto name = "addMicroseconds";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(6 - scale));
        if (scale <= 6)
        {
            auto division = std::div((t.fractional + delta), static_cast<Int64>(10e6));
            return {t.whole * multiplier + division.quot, division.rem};
        }
        else
        {
            auto division = std::div((t.fractional + delta * multiplier), static_cast<Int64>(10e6 * multiplier));
            return {t.whole + division.quot, division.rem};
        }
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(6 - scale));
        return scale <= 6 ? t * multiplier + delta : t + delta * multiplier;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(6);
        return t * multiplier + delta;
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(UInt16, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addMicroSeconds() cannot be used with Date", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(Int32, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addMicroSeconds() cannot be used with Date32", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct AddMillisecondsImpl
{
    static constexpr auto name = "addMilliseconds";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 scale = DataTypeDateTime64::default_scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(3 - scale));
        if (scale <= 3)
        {
            auto division = std::div((t.fractional + delta), static_cast<Int64>(1000));
            return {t.whole * multiplier + division.quot, division.rem};
        }
        else
        {
            auto division = std::div((t.fractional + delta * multiplier), static_cast<Int64>(1000 * multiplier));
            return {t.whole + division.quot,division.rem};
        }
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(3 - scale));
        return scale <= 3 ? t * multiplier + delta : t + delta * multiplier;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(3);
        return t * multiplier + delta;
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(UInt16, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addMilliSeconds() cannot be used with Date", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64 execute(Int32, Int64, const DateLUTImpl &, UInt16 = 0)
    {
        throw Exception("addMilliSeconds() cannot be used with Date32", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return {t.whole + delta, t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        return t + delta * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return t + delta;
    }

    static inline NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        // use default datetime64 scale
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta) * 1000;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta;
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return {t.whole + delta * 60, t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        return t + 60 * delta * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return t + delta * 60;
    }

    static inline NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        // use default datetime64 scale
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta * 60) * 1000;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 60;
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return {t.whole + delta * 3600, t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl &, UInt16 scale = 0)
    {
        return t + 3600 * delta * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return t + delta * 3600;
    }

    static inline NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        // use default datetime64 scale
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta * 3600) * 1000;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 3600;
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return {time_zone.addDays(t.whole, delta), t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale = 0)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addDays(d.quot, delta) * multiplier + d.rem;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addDays(t, delta);
    }

    static inline NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        Int64 r = d + delta;
        if (r < 0)
            return 0;
        if (r > 65535)
            return 65535;
        return static_cast<UInt16>(r);
    }

    static inline NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return std::max(static_cast<Int32>(d + delta), -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch()));
    }
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return {time_zone.addWeeks(t.whole, delta), t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int32 delta, const DateLUTImpl & time_zone, UInt16 scale = 0)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addDays(d.quot, delta * 7) * multiplier + d.rem;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addWeeks(t, delta);
    }

    static inline NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int32 delta, const DateLUTImpl &, UInt16 = 0)
    {
        Int64 r = d + delta * 7;
        if (r < 0)
            return 0;
        if (r > 65535)
            return 65535;
        return static_cast<UInt16>(r);
    }

    static inline NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int32 delta, const DateLUTImpl &, UInt16 = 0)
    {
        return std::max(static_cast<Int32>(d + delta * 7), -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch()));
    }
};

struct AddMonthsImpl
{
    static constexpr auto name = "addMonths";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return {time_zone.addMonths(t.whole, delta), t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale = 0)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addMonths(d.quot, delta) * multiplier + d.rem;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addMonths(t, delta);
    }

    static inline NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addMonths(DayNum(d), delta);
    }

    static inline NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addMonths(ExtendedDayNum(d), delta);
    }
};

struct AddQuartersImpl
{
    static constexpr auto name = "addQuarters";

    static inline DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return {time_zone.addQuarters(t.whole, delta), t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int32 delta, const DateLUTImpl & time_zone, UInt16 scale = 0)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addQuarters(d.quot, delta) * multiplier + d.rem;
    }

    static inline UInt32 execute(UInt32 t, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addQuarters(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addQuarters(DayNum(d), delta);
    }

    static inline Int32 execute(Int32 d, Int32 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addQuarters(ExtendedDayNum(d), delta);
    }
};

struct AddYearsImpl
{
    static constexpr auto name = "addYears";

    static inline NO_SANITIZE_UNDEFINED DecimalUtils::DecimalComponents<DateTime64>
    execute(DecimalUtils::DecimalComponents<DateTime64> t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return {time_zone.addYears(t.whole, delta), t.fractional};
    }

    static inline NO_SANITIZE_UNDEFINED DateTime64
    execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale = 0)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addYears(d.quot, delta) * multiplier + d.rem;
    }

    static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addYears(t, delta);
    }

    static inline NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addYears(DayNum(d), delta);
    }

    static inline NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, UInt16 = 0)
    {
        return time_zone.addYears(ExtendedDayNum(d), delta);
    }
};

template <typename Transform>
struct SubtractIntervalImpl : public Transform
{
    using Transform::Transform;

    template <typename T>
    inline NO_SANITIZE_UNDEFINED auto execute(T t, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        /// Signed integer overflow is Ok.
        return Transform::execute(t, -delta, time_zone, scale);
    }
};

struct SubtractNanosecondsImpl : SubtractIntervalImpl<AddNanosecondsImpl> { static constexpr auto name = "subtractNanoseconds"; };
struct SubtractMicrosecondsImpl : SubtractIntervalImpl<AddMicrosecondsImpl> { static constexpr auto name = "subtractMicroseconds"; };
struct SubtractMillisecondsImpl : SubtractIntervalImpl<AddMillisecondsImpl> { static constexpr auto name = "subtractMilliseconds"; };
struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl> { static constexpr auto name = "subtractSeconds"; };
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl> { static constexpr auto name = "subtractMinutes"; };
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl> { static constexpr auto name = "subtractHours"; };
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl> { static constexpr auto name = "subtractDays"; };
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl> { static constexpr auto name = "subtractWeeks"; };
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl> { static constexpr auto name = "subtractMonths"; };
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl> { static constexpr auto name = "subtractQuarters"; };
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl> { static constexpr auto name = "subtractYears"; };


template <typename Transform>
struct Adder
{
    const Transform transform;

    explicit Adder(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromVectorType, typename ToVectorType>
    void NO_INLINE vectorConstant(const FromVectorType & vec_from, ToVectorType & vec_to, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], checkOverflow(delta), time_zone, scale);
    }

    template <typename FromVectorType, typename ToVectorType>
    void vectorVector(const FromVectorType & vec_from, ToVectorType & vec_to, const IColumn & delta, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ vectorVector(vec_from, vec_to, column, time_zone, scale, size); return true; });
    }

    template <typename FromType, typename ToVectorType>
    void constantVector(const FromType & from, ToVectorType & vec_to, const IColumn & delta, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        size_t size = delta.size();
        vec_to.resize(size);

        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ constantVector(from, vec_to, column, time_zone, scale, size); return true; });
    }

private:

    template <typename Value>
    static Int64 checkOverflow(Value val)
    {
        Int64 result;
        if (accurate::convertNumeric<Value, Int64, false>(val, result))
            return result;
        throw DB::Exception("Numeric overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    template <typename FromVectorType, typename ToVectorType, typename DeltaColumnType>
    NO_INLINE NO_SANITIZE_UNDEFINED void vectorVector(
        const FromVectorType & vec_from, ToVectorType & vec_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], checkOverflow(delta.getData()[i]), time_zone, scale);
    }

    template <typename FromType, typename ToVectorType, typename DeltaColumnType>
    NO_INLINE NO_SANITIZE_UNDEFINED void constantVector(
        const FromType & from, ToVectorType & vec_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(from, checkOverflow(delta.getData()[i]), time_zone, scale);
    }
};


template <typename FromDataType, typename ToDataType, typename Transform>
struct DateTimeAddIntervalImpl
{
    static ColumnPtr execute(Transform transform, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, UInt16 scale = 0)
    {
        using FromValueType = typename FromDataType::FieldType;
        using FromColumnType = typename FromDataType::ColumnType;
        using ToColumnType = typename ToDataType::ColumnType;

        auto op = Adder<Transform>{std::move(transform)};

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        const ColumnPtr source_col = arguments[0].column;

        auto result_col = result_type->createColumn();
        auto col_to = assert_cast<ToColumnType *>(result_col.get());

        const IColumn & delta_column = *arguments[1].column;
        if (const auto * sources = checkAndGetColumn<FromColumnType>(source_col.get()))
        {
            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                op.vectorConstant(sources->getData(), col_to->getData(), delta_const_column->getInt(0), time_zone, scale);
            else
                op.vectorVector(sources->getData(), col_to->getData(), delta_column, time_zone, scale);
        }
        else if (const auto * sources_const = checkAndGetColumnConst<FromColumnType>(source_col.get()))
        {
            op.constantVector(
                sources_const->template getValue<FromValueType>(),
                col_to->getData(), delta_column, time_zone, scale);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                            arguments[0].column->getName(), Transform::name);
        }

        return result_col;
    }
};

namespace date_and_time_type_details
{
// Compile-time mapping of value (DataType::FieldType) types to corresponding DataType
template <typename FieldType> struct ResultDataTypeMap {};
template <> struct ResultDataTypeMap<UInt16>     { using ResultDataType = DataTypeDate; };
template <> struct ResultDataTypeMap<Int16>      { using ResultDataType = DataTypeDate; };
template <> struct ResultDataTypeMap<UInt32>     { using ResultDataType = DataTypeDateTime; };
template <> struct ResultDataTypeMap<Int32>      { using ResultDataType = DataTypeDate32; };
template <> struct ResultDataTypeMap<DateTime64> { using ResultDataType = DataTypeDateTime64; };
template <> struct ResultDataTypeMap<Int64>      { using ResultDataType = DataTypeDateTime64; };
}

template <typename Transform>
class FunctionDateOrDateTimeAddInterval : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeAddInterval>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNativeNumber(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " (delta) must be number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!isDate(arguments[0].type) && !isDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception{"Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
        {
            if (!WhichDataType(arguments[0].type).isDateTime()
                || !WhichDataType(arguments[2].type).isString())
            {
                throw Exception(
                    "Function " + getName() + " supports 2 or 3 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument must be number. "
                    "The 3rd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        switch (arguments[0].type->getTypeId())
        {
            case TypeIndex::Date:
                return resolveReturnType<DataTypeDate>(arguments);
            case TypeIndex::Date32:
                return resolveReturnType<DataTypeDate32>(arguments);
            case TypeIndex::DateTime:
                return resolveReturnType<DataTypeDateTime>(arguments);
            case TypeIndex::DateTime64:
                return resolveReturnType<DataTypeDateTime64>(arguments);
            default:
            {
                throw Exception("Invalid type of 1st argument of function " + getName() + ": "
                    + arguments[0].type->getName() + ", expected: Date, DateTime or DateTime64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }

    /// Helper templates to deduce return type based on argument type, since some overloads may promote or denote types,
    /// e.g. addSeconds(Date, 1) => DateTime
    template <typename FieldType>
    using TransformExecuteReturnType = decltype(std::declval<Transform>().execute(FieldType(), 0, std::declval<DateLUTImpl>(), 0));

    // Deduces RETURN DataType from INPUT DataType, based on return type of Transform{}.execute(INPUT_TYPE, UInt64, DateLUTImpl).
    // e.g. for Transform-type that has execute()-overload with 'UInt16' input and 'UInt32' return,
    // argument type is expected to be 'Date', and result type is deduced to be 'DateTime'.
    template <typename FromDataType>
    using TransformResultDataType = typename date_and_time_type_details::ResultDataTypeMap<TransformExecuteReturnType<typename FromDataType::FieldType>>::ResultDataType;

    template <typename FromDataType>
    DataTypePtr resolveReturnType(const ColumnsWithTypeAndName & arguments) const
    {
        using ResultDataType = TransformResultDataType<FromDataType>;

        if constexpr (std::is_same_v<ResultDataType, DataTypeDate>)
            return std::make_shared<DataTypeDate>();
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDate32>)
            return std::make_shared<DataTypeDate32>();
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime>)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            if (typeid_cast<const DataTypeDateTime64 *>(arguments[0].type.get()))
            {
                const auto & datetime64_type = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type);

                auto from_scale = datetime64_type.getScale();
                auto scale = from_scale;

                if (std::is_same_v<Transform, AddNanosecondsImpl>)
                    scale = 9;
                else if (std::is_same_v<Transform, AddMicrosecondsImpl>)
                    scale = 6;
                else if (std::is_same_v<Transform, AddMillisecondsImpl>)
                    scale = 3;

                scale = std::max(scale, from_scale);

                return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
            }
            else
            {
                auto scale = DataTypeDateTime64::default_scale;

                if (std::is_same_v<Transform, AddNanosecondsImpl>)
                    scale = 9;
                else if (std::is_same_v<Transform, AddMicrosecondsImpl>)
                    scale = 6;
                else if (std::is_same_v<Transform, AddMillisecondsImpl>)
                    scale = 3;

                return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
            }
        }
        else
        {
            static_assert("Failed to resolve return type.");
        }

        //to make PVS and GCC happy.
        return nullptr;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
        {
            return DateTimeAddIntervalImpl<DataTypeDate, TransformResultDataType<DataTypeDate>, Transform>::execute(
                Transform{}, arguments, result_type);
        }
        else if (which.isDate32())
        {
            return DateTimeAddIntervalImpl<DataTypeDate32, TransformResultDataType<DataTypeDate32>, Transform>::execute(
                Transform{}, arguments, result_type);
        }
        else if (which.isDateTime())
        {
            return DateTimeAddIntervalImpl<DataTypeDateTime, TransformResultDataType<DataTypeDateTime>, Transform>::execute(
                Transform{}, arguments, result_type);
        }
        else if (const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(from_type))
        {
            auto from_scale = datetime64_type->getScale();
            return DateTimeAddIntervalImpl<DataTypeDateTime64, TransformResultDataType<DataTypeDateTime64>, Transform>::execute(
                Transform{}, arguments, result_type, from_scale);
        }
        else
            throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

}

