#pragma once
#include <type_traits>
#include <Core/AccurateComparison.h>
#include <Core/DecimalFunctions.h>
#include <Common/DateLUTImpl.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
///  - Int32      => DataTypeDate32
///  - DateTime64 => DataTypeDateTime64
///  - Int8       => error
/// Please note that INPUT and OUTPUT types may differ, e.g.:
///  - 'AddSecondsImpl::execute(UInt32, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(DateTime, ...) -> DateTime'
///  - 'AddSecondsImpl::execute(UInt16, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(Date, ...) -> DateTime'

struct AddNanosecondsImpl
{
    static constexpr auto name = "addNanoseconds";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(9 - scale);
        return DateTime64(DecimalUtils::multiplyAdd(t.value, multiplier, delta));
    }
    static NO_SANITIZE_UNDEFINED DateTime64 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(9);
        return DateTime64(DecimalUtils::multiplyAdd(static_cast<Int64>(t), multiplier, delta));
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(UInt16, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addNanoseconds() cannot be used with Date");
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(Int32, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addNanoseconds() cannot be used with Date32");
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddMicrosecondsImpl
{
    static constexpr auto name = "addMicroseconds";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(6 - scale));
        return DateTime64(scale <= 6
            ? DecimalUtils::multiplyAdd(t.value, multiplier, delta)
            : DecimalUtils::multiplyAdd(delta, multiplier, t.value));
    }
    static NO_SANITIZE_UNDEFINED DateTime64 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(6);
        return DateTime64(DecimalUtils::multiplyAdd(static_cast<Int64>(t), multiplier, delta));
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(UInt16, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMicroseconds() cannot be used with Date");
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(Int32, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMicroseconds() cannot be used with Date32");
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddMillisecondsImpl
{
    static constexpr auto name = "addMilliseconds";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(std::abs(3 - scale));
        return DateTime64(scale <= 3
            ? DecimalUtils::multiplyAdd(t.value, multiplier, delta)
            : DecimalUtils::multiplyAdd(delta, multiplier, t.value));
    }
    static NO_SANITIZE_UNDEFINED DateTime64 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(3);
        return DateTime64(DecimalUtils::multiplyAdd(static_cast<Int64>(t), multiplier, delta));
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(UInt16, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMilliseconds() cannot be used with Date");
    }
    static NO_SANITIZE_UNDEFINED Int8 execute(Int32, Int64, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMilliseconds() cannot be used with Date32");
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        return DateTime64(DecimalUtils::multiplyAdd(delta, DecimalUtils::scaleMultiplier<DateTime64>(scale), t.value));
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(t + delta);
    }
    static NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        // use default datetime64 scale
        static_assert(DataTypeDateTime64::default_scale == 3);
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta) * 1000;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)) + delta);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        return t + 60 * delta * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(t + delta * 60);
    }
    static NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        // use default datetime64 scale
        static_assert(DataTypeDateTime64::default_scale == 3);
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta * 60) * 1000;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)) + delta * 60);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16 scale)
    {
        return t + 3600 * delta * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(t + delta * 3600);
    }
    static NO_SANITIZE_UNDEFINED Int64 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        // use default datetime64 scale
        static_assert(DataTypeDateTime64::default_scale == 3);
        return (time_zone.fromDayNum(ExtendedDayNum(d)) + delta * 3600) * 1000;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)) + delta * 3600);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16 scale)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addDays(d.quot, delta) * multiplier + d.rem;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.addDays(t, delta));
    }
    static NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return d + delta;
    }
    static NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<Int32>(d + delta);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16 scale)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addDays(d.quot, delta * 7) * multiplier + d.rem;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.addWeeks(t, delta));
    }
    static NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt16>(d + delta * 7);
    }
    static NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl &, const DateLUTImpl &, UInt16)
    {
        return static_cast<Int32>(d + delta * 7);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddMonthsImpl
{
    static constexpr auto name = "addMonths";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16 scale)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addMonths(d.quot, delta) * multiplier + d.rem;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.addMonths(t, delta));
    }
    static NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addMonths(DayNum(d), delta);
    }
    static NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addMonths(ExtendedDayNum(d), delta);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddQuartersImpl
{
    static constexpr auto name = "addQuarters";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16 scale)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addQuarters(d.quot, delta) * multiplier + d.rem;
    }
    static UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.addQuarters(t, delta));
    }
    static UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addQuarters(DayNum(d), delta);
    }
    static Int32 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addQuarters(ExtendedDayNum(d), delta);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

struct AddYearsImpl
{
    static constexpr auto name = "addYears";

    static NO_SANITIZE_UNDEFINED DateTime64 execute(DateTime64 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16 scale)
    {
        auto multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        auto d = std::div(t, multiplier);
        return time_zone.addYears(d.quot, delta) * multiplier + d.rem;
    }
    static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return static_cast<UInt32>(time_zone.addYears(t, delta));
    }
    static NO_SANITIZE_UNDEFINED UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addYears(DayNum(d), delta);
    }
    static NO_SANITIZE_UNDEFINED Int32 execute(Int32 d, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl &, UInt16)
    {
        return time_zone.addYears(ExtendedDayNum(d), delta);
    }
    static DateTime64 execute(std::string_view s, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale)
    {
        ReadBufferFromString buf(s);
        DateTime64 t;
        parseDateTime64BestEffort(t, scale, buf, time_zone, utc_time_zone);
        return execute(t, delta, time_zone, utc_time_zone, scale);
    }
};

template <typename Transform>
struct SubtractIntervalImpl : public Transform
{
    using Transform::Transform;

    template <typename T>
    NO_SANITIZE_UNDEFINED auto execute(T t, Int64 delta, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone, UInt16 scale) const
    {
        /// Signed integer overflow is Ok.
        return Transform::execute(t, -delta, time_zone, utc_time_zone, scale);
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
struct Processor
{
    const Transform transform;

    explicit Processor(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromColumnType, typename ToColumnType>
    void NO_INLINE vectorConstant(const FromColumnType & col_from, ToColumnType & col_to, Int64 delta, const DateLUTImpl & time_zone, UInt16 scale, size_t input_rows_count) const
    {
        static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

        if constexpr (std::is_same_v<FromColumnType, ColumnString>)
        {
            auto & vec_to = col_to.getData();

            vec_to.resize(input_rows_count);

            for (size_t i = 0 ; i < input_rows_count; ++i)
            {
                std::string_view from = col_from.getDataAt(i).toView();
                vec_to[i] = transform.execute(from, checkOverflow(delta), time_zone, utc_time_zone, scale);
            }
        }
        else
        {
            const auto & vec_from = col_from.getData();
            auto & vec_to = col_to.getData();

            vec_to.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
                vec_to[i] = transform.execute(vec_from[i], checkOverflow(delta), time_zone, utc_time_zone, scale);
        }
    }

    template <typename FromColumnType, typename ToColumnType>
    void vectorVector(const FromColumnType & col_from, ToColumnType & col_to, const IColumn & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t input_rows_count) const
    {
        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ vectorVector(col_from, col_to, column, time_zone, scale, input_rows_count); return true; });
    }

    template <typename FromType, typename ToColumnType>
    void constantVector(const FromType & from, ToColumnType & col_to, const IColumn & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t input_rows_count) const
    {
        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ constantVector(from, col_to, column, time_zone, scale, input_rows_count); return true; });
    }

private:
    template <typename Value>
    static Int64 checkOverflow(Value val)
    {
        Int64 result;
        if (accurate::convertNumeric<Value, Int64, false>(val, result))
            return result;
        throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
    }

    template <typename FromColumnType, typename ToColumnType, typename DeltaColumnType>
    NO_INLINE NO_SANITIZE_UNDEFINED void vectorVector(
        const FromColumnType & col_from, ToColumnType & col_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t input_rows_count) const
    {
        static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

        if constexpr (std::is_same_v<FromColumnType, ColumnString>)
        {
            auto & vec_to = col_to.getData();

            vec_to.resize(input_rows_count);

            for (size_t i = 0 ; i < input_rows_count; ++i)
            {
                std::string_view from = col_from.getDataAt(i).toView();
                vec_to[i] = transform.execute(from, checkOverflow(delta.getData()[i]), time_zone, utc_time_zone, scale);
            }
        }
        else
        {
            const auto & vec_from = col_from.getData();
            auto & vec_to = col_to.getData();

            vec_to.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
                vec_to[i] = transform.execute(vec_from[i], checkOverflow(delta.getData()[i]), time_zone, utc_time_zone, scale);
        }
    }

    template <typename FromType, typename ToColumnType, typename DeltaColumnType>
    NO_INLINE NO_SANITIZE_UNDEFINED void constantVector(
        const FromType & from, ToColumnType & col_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, UInt16 scale, size_t input_rows_count) const
    {
        static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

        auto & vec_to = col_to.getData();

        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
            vec_to[i] = transform.execute(from, checkOverflow(delta.getData()[i]), time_zone, utc_time_zone, scale);
    }
};


template <typename FromDataType, typename ToDataType, typename Transform>
struct DateTimeAddIntervalImpl
{
    static ColumnPtr execute(Transform transform, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, UInt16 scale, size_t input_rows_count)
    {
        using FromValueType = typename FromDataType::FieldType;
        using FromColumnType = typename FromDataType::ColumnType;
        using ToColumnType = typename ToDataType::ColumnType;

        const IColumn & source_column = *arguments[0].column;
        const IColumn & delta_column = *arguments[1].column;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto result_col = result_type->createColumn();
        auto col_to = assert_cast<ToColumnType *>(result_col.get());

        auto processor = Processor<Transform>{std::move(transform)};

        if (const auto * sources = checkAndGetColumn<FromColumnType>(&source_column))
        {
            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                processor.vectorConstant(*sources, *col_to, delta_const_column->getInt(0), time_zone, scale, input_rows_count);
            else
                processor.vectorVector(*sources, *col_to, delta_column, time_zone, scale, input_rows_count);
        }
        else if (const auto * sources_const = checkAndGetColumnConst<FromColumnType>(&source_column))
        {
            processor.constantVector(
                sources_const->template getValue<FromValueType>(),
                *col_to, delta_column, time_zone, scale, input_rows_count);
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
template <> struct ResultDataTypeMap<UInt32>     { using ResultDataType = DataTypeDateTime; };
template <> struct ResultDataTypeMap<Int32>      { using ResultDataType = DataTypeDate32; };
template <> struct ResultDataTypeMap<DateTime64> { using ResultDataType = DataTypeDateTime64; };
template <> struct ResultDataTypeMap<Int64>      { using ResultDataType = DataTypeDateTime64; };
template <> struct ResultDataTypeMap<Int8>       { using ResultDataType = DataTypeInt8; }; // error
}

template <typename Transform>
class FunctionDateOrDateTimeAddInterval : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeAddInterval>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());

        if (arguments.size() == 2)
        {
            if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type) && !isString(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}. "
                    "Must be a date, a date with time or a String", arguments[0].type->getName(), getName());
        }
        else
        {
            if (!WhichDataType(arguments[0].type).isDateTimeOrDateTime64())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}. "
                    "Must be a DateTime/DateTime64", arguments[0].type->getName(), getName());

            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}. "
                                "The 3rd argument must be a constant string with a timezone name. "
                                "The timezone argument is allowed only when the 1st argument has the type DateTime",
                                arguments[2].type->getName(), getName());
        }

        if (!isNativeNumber(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be a number", getName());

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
            case TypeIndex::String:
                return resolveReturnType<DataTypeString>(arguments);
            default:
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid type of 1st argument of function {}: "
                    "{}, expected: Date, DateTime or DateTime64.", getName(), arguments[0].type->getName());
        }
    }

    /// Helper templates to deduce return type based on argument type, since some overloads may promote or denote types,
    /// e.g. addSeconds(Date, 1) => DateTime
    template <typename FieldType>
    using TransformExecuteReturnType = decltype(std::declval<Transform>().execute(FieldType(), 0, std::declval<DateLUTImpl>(), std::declval<DateLUTImpl>(), 0));

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
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            static constexpr auto target_scale = std::invoke(
                []() -> std::optional<UInt32>
                {
                    if constexpr (std::is_base_of_v<AddNanosecondsImpl, Transform>)
                        return 9;
                    else if constexpr (std::is_base_of_v<AddMicrosecondsImpl, Transform>)
                        return 6;
                    else if constexpr (std::is_base_of_v<AddMillisecondsImpl, Transform>)
                        return 3;

                    return {};
                });

            auto timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false);
            if (const auto* datetime64_type = typeid_cast<const DataTypeDateTime64 *>(arguments[0].type.get()))
            {
                const auto from_scale = datetime64_type->getScale();
                return std::make_shared<DataTypeDateTime64>(std::max(from_scale, target_scale.value_or(from_scale)), std::move(timezone));
            }

            return std::make_shared<DataTypeDateTime64>(target_scale.value_or(DataTypeDateTime64::default_scale), std::move(timezone));
        }
        else if constexpr (std::is_same_v<ResultDataType, DataTypeInt8>)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} cannot be used with {}", getName(), arguments[0].type->getName());

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type in datetime add interval function");
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeAddIntervalImpl<DataTypeDate, TransformResultDataType<DataTypeDate>, Transform>::execute(Transform{}, arguments, result_type, 0, input_rows_count);
        if (which.isDate32())
            return DateTimeAddIntervalImpl<DataTypeDate32, TransformResultDataType<DataTypeDate32>, Transform>::execute(
                Transform{}, arguments, result_type, 0, input_rows_count);
        if (which.isDateTime())
            return DateTimeAddIntervalImpl<DataTypeDateTime, TransformResultDataType<DataTypeDateTime>, Transform>::execute(
                Transform{}, arguments, result_type, 0, input_rows_count);
        if (which.isDateTime64())
        {
            const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(from_type);
            auto from_scale = datetime64_type->getScale();
            return DateTimeAddIntervalImpl<DataTypeDateTime64, TransformResultDataType<DataTypeDateTime64>, Transform>::execute(
                Transform{}, arguments, result_type, from_scale, input_rows_count);
        }
        if (which.isString())
            return DateTimeAddIntervalImpl<DataTypeString, DataTypeDateTime64, Transform>::execute(
                Transform{}, arguments, result_type, 3, input_rows_count);
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument of function {}",
            arguments[0].type->getName(),
            getName());
    }
};

}
