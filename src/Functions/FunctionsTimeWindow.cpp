#include <numeric>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTimeWindow.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/IntervalKind.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::tuple<IntervalKind::Kind, Int64>
dispatchForIntervalColumns(const ColumnWithTypeAndName & interval_column, const String & function_name)
{
    const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
    if (!interval_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
            interval_column.name, function_name);
    const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
    if (!interval_column_const_int64)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
            interval_column.name, function_name);
    Int64 num_units = interval_column_const_int64->getValue<Int64>();
    if (num_units <= 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value for column {} of function {} must be positive",
            interval_column.name, function_name);

    return {interval_type->getKind(), num_units};
}

ColumnPtr executeWindowBound(const ColumnPtr & column, size_t index, const String & function_name)
{
    chassert(index == 0 || index == 1);
    if (const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column.get()); col_tuple)
    {
        if (index >= col_tuple->tupleSize()
            || (!checkColumn<ColumnVector<UInt32>>(*col_tuple->getColumnPtr(index))
                && !checkColumn<ColumnVector<UInt16>>(*col_tuple->getColumnPtr(index))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for first argument of function {}. "
                "Must be a Tuple(DateTime, DateTime)", function_name);
        return col_tuple->getColumnPtr(index);
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal column for first argument of function {}. "
        "Must be Tuple",
        function_name);
}

IntervalKind extractIntervalKind(const IDataType * argument)
{
    const auto * interval_type = checkAndGetDataType<DataTypeInterval>(argument);
    chassert(interval_type);
    return interval_type->getKind();
}

IntervalKind extractIntervalKind(const ColumnWithTypeAndName & argument)
{
    return extractIntervalKind(argument.type.get());
}

bool isIntervalDate(IntervalKind kind)
{
    return kind == IntervalKind::Kind::Year || kind == IntervalKind::Kind::Quarter || kind == IntervalKind::Kind::Month || kind == IntervalKind::Kind::Week;
}

bool isTupleOfTwoDateTimes(const IDataType & type)
{
    if (!isTuple(type))
        return false;

    const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(&type);
    if (!tuple_type || tuple_type->getElements().size() != 2)
        return false;

    const auto & elems = tuple_type->getElements();
    // Check if elements are Date/DateTime, allowing nullable versions
    auto check_elem = [](const DataTypePtr & elem)
    {
        auto base_type = removeNullable(elem);
        return isDate(base_type) || isDateTime(base_type);
    };

    return check_elem(elems[0]) && check_elem(elems[1]);
}

enum TimeWindowFunctionName
{
    TUMBLE,
    TUMBLE_START,
    TUMBLE_END,
    HOP,
    HOP_START,
    HOP_END
};

template <TimeWindowFunctionName type>
struct TimeWindowImpl
{
    static constexpr auto name = "UNKNOWN";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name);

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count);
};

template <TimeWindowFunctionName type>
class FunctionTimeWindow final : public IFunction
{
public:
    static constexpr auto name = TimeWindowImpl<type>::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeWindow>(); }

    explicit FunctionTimeWindow(bool is_monotonic_ = false) : is_monotonic(is_monotonic_) {}

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    /// At the zero time, a hop window longer than its hop would start before the epoch.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return type != HOP && type != HOP_START && type != HOP_END; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    /// The bounds of a hopping window are non-decreasing in the time if the start of the hop interval is:
    /// the end is the start of the hop interval plus the hop, and the start is the end minus the window.
    /// The `hop` tuple is then non-decreasing in lexicographic order. Whether the start of the hop interval is
    /// non-decreasing depends on the interval and the time zone, see `isHopMonotonic`.
    [[nodiscard]] bool hasInformationAboutMonotonicity() const override { return is_monotonic; }

    [[nodiscard]] Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        if (!is_monotonic)
            return {};
        return {.is_monotonic = true, .is_always_monotonic = true};
    }

    /// No declarative signature yet. These variants (`tumble`, `tumbleStart`,
    /// `tumbleEnd`, `hop`, `hopStart`, `hopEnd`) return either a `DateTime`
    /// (for `*Start`/`*End`) or a `Tuple(DateTime, DateTime)` (for
    /// `tumble` and `hop`). The DSL has no return-type union, so a documentation-only
    /// `... -> DateTime | Tuple(...)` cannot be parsed — it raises `SYNTAX_ERROR` on the
    /// types-only path. Keep it empty until return-type unions are supported; the
    /// `getReturnTypeImpl(ColumnsWithTypeAndName)` override resolves the exact shape.
    [[nodiscard]] String getSignatureString() const override
    {
        return "";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override;

private:
    const bool is_monotonic;
};

/// Whether `toStartOfHourInterval(t, hours)` is non-decreasing in `t` over the range of `DateTime`.
/// Multi-hour intervals are counted from the local midnight in the hour numbers after an offset change,
/// so on a day when the clocks go back the start of the interval can move backwards: in `Pacific/Chatham`
/// the clocks go from 03:45 back to 02:45, and with three-hour intervals the start moves from 03:00 to 00:00.
/// Walks all the days of `DateTime`, so it is called once per function, not per row.
bool isStartOfHourIntervalMonotonic(const DateLUTImpl & time_zone, UInt64 hours)
{
    /// One-hour intervals are rounded on a fixed grid or from the local midnight, without a jump.
    if (hours <= 1)
        return true;

    /// The time of an offset change and its amount are multiples of 15 minutes from the local midnight.
    static constexpr Int64 step = 900;
    static constexpr Int64 max_time = std::numeric_limits<UInt32>::max();

    /// On a day without an offset change the time since the local midnight is rounded down, and the result always
    /// lies between the midnight and the time, so the days are ordered and only the days with a change are checked.
    Int64 day_begin = time_zone.fromDayNum(ExtendedDayNum(0));
    for (Int32 day = 0; day_begin <= max_time; ++day)
    {
        const Int64 day_end = time_zone.fromDayNum(ExtendedDayNum(day + 1));
        if (day_end - day_begin != 86400)
        {
            Int64 prev = 0;
            for (Int64 time = std::max<Int64>(day_begin, 0); time < day_end && time <= max_time; time += step)
            {
                const Int64 start = time_zone.toStartOfHourInterval(static_cast<UInt32>(time), hours);
                if (start < prev)
                    return false;
                prev = start;
            }
        }
        day_begin = day_end;
    }

    return true;
}

/// Whether the bounds of the hopping window are non-decreasing functions of the time, for the given arguments.
bool isHopMonotonic(const ColumnsWithTypeAndName & arguments)
{
    /// `hopStart(window)` and `hopEnd(window)` only extract an element of a window tuple.
    if (arguments.size() < 3)
        return false;

    /// The arguments may still be wrapped into `Nullable` and `LowCardinality` here.
    auto nested_arguments = createBlockWithNestedColumns(arguments);
    for (auto & argument : nested_arguments)
    {
        argument.type = recursiveRemoveLowCardinality(argument.type);
        argument.column = recursiveRemoveLowCardinality(argument.column);
    }

    const auto * hop_interval_type = checkAndGetDataType<DataTypeInterval>(nested_arguments[1].type.get());
    if (!hop_interval_type)
        return false;

    /// The calendar intervals round the number of the local day, which is non-decreasing in the time.
    /// One-hour and shorter intervals are rounded on a fixed grid or from the local midnight, without a jump.
    if (hop_interval_type->getKind() != IntervalKind::Kind::Hour)
        return true;

    /// Multi-hour intervals are counted from the local midnight in the hour numbers after an offset change,
    /// so the start of the interval can move backwards when the clocks go back.
    const auto * hop_interval_column = checkAndGetColumnConst<ColumnInt64>(nested_arguments[1].column.get());
    if (!hop_interval_column)
        return false;

    const Int64 hop_num_units = hop_interval_column->getValue<Int64>();
    if (hop_num_units <= 0)
        return false;

    const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(nested_arguments, 3, 0);
    return isStartOfHourIntervalMonotonic(time_zone, static_cast<UInt64>(hop_num_units));
}

/// Builds the hopping window functions with the information about monotonicity,
/// which depends on the hop interval and the time zone.
template <TimeWindowFunctionName type>
class HopOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = TimeWindowImpl<type>::name;
    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<HopOverloadResolver>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return TimeWindowImpl<type>::getReturnType(arguments, name);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        auto function = std::make_shared<FunctionTimeWindow<type>>(isHopMonotonic(arguments));

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<FunctionToFunctionBaseAdaptor>(function, data_types, return_type);
    }
};

using FunctionTumble = FunctionTimeWindow<TUMBLE>;
using FunctionTumbleStart = FunctionTimeWindow<TUMBLE_START>;
using FunctionTumbleEnd = FunctionTimeWindow<TUMBLE_END>;
using FunctionHop = HopOverloadResolver<HOP>;
using FunctionHopStart = HopOverloadResolver<HOP_START>;
using FunctionHopEnd = HopOverloadResolver<HOP_END>;


template <>
struct TimeWindowImpl<TUMBLE>
{
    static constexpr auto name = "tumble";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"time_column", &isDateTime, nullptr, "DateTime"},
            {"interval", &isInterval, nullptr, "Interval"}
        };
        FunctionArgumentDescriptors optional_args{
            {"timezone", &isString, nullptr, "String"}
        };
        validateFunctionArguments(function_name, arguments, mandatory_args, optional_args);

        IntervalKind interval_kind = extractIntervalKind(arguments.at(1));
        DataTypePtr data_type = nullptr;

        if (isIntervalDate(interval_kind))
            data_type = std::make_shared<DataTypeDate>();
        else
            data_type = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));

        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto & from_datatype = *time_column.type.get();
        const auto which_type = WhichDataType(from_datatype);
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        if (!which_type.isDateTime() || !time_column_vec)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column {} of function {}. "
                "Must contain dates or dates with time", time_column.name, function_name);

        auto interval = dispatchForIntervalColumns(interval_column, function_name);

        switch (std::get<0>(interval))
        {
            /// TODO: add proper support for fractional seconds
            case IntervalKind::Kind::Second:
                return executeTumble<UInt32, IntervalKind::Kind::Second>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Minute:
                return executeTumble<UInt32, IntervalKind::Kind::Minute>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Hour:
                return executeTumble<UInt32, IntervalKind::Kind::Hour>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Day:
                return executeTumble<UInt32, IntervalKind::Kind::Day>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Week:
                return executeTumble<UInt16, IntervalKind::Kind::Week>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Month:
                return executeTumble<UInt16, IntervalKind::Kind::Month>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Quarter:
                return executeTumble<UInt16, IntervalKind::Kind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Year:
                return executeTumble<UInt16, IntervalKind::Kind::Year>(*time_column_vec, std::get<1>(interval), time_zone, input_rows_count);
            default:
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Fraction seconds are unsupported by windows yet");
        }
    }

    template <typename ToType, IntervalKind::Kind unit>
    static ColumnPtr executeTumble(const ColumnDateTime & time_column, UInt64 num_units, const DateLUTImpl & time_zone, size_t input_rows_count)
    {
        const auto & time_data = time_column.getData();
        auto start = ColumnVector<ToType>::create();
        auto end = ColumnVector<ToType>::create();
        auto & start_data = start->getData();
        auto & end_data = end->getData();
        start_data.resize(input_rows_count);
        end_data.resize(input_rows_count);
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone);
            end_data[i] = static_cast<ToType>(AddTime<unit>::execute(start_data[i], num_units, time_zone));
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
struct TimeWindowImpl<TUMBLE_START>
{
    static constexpr auto name = "tumbleStart";

    static DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t tuple_index)
    {
        if (arguments.size() == 1)
        {
            FunctionArgumentDescriptors mandatory_args{
                {"window_tuple", &isTupleOfTwoDateTimes, nullptr, "Tuple(Date/DateTime, Date/DateTime)"}
            };
            validateFunctionArguments(function_name, arguments, mandatory_args, {});

            const auto & tuple_elems = std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElements();
            return tuple_elems[tuple_index];
        }

        return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<TUMBLE>::getReturnType(arguments, function_name))
            ->getElement(tuple_index);
    }

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return getReturnTypeImpl(arguments, function_name, 0);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        ColumnPtr result_column;
        if (arguments.size() == 1)
            result_column = time_column.column;
        else
            result_column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name, input_rows_count);
        return executeWindowBound(result_column, 0, function_name);
    }
};

template <>
struct TimeWindowImpl<TUMBLE_END>
{
    static constexpr auto name = "tumbleEnd";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return TimeWindowImpl<TUMBLE_START>::getReturnTypeImpl(arguments, function_name, 1);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String& function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        ColumnPtr result_column;
        if (arguments.size() == 1)
            result_column = time_column.column;
        else
            result_column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name, input_rows_count);
        return executeWindowBound(result_column, 1, function_name);
    }
};

template <>
struct TimeWindowImpl<HOP>
{
    static constexpr auto name = "hop";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"time_column", &isDateTime, nullptr, "DateTime"},
            {"hop_interval", &isInterval, nullptr, "Interval"},
            {"window_interval", &isInterval, nullptr, "Interval"}
        };
        FunctionArgumentDescriptors optional_args{
            {"timezone", &isString, nullptr, "String"}
        };
        validateFunctionArguments(function_name, arguments, mandatory_args, optional_args);

        IntervalKind hop_interval_kind = extractIntervalKind(arguments.at(1));
        if (hop_interval_kind != extractIntervalKind(arguments.at(2)))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal type of window and hop column of function {}, must be same",
                function_name);

        DataTypePtr data_type = nullptr;
        if (isIntervalDate(hop_interval_kind))
            data_type = std::make_shared<DataTypeDate>();
        else
            data_type = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0, false));
        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto & from_datatype = *time_column.type.get();
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);
        if (!WhichDataType(from_datatype).isDateTime() || !time_column_vec)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column {} argument of function {}. "
                "Must contain dates or dates with time", time_column.name, function_name);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Value for hop interval of function {} must not larger than window interval", function_name);

        switch (std::get<0>(window_interval))
        {
            /// TODO: add proper support for fractional seconds
            case IntervalKind::Kind::Second:
                return executeHop<UInt32, IntervalKind::Kind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Minute:
                return executeHop<UInt32, IntervalKind::Kind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Hour:
                return executeHop<UInt32, IntervalKind::Kind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Day:
                return executeHop<UInt32, IntervalKind::Kind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Week:
                return executeHop<UInt16, IntervalKind::Kind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Month:
                return executeHop<UInt16, IntervalKind::Kind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Quarter:
                return executeHop<UInt16, IntervalKind::Kind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Year:
                return executeHop<UInt16, IntervalKind::Kind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            default:
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Fraction seconds are unsupported by windows yet");
        }
    }

    /// Adds `delta` units of `kind` to a window bound without wrapping around and throws if the result does not fit into `ToType`.
    /// Together with the monotonic start of the hop interval, this makes the window bounds monotonic functions of the time.
    template <typename ToType, IntervalKind::Kind kind>
    static ToType addUnitsChecked(Int64 value, Int64 delta, const DateLUTImpl & time_zone)
    {
        Int64 result = 0;
        bool overflow = false;
        if constexpr (kind == IntervalKind::Kind::Month)
            result = time_zone.addMonths(ExtendedDayNum(static_cast<Int32>(value)), delta);
        else if constexpr (kind == IntervalKind::Kind::Quarter)
        {
            /// `addQuarters` multiplies by 3 with wraparound, so count the months here and check the multiplication.
            Int64 months = 0;
            overflow = common::mulOverflow(delta, static_cast<Int64>(3), months);
            if (!overflow)
                result = time_zone.addMonths(ExtendedDayNum(static_cast<Int32>(value)), months);
        }
        else if constexpr (kind == IntervalKind::Kind::Year)
            result = time_zone.addYears(ExtendedDayNum(static_cast<Int32>(value)), delta);
        else
        {
            /// Weeks are counted in days, as the bounds are `Date`, and the other units in seconds.
            constexpr Int64 unit = kind == IntervalKind::Kind::Week ? 7
                : kind == IntervalKind::Kind::Day ? 86400
                : kind == IntervalKind::Kind::Hour ? 3600
                : kind == IntervalKind::Kind::Minute ? 60
                : 1;
            Int64 shift = 0;
            overflow = common::mulOverflow(delta, unit, shift) || common::addOverflow(value, shift, result);
        }

        /// The calendar arithmetic saturates at the ends of the calendar, far outside of the range of `Date`.
        if (overflow || result < 0 || result > static_cast<Int64>(std::numeric_limits<ToType>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time overflow in function {}", name);
        return static_cast<ToType>(result);
    }

    template <typename ToType, IntervalKind::Kind kind>
    static ColumnPtr
    executeHop(const ColumnDateTime & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone, size_t input_rows_count)
    {
        const auto & time_data = time_column.getData();
        auto start = ColumnVector<ToType>::create();
        auto end = ColumnVector<ToType>::create();
        auto & start_data = start->getData();
        auto & end_data = end->getData();
        start_data.resize(input_rows_count);
        end_data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// The first window containing the time ends one hop after the start of its hop interval.
            const Int64 hop_start = ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone);
            const ToType wend = addUnitsChecked<ToType, kind>(hop_start, hop_num_units, time_zone);
            end_data[i] = wend;
            start_data[i] = addUnitsChecked<ToType, kind>(wend, -static_cast<Int64>(window_num_units), time_zone);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
struct TimeWindowImpl<HOP_START>
{
    static constexpr auto name = "hopStart";

    static DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t tuple_index)
    {
        if (arguments.size() == 1)
        {
            FunctionArgumentDescriptors mandatory_args{
                {"window_tuple", &isTupleOfTwoDateTimes, nullptr, "Tuple(Date/DateTime, Date/DateTime)"}
            };
            validateFunctionArguments(function_name, arguments, mandatory_args, {});

            const auto & tuple_elems = std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElements();
            return tuple_elems[tuple_index];
        }

        return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<HOP>::getReturnType(arguments, function_name))->getElement(tuple_index);
    }

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return getReturnTypeImpl(arguments, function_name, 0);
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        ColumnPtr result_column;
        if (arguments.size() == 1)
            result_column = time_column.column;
        else
            result_column = TimeWindowImpl<HOP>::dispatchForColumns(arguments, function_name, input_rows_count);
        return executeWindowBound(result_column, 0, function_name);
    }
};

template <>
struct TimeWindowImpl<HOP_END>
{
    static constexpr auto name = "hopEnd";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return TimeWindowImpl<HOP_START>::getReturnTypeImpl(arguments, function_name, 1);
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        ColumnPtr result_column;
        if (arguments.size() == 1)
            result_column = time_column.column;
        else
            result_column = TimeWindowImpl<HOP>::dispatchForColumns(arguments, function_name, input_rows_count);

        return executeWindowBound(result_column, 1, function_name);
    }
};

template <TimeWindowFunctionName type>
DataTypePtr FunctionTimeWindow<type>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    return TimeWindowImpl<type>::getReturnType(arguments, name);
}

template <TimeWindowFunctionName type>
ColumnPtr FunctionTimeWindow<type>::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    return TimeWindowImpl<type>::dispatchForColumns(arguments, name, input_rows_count);
}

}

REGISTER_FUNCTION(TimeWindow)
{
    FunctionDocumentation::Description description_tumble = R"(
A tumbling time window assigns records to non-overlapping, continuous windows with a fixed duration (`interval`).
    )";
    FunctionDocumentation::Syntax syntax_tumble = "tumble(time_attr, interval[, timezone])";
    FunctionDocumentation::Arguments arguments_tumble = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"interval", "Window interval in Interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tumble = {"Returns the inclusive lower and exclusive upper bound of the corresponding tumbling window.", {"Tuple(DateTime, DateTime)"}};
    FunctionDocumentation::Examples examples_tumble = {{"Tumbling window", "SELECT tumble(toDateTime('2026-01-02 03:04:05', 'UTC'), toIntervalDay('1'))", "('2026-01-02 00:00:00','2026-01-03 00:00:00')"}};
    FunctionDocumentation::IntroducedIn introduced_in_tumble = {21, 12};
    FunctionDocumentation::Category category_tumble = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_tumble = {description_tumble, syntax_tumble, arguments_tumble, {}, returned_value_tumble, examples_tumble, introduced_in_tumble, category_tumble};

    FunctionDocumentation::Description description_tumble_start = R"(
Returns the inclusive lower bound of the corresponding tumbling window.
    )";
    FunctionDocumentation::Syntax syntax_tumble_start = "tumbleStart(time_attr, interval[, timezone])";
    FunctionDocumentation::Arguments arguments_tumble_start = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"interval", "Window interval in Interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tumble_start = {"Returns the inclusive lower bound of the corresponding tumbling window.", {"DateTime"}};
    FunctionDocumentation::Examples examples_tumble_start = {{"Tumbling window start", "SELECT tumbleStart(toDateTime('2026-01-02 03:04:05', 'UTC'), toIntervalDay('1'))", "2026-01-02 00:00:00"}};
    FunctionDocumentation::IntroducedIn introduced_in_tumble_start = {22, 1};
    FunctionDocumentation::Category category_tumble_start = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_tumble_start = {description_tumble_start, syntax_tumble_start, arguments_tumble_start, {}, returned_value_tumble_start, examples_tumble_start, introduced_in_tumble_start, category_tumble_start};

    FunctionDocumentation::Description description_tumble_end = R"(
Returns the exclusive upper bound of the corresponding tumbling window.
    )";
    FunctionDocumentation::Syntax syntax_tumble_end = "tumbleEnd(time_attr, interval[, timezone])";
    FunctionDocumentation::Arguments arguments_tumble_end = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"interval", "Window interval in Interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tumble_end = {"Returns the exclusive upper bound of the corresponding tumbling window.", {"DateTime"}};
    FunctionDocumentation::Examples examples_tumble_end = {{"Tumbling window end", "SELECT tumbleEnd(toDateTime('2026-01-02 03:04:05', 'UTC'), toIntervalDay('1'))", "2026-01-03 00:00:00"}};
    FunctionDocumentation::IntroducedIn introduced_in_tumble_end = {22, 1};
    FunctionDocumentation::Category category_tumble_end = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_tumble_end = {description_tumble_end, syntax_tumble_end, arguments_tumble_end, {}, returned_value_tumble_end, examples_tumble_end, introduced_in_tumble_end, category_tumble_end};

    FunctionDocumentation::Description description_hop = R"(
A hopping time window has a fixed duration (`window_interval`) and hops by a specified hop interval (`hop_interval`). If the `hop_interval` is smaller than the `window_interval`, hopping windows are overlapping. Thus, records can be assigned to multiple windows.

Since one record can be assigned to multiple hop windows, the function only returns the bound of the first window.
    )";
    FunctionDocumentation::Syntax syntax_hop = "hop(time_attr, hop_interval, window_interval[, timezone])";
    FunctionDocumentation::Arguments arguments_hop = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"hop_interval", "Positive Hop interval.", {"Interval"}},
        {"window_interval", "Positive Window interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hop = {"Returns the inclusive lower and exclusive upper bound of the corresponding hopping window.", {"Tuple(DateTime, DateTime)"}};
    FunctionDocumentation::Examples examples_hop = {{"Hopping window", "SELECT hop(toDateTime('2026-01-02 03:04:05', 'UTC'), INTERVAL '1' DAY, INTERVAL '2' DAY)", "('2026-01-01 00:00:00','2026-01-03 00:00:00')"}};
    FunctionDocumentation::IntroducedIn introduced_in_hop = {21, 12};
    FunctionDocumentation::Category category_hop = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_hop = {description_hop, syntax_hop, arguments_hop, {}, returned_value_hop, examples_hop, introduced_in_hop, category_hop};

    FunctionDocumentation::Description description_hop_start = R"(
Returns the inclusive lower bound of the corresponding hopping window.

Since one record can be assigned to multiple hop windows, the function only returns the bound of the first window.
    )";
    FunctionDocumentation::Syntax syntax_hop_start = "hopStart(time_attr, hop_interval, window_interval[, timezone])";
    FunctionDocumentation::Arguments arguments_hop_start = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"hop_interval", "Positive Hop interval.", {"Interval"}},
        {"window_interval", "Positive Window interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hop_start = {"Returns the inclusive lower bound of the corresponding hopping window.", {"DateTime"}};
    FunctionDocumentation::Examples examples_hop_start = {{"Hopping window start", "SELECT hopStart(toDateTime('2026-01-02 03:04:05', 'UTC'), INTERVAL '1' DAY, INTERVAL '2' DAY)", "2026-01-01 00:00:00"}};
    FunctionDocumentation::IntroducedIn introduced_in_hop_start = {22, 1};
    FunctionDocumentation::Category category_hop_start = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_hop_start = {description_hop_start, syntax_hop_start, arguments_hop_start, {}, returned_value_hop_start, examples_hop_start, introduced_in_hop_start, category_hop_start};

    FunctionDocumentation::Description description_hop_end = R"(
Returns the exclusive upper bound of the corresponding hopping window.

Since one record can be assigned to multiple hop windows, the function only returns the bound of the first window.
    )";
    FunctionDocumentation::Syntax syntax_hop_end = "hopEnd(time_attr, hop_interval, window_interval[, timezone])";
    FunctionDocumentation::Arguments arguments_hop_end = {
        {"time_attr", "Date and time.", {"DateTime"}},
        {"hop_interval", "Positive Hop interval.", {"Interval"}},
        {"window_interval", "Positive Window interval.", {"Interval"}},
        {"timezone", "Optional. Timezone name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hop_end = {"Returns the exclusive upper bound of the corresponding hopping window.", {"DateTime"}};
    FunctionDocumentation::Examples examples_hop_end = {{"Hopping window end", "SELECT hopEnd(toDateTime('2026-01-02 03:04:05', 'UTC'), INTERVAL '1' DAY, INTERVAL '2' DAY)", "2026-01-03 00:00:00"}};
    FunctionDocumentation::IntroducedIn introduced_in_hop_end = {22, 1};
    FunctionDocumentation::Category category_hop_end = FunctionDocumentation::Category::TimeWindow;
    FunctionDocumentation documentation_hop_end = {description_hop_end, syntax_hop_end, arguments_hop_end, {}, returned_value_hop_end, examples_hop_end, introduced_in_hop_end, category_hop_end};

    factory.registerFunction<FunctionTumble>(documentation_tumble);
    factory.registerFunction<FunctionTumbleStart>(documentation_tumble_start);
    factory.registerFunction<FunctionTumbleEnd>(documentation_tumble_end);
    factory.registerFunction<FunctionHop>(documentation_hop);
    factory.registerFunction<FunctionHopStart>(documentation_hop_start);
    factory.registerFunction<FunctionHopEnd>(documentation_hop_end);
}
}
