#include <numeric>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTimeWindow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int SYNTAX_ERROR;
}

namespace
{
    std::tuple<IntervalKind::Kind, Int64>
    dispatchForIntervalColumns(const ColumnWithTypeAndName & interval_column, const String & function_name)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal column " + interval_column.name + " of argument of function " + function_name, ErrorCodes::ILLEGAL_COLUMN);
        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(
                "Illegal column " + interval_column.name + " of argument of function " + function_name, ErrorCodes::ILLEGAL_COLUMN);
        Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception(
                "Value for column " + interval_column.name + " of function " + function_name + " must be positive",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        return {interval_type->getKind(), num_units};
    }

    ColumnPtr executeWindowBound(const ColumnPtr & column, int index, const String & function_name)
    {
        if (const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column.get()); col_tuple)
        {
            if (!checkColumn<ColumnVector<UInt32>>(*col_tuple->getColumnPtr(index)))
                throw Exception(
                    "Illegal column for first argument of function " + function_name + ". Must be a Tuple(DataTime, DataTime)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return col_tuple->getColumnPtr(index);
        }
        else
        {
            throw Exception(
                "Illegal column for first argument of function " + function_name + ". Must be Tuple",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    void checkFirstArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isDateTime(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". Should be a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name, IntervalKind & interval_kind, bool & result_type_is_date)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(argument.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". Should be an interval of time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        interval_kind = interval_type->getKind();
        result_type_is_date = (interval_type->getKind() == IntervalKind::Year) || (interval_type->getKind() == IntervalKind::Quarter)
            || (interval_type->getKind() == IntervalKind::Month) || (interval_type->getKind() == IntervalKind::Week);
    }

    void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name, bool & result_type_is_date)
    {
        IntervalKind interval_kind;
        checkIntervalArgument(argument, function_name, interval_kind, result_type_is_date);
    }

    void checkTimeZoneArgument(
        const ColumnWithTypeAndName & argument,
        const String & function_name)
    {
        if (!WhichDataType(argument.type).isString())
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". This argument is optional and must be a constant string with timezone name",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool checkIntervalOrTimeZoneArgument(const ColumnWithTypeAndName & argument, const String & function_name, IntervalKind & interval_kind, bool & result_type_is_date)
    {
        if (WhichDataType(argument.type).isString())
        {
            checkTimeZoneArgument(argument, function_name);
            return false;
        }
        checkIntervalArgument(argument, function_name, interval_kind, result_type_is_date);
        return true;
    }
}

template <>
struct TimeWindowImpl<TUMBLE>
{
    static constexpr auto name = "tumble";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;

        if (arguments.size() == 2)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, result_type_is_date);
        }
        else if (arguments.size() == 3)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, result_type_is_date);
            checkTimeZoneArgument(arguments.at(2), function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr data_type = nullptr;
        if (result_type_is_date)
            data_type = std::make_shared<DataTypeDate>();
        else
            data_type = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));

        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto & from_datatype = *time_column.type.get();
        const auto which_type = WhichDataType(from_datatype);
        const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        if (!which_type.isDateTime() || !time_column_vec)
            throw Exception(
                "Illegal column " + time_column.name + " of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto interval = dispatchForIntervalColumns(interval_column, function_name);

        switch (std::get<0>(interval))
        {
                //TODO: add proper support for fractional seconds
//            case IntervalKind::Nanosecond:
//                return executeTumble<UInt32, IntervalKind::Nanosecond>(*time_column_vec, std::get<1>(interval), time_zone);
//            case IntervalKind::Microsecond:
//                return executeTumble<UInt32, IntervalKind::Microsecond>(*time_column_vec, std::get<1>(interval), time_zone);
//            case IntervalKind::Millisecond:
//                return executeTumble<UInt32, IntervalKind::Millisecond>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Second:
                return executeTumble<UInt32, IntervalKind::Second>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Minute:
                return executeTumble<UInt32, IntervalKind::Minute>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Hour:
                return executeTumble<UInt32, IntervalKind::Hour>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Day:
                return executeTumble<UInt32, IntervalKind::Day>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Week:
                return executeTumble<UInt16, IntervalKind::Week>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Month:
                return executeTumble<UInt16, IntervalKind::Month>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Quarter:
                return executeTumble<UInt16, IntervalKind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Year:
                return executeTumble<UInt16, IntervalKind::Year>(*time_column_vec, std::get<1>(interval), time_zone);
            default:
                throw Exception("Fraction seconds are unsupported by windows yet", ErrorCodes::SYNTAX_ERROR);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind unit>
    static ColumnPtr executeTumble(const ColumnUInt32 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        auto start = ColumnVector<ToType>::create();
        auto end = ColumnVector<ToType>::create();
        auto & start_data = start->getData();
        auto & end_data = end->getData();
        start_data.resize(size);
        end_data.resize(size);
        for (size_t i = 0; i != size; ++i)
        {
            start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone);
            end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone);
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

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        if (arguments.size() == 1)
        {
            auto type = WhichDataType(arguments[0].type);
            if (type.isTuple())
                return std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElement(0);
            else if (type.isUInt32())
                return std::make_shared<DataTypeDateTime>();
            else
                throw Exception(
                    "Illegal type of first argument of function " + function_name + " should be DateTime, Tuple or UInt32",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<TUMBLE>::getReturnType(arguments, function_name))
                ->getElement(0);
        }
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            else //isTuple
                result_column = time_column.column;
        }
        else
            result_column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        return executeWindowBound(result_column, 0, function_name);
    }
};

template <>
struct TimeWindowImpl<TUMBLE_END>
{
    static constexpr auto name = "tumbleEnd";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return TimeWindowImpl<TUMBLE_START>::getReturnType(arguments, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String& function_name)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            else //isTuple
                result_column = time_column.column;
        }
        else
            result_column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        return executeWindowBound(result_column, 1, function_name);
    }
};

template <>
struct TimeWindowImpl<HOP>
{
    static constexpr auto name = "hop";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;
        IntervalKind interval_kind_1;
        IntervalKind interval_kind_2;

        if (arguments.size() == 3)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments.at(2), function_name, interval_kind_2, result_type_is_date);
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments.at(2), function_name, interval_kind_2, result_type_is_date);
            checkTimeZoneArgument(arguments.at(3), function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (interval_kind_1 != interval_kind_2)
            throw Exception(
                "Illegal type of window and hop column of function " + function_name + ", must be same", ErrorCodes::ILLEGAL_COLUMN);

        DataTypePtr data_type = nullptr;
        if (result_type_is_date)
            data_type = std::make_shared<DataTypeDate>();
        else
            data_type = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0));
        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto & from_datatype = *time_column.type.get();
        const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);
        if (!WhichDataType(from_datatype).isDateTime() || !time_column_vec)
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name
                    + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
                //TODO: add proper support for fractional seconds
//            case IntervalKind::Nanosecond:
//                return executeHop<UInt32, IntervalKind::Nanosecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
//            case IntervalKind::Microsecond:
//                return executeHop<UInt32, IntervalKind::Microsecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
//            case IntervalKind::Millisecond:
//                return executeHop<UInt32, IntervalKind::Millisecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Second:
                return executeHop<UInt32, IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHop<UInt32, IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHop<UInt32, IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHop<UInt32, IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHop<UInt16, IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHop<UInt16, IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHop<UInt16, IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHop<UInt16, IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            default:
                throw Exception("Fraction seconds are unsupported by windows yet", ErrorCodes::SYNTAX_ERROR);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind kind>
    static ColumnPtr
    executeHop(const ColumnUInt32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        auto start = ColumnVector<ToType>::create();
        auto end = ColumnVector<ToType>::create();
        auto & start_data = start->getData();
        auto & end_data = end->getData();
        start_data.resize(size);
        end_data.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            ToType wstart = ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone);
            ToType wend = AddTime<kind>::execute(wstart, hop_num_units, time_zone);
            wstart = AddTime<kind>::execute(wend, -1 * window_num_units, time_zone);
            ToType wend_latest;

            do
            {
                wend_latest = wend;
                wend = AddTime<kind>::execute(wend, -1 * hop_num_units, time_zone);
            } while (wend > time_data[i]);

            end_data[i] = wend_latest;
            start_data[i] = AddTime<kind>::execute(wend_latest, -1 * window_num_units, time_zone);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
struct TimeWindowImpl<WINDOW_ID>
{
    static constexpr auto name = "windowID";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;
        IntervalKind interval_kind_1;
        IntervalKind interval_kind_2;

        if (arguments.size() == 2)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, interval_kind_1, result_type_is_date);
        }
        else if (arguments.size() == 3)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, interval_kind_1, result_type_is_date);
            if (checkIntervalOrTimeZoneArgument(arguments.at(2), function_name, interval_kind_2, result_type_is_date))
            {
                if (interval_kind_1 != interval_kind_2)
                    throw Exception(
                        "Illegal type of window and hop column of function " + function_name + ", must be same", ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments.at(0), function_name);
            checkIntervalArgument(arguments.at(1), function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments.at(2), function_name, interval_kind_2, result_type_is_date);
            checkTimeZoneArgument(arguments.at(3), function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2, 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (result_type_is_date)
            return std::make_shared<DataTypeUInt16>();
        else
            return std::make_shared<DataTypeUInt32>();
    }

    [[maybe_unused]] static ColumnPtr
    dispatchForHopColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto & from_datatype = *time_column.type.get();
        const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);
        if (!WhichDataType(from_datatype).isDateTime() || !time_column_vec)
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name
                    + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
                //TODO: add proper support for fractional seconds
//            case IntervalKind::Nanosecond:
//                return executeHopSlice<UInt32, IntervalKind::Nanosecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
//            case IntervalKind::Microsecond:
//                return executeHopSlice<UInt32, IntervalKind::Microsecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
//            case IntervalKind::Millisecond:
//                return executeHopSlice<UInt32, IntervalKind::Millisecond>(
//                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Second:
                return executeHopSlice<UInt32, IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHopSlice<UInt32, IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHopSlice<UInt32, IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHopSlice<UInt32, IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHopSlice<UInt16, IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHopSlice<UInt16, IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHopSlice<UInt16, IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHopSlice<UInt16, IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            default:
                throw Exception("Fraction seconds are unsupported by windows yet", ErrorCodes::SYNTAX_ERROR);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind kind>
    static ColumnPtr
    executeHopSlice(const ColumnUInt32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        Int64 gcd_num_units = std::gcd(hop_num_units, window_num_units);

        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto end = ColumnVector<ToType>::create();
        auto & end_data = end->getData();
        end_data.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            ToType wstart = ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone);
            ToType wend = AddTime<kind>::execute(wstart, hop_num_units, time_zone);
            ToType wend_latest;

            do
            {
                wend_latest = wend;
                wend = AddTime<kind>::execute(wend, -1 * gcd_num_units, time_zone);
            } while (wend > time_data[i]);

            end_data[i] = wend_latest;
        }
        return end;
    }

    [[maybe_unused]] static ColumnPtr
    dispatchForTumbleColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        ColumnPtr column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        return executeWindowBound(column, 1, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        if (arguments.size() == 2)
            return dispatchForTumbleColumns(arguments, function_name);
        else
        {
            const auto & third_column = arguments[2];
            if (arguments.size() == 3 && WhichDataType(third_column.type).isString())
                return dispatchForTumbleColumns(arguments, function_name);
            else
                return dispatchForHopColumns(arguments, function_name);
        }
    }
};

template <>
struct TimeWindowImpl<HOP_START>
{
    static constexpr auto name = "hopStart";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        if (arguments.size() == 1)
        {
            auto type = WhichDataType(arguments[0].type);
            if (type.isTuple())
                return std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElement(0);
            else if (type.isUInt32())
                return std::make_shared<DataTypeDateTime>();
            else
                throw Exception(
                    "Illegal type of first argument of function " + function_name + " should be DateTime, Tuple or UInt32",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<HOP>::getReturnType(arguments, function_name))->getElement(0);
        }
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            else //isTuple
                result_column = time_column.column;
        }
        else
            result_column = TimeWindowImpl<HOP>::dispatchForColumns(arguments, function_name);
        return executeWindowBound(result_column, 0, function_name);
    }
};

template <>
struct TimeWindowImpl<HOP_END>
{
    static constexpr auto name = "hopEnd";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return TimeWindowImpl<HOP_START>::getReturnType(arguments, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            else //isTuple
                result_column = time_column.column;
        }
        else
            result_column = TimeWindowImpl<HOP>::dispatchForColumns(arguments, function_name);

        return executeWindowBound(result_column, 1, function_name);
    }
};

template <TimeWindowFunctionName type>
DataTypePtr FunctionTimeWindow<type>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    return TimeWindowImpl<type>::getReturnType(arguments, name);
}

template <TimeWindowFunctionName type>
ColumnPtr FunctionTimeWindow<type>::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const
{
    return TimeWindowImpl<type>::dispatchForColumns(arguments, name);
}

void registerFunctionsTimeWindow(FunctionFactory& factory)
{
    factory.registerFunction<FunctionTumble>();
    factory.registerFunction<FunctionHop>();
    factory.registerFunction<FunctionTumbleStart>();
    factory.registerFunction<FunctionTumbleEnd>();
    factory.registerFunction<FunctionHopStart>();
    factory.registerFunction<FunctionHopEnd>();
    factory.registerFunction<FunctionWindowId>();
}
}
