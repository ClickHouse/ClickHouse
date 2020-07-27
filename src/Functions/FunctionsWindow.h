#pragma once

#include <numeric>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <IO/WriteHelpers.h>
#include <common/DateLUT.h>

#include "IFunctionImpl.h"

namespace DB
{

/** Window functions:
  *
  * TUMBLE(time_attr, interval [, timezone])
  * 
  * TUMBLE_START(window_id)
  * 
  * TUMBLE_START(time_attr, interval [, timezone])
  * 
  * TUMBLE_END(window_id)
  * 
  * TUMBLE_END(time_attr, interval [, timezone])
  * 
  * HOP(time_attr, hop_interval, window_interval [, timezone])
  * 
  * HOP_START(window_id)
  * 
  * HOP_START(time_attr, hop_interval, window_interval [, timezone])
  * 
  * HOP_END(window_id)
  * 
  * HOP_END(time_attr, hop_interval, window_interval [, timezone])
  * 
  */
enum WindowFunctionName
{
    TUMBLE,
    TUMBLE_START,
    TUMBLE_END,
    HOP,
    HOP_START,
    HOP_END,
    WINDOW_ID
};
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

template <IntervalKind::Kind unit>
struct ToStartOfTransform;

#define TRANSFORM_DATE(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static DayNum execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(t), delta); \
        } \
    };
    TRANSFORM_DATE(Year)
    TRANSFORM_DATE(Quarter)
    TRANSFORM_DATE(Month)
    TRANSFORM_DATE(Week)
#undef TRANSFORM_DATE

    template <>
    struct ToStartOfTransform<IntervalKind::Day>
    {
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfDayInterval(time_zone.toDayNum(t), delta);
        }
    };

#define TRANSFORM_TIME(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(t, delta); \
        } \
    };
    TRANSFORM_TIME(Hour)
    TRANSFORM_TIME(Minute)
    TRANSFORM_TIME(Second)
#undef TRANSFORM_DATE

    template <IntervalKind::Kind unit>
    struct AddTime;

#define ADD_DATE(INTERVAL_KIND) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static DayNum execute(UInt16 d, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.add##INTERVAL_KIND##s(DayNum(d), delta); \
        } \
    };
    ADD_DATE(Year)
    ADD_DATE(Quarter)
    ADD_DATE(Month)
#undef ADD_DATE

    template <>
    struct AddTime<IntervalKind::Week>
    {
        static DayNum execute(UInt16 d, UInt64 delta, const DateLUTImpl &) { return DayNum(d + 7 * delta);}
    };

#define ADD_TIME(INTERVAL_KIND, INTERVAL) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return t + INTERVAL * delta; } \
    };
    ADD_TIME(Day, 86400)
    ADD_TIME(Hour, 3600)
    ADD_TIME(Minute, 60)
    ADD_TIME(Second, 1)
#undef ADD_TIME

namespace
{
    static std::tuple<IntervalKind::Kind, Int64>
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

    static ColumnPtr executeWindowBound(const ColumnPtr & column, int index, const String & function_name)
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
        auto interval_type = checkAndGetDataType<DataTypeInterval>(argument.type.get());
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


    template <WindowFunctionName type>
    struct WindowImpl
    {
        static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name);
    };

    template <>
    struct WindowImpl<TUMBLE>
    {
        static constexpr auto name = "TUMBLE";

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

            DataTypePtr dataType = nullptr;
            if (result_type_is_date)
                dataType = std::make_shared<DataTypeDate>();
            else
                dataType = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));

            return std::make_shared<DataTypeTuple>(DataTypes{dataType, dataType});
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto & interval_column = block.getByPosition(arguments[1]);
            const auto & from_datatype = *time_column.type.get();
            const auto which_type = WhichDataType(from_datatype);
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
            const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);
            if (!which_type.isDateTime() || !time_column_vec)
                throw Exception(
                    "Illegal column " + time_column.name + " of function " + function_name + ". Must contain dates or dates with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto interval = dispatchForIntervalColumns(interval_column, function_name);

            switch (std::get<0>(interval))
            {
                case IntervalKind::Second:
                    return execute_tumble<UInt32, IntervalKind::Second>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Minute:
                    return execute_tumble<UInt32, IntervalKind::Minute>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Hour:
                    return execute_tumble<UInt32, IntervalKind::Hour>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Day:
                    return execute_tumble<UInt32, IntervalKind::Day>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Week:
                    return execute_tumble<UInt16, IntervalKind::Week>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Month:
                    return execute_tumble<UInt16, IntervalKind::Month>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Quarter:
                    return execute_tumble<UInt16, IntervalKind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Year:
                    return execute_tumble<UInt16, IntervalKind::Year>(*time_column_vec, std::get<1>(interval), time_zone);
            }
            __builtin_unreachable();
        }

        template <typename ToType, IntervalKind::Kind unit>
        static ColumnPtr execute_tumble(const ColumnUInt32 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
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
    struct WindowImpl<TUMBLE_START>
    {
        static constexpr auto name = "TUMBLE_START";

        static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
        {
            if (arguments.size() == 1)
            {
                auto type_ = WhichDataType(arguments[0].type);
                if (!type_.isTuple() && !type_.isUInt32())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be DateTime, Tuple or UInt32", ErrorCodes::ILLEGAL_COLUMN);
            }
            else
            {
                WindowImpl<TUMBLE>::getReturnType(arguments, function_name);
            }
            return std::make_shared<DataTypeDateTime>();
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto which_type = WhichDataType(time_column.type);
            ColumnPtr result_column_;
            if (which_type.isDateTime())
                result_column_ = WindowImpl<TUMBLE>::dispatchForColumns(block, arguments, function_name);
            else
                result_column_ = block.getByPosition(arguments[0]).column;
            return executeWindowBound(result_column_, 0, function_name);
        }
    };


    template <>
    struct WindowImpl<TUMBLE_END>
    {
        static constexpr auto name = "TUMBLE_END";

        [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
        {
            return WindowImpl<TUMBLE_START>::getReturnType(arguments, function_name);
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto which_type = WhichDataType(time_column.type);
            ColumnPtr result_column_;
            if (which_type.isDateTime())
                result_column_ = WindowImpl<TUMBLE>::dispatchForColumns(block, arguments, function_name);
            else
                result_column_ = block.getByPosition(arguments[0]).column;
            return executeWindowBound(result_column_, 1, function_name);
        }
    };

    template <>
    struct WindowImpl<HOP>
    {
        static constexpr auto name = "HOP";

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

            DataTypePtr dataType = nullptr;
            if (result_type_is_date)
                dataType = std::make_shared<DataTypeDate>();
            else
                dataType = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0));
            return std::make_shared<DataTypeTuple>(DataTypes{dataType, dataType});
        }

        static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto & hop_interval_column = block.getByPosition(arguments[1]);
            const auto & window_interval_column = block.getByPosition(arguments[2]);
            const auto & from_datatype = *time_column.type.get();
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
            const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 3, 0);
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
                case IntervalKind::Second:
                    return execute_hop<UInt32, IntervalKind::Second>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Minute:
                    return execute_hop<UInt32, IntervalKind::Minute>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Hour:
                    return execute_hop<UInt32, IntervalKind::Hour>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Day:
                    return execute_hop<UInt32, IntervalKind::Day>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Week:
                    return execute_hop<UInt16, IntervalKind::Week>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Month:
                    return execute_hop<UInt16, IntervalKind::Month>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Quarter:
                    return execute_hop<UInt16, IntervalKind::Quarter>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Year:
                    return execute_hop<UInt16, IntervalKind::Year>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            }
            __builtin_unreachable();
        }

        template <typename ToType, IntervalKind::Kind kind>
        static ColumnPtr
        execute_hop(const ColumnUInt32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
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

                ToType wend_ = wend;
                ToType wend_latest;

                do
                {
                    wend_latest = wend_;
                    wend_ = AddTime<kind>::execute(wend_, -1 * hop_num_units, time_zone);
                } while (wend_ > time_data[i]);

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
    struct WindowImpl<WINDOW_ID>
    {
        static constexpr auto name = "WINDOW_ID";

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
        dispatchForHopColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto & hop_interval_column = block.getByPosition(arguments[1]);
            const auto & window_interval_column = block.getByPosition(arguments[2]);
            const auto & from_datatype = *time_column.type.get();
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
            const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 3, 0);
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
                case IntervalKind::Second:
                    return execute_hop_slice<UInt32, IntervalKind::Second>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Minute:
                    return execute_hop_slice<UInt32, IntervalKind::Minute>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Hour:
                    return execute_hop_slice<UInt32, IntervalKind::Hour>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Day:
                    return execute_hop_slice<UInt32, IntervalKind::Day>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Week:
                    return execute_hop_slice<UInt16, IntervalKind::Week>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Month:
                    return execute_hop_slice<UInt16, IntervalKind::Month>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Quarter:
                    return execute_hop_slice<UInt16, IntervalKind::Quarter>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Year:
                    return execute_hop_slice<UInt16, IntervalKind::Year>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            }
            __builtin_unreachable();
        }

        template <typename ToType, IntervalKind::Kind kind>
        static ColumnPtr
        execute_hop_slice(const ColumnUInt32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
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

                ToType wend_ = wend;
                ToType wend_latest;

                do
                {
                    wend_latest = wend_;
                    wend_ = AddTime<kind>::execute(wend_, -1 * gcd_num_units, time_zone);
                } while (wend_ > time_data[i]);

                end_data[i] = wend_latest;
            }
            return end;
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForTumbleColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            ColumnPtr column = WindowImpl<TUMBLE>::dispatchForColumns(block, arguments, function_name);
            return executeWindowBound(column, 1, function_name);
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            if (arguments.size() == 2)
                return dispatchForTumbleColumns(block, arguments, function_name);
            else
            {
                const auto & third_column = block.getByPosition(arguments[2]);
                if (arguments.size() == 3 && WhichDataType(third_column.type).isString())
                    return dispatchForTumbleColumns(block, arguments, function_name);
                else
                    return dispatchForHopColumns(block, arguments, function_name);
            }
        }
    };

    template <>
    struct WindowImpl<HOP_START>
    {
        static constexpr auto name = "HOP_START";

        static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
        {
            if (arguments.size() == 1)
            {
                auto type_ = WhichDataType(arguments[0].type);
                if (!type_.isTuple() && !type_.isUInt32())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be DateTime, Tuple or UInt32",
                        ErrorCodes::ILLEGAL_COLUMN);
            }
            else
            {
                WindowImpl<HOP>::getReturnType(arguments, function_name);
            }
            return std::make_shared<DataTypeDateTime>();
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto which_type = WhichDataType(time_column.type);
            ColumnPtr result_column_;
            if (arguments.size() == 1)
            {
                if (which_type.isUInt32())
                    return time_column.column;
                else //isTuple
                    result_column_ = time_column.column;
            }
            else
                result_column_ = WindowImpl<HOP>::dispatchForColumns(block, arguments, function_name);
            return executeWindowBound(result_column_, 0, function_name);
        }
    };

    template <>
    struct WindowImpl<HOP_END>
    {
        static constexpr auto name = "HOP_END";

        [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
        {
            return WindowImpl<HOP_START>::getReturnType(arguments, function_name);
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto which_type = WhichDataType(time_column.type);
            ColumnPtr result_column_;
            if (arguments.size() == 1)
            {
                if (which_type.isUInt32())
                    return time_column.column;
                else //isTuple
                    result_column_ = time_column.column;
            }
            else
                result_column_ = WindowImpl<HOP>::dispatchForColumns(block, arguments, function_name);

            return executeWindowBound(result_column_, 1, function_name);
        }
    };
};

template <WindowFunctionName type>
class FunctionWindow : public IFunction
{
public:
    static constexpr auto name = WindowImpl<type>::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionWindow>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override { return WindowImpl<type>::getReturnType(arguments, name); }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto result_column = WindowImpl<type>::dispatchForColumns(block, arguments, name);
        block.getByPosition(result).column = std::move(result_column);
    }
};

using FunctionTumble = FunctionWindow<TUMBLE>;
using FunctionTumbleStart = FunctionWindow<TUMBLE_START>;
using FunctionTumbleEnd = FunctionWindow<TUMBLE_END>;
using FunctionHop = FunctionWindow<HOP>;
using FunctionWindowId = FunctionWindow<WINDOW_ID>;
using FunctionHopStart = FunctionWindow<HOP_START>;
using FunctionHopEnd = FunctionWindow<HOP_END>;
}
