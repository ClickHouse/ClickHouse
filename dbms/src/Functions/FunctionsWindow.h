#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
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
    HOP_END
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
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(t), delta); \
        } \
    };
    TRANSFORM_DATE(Year)
    TRANSFORM_DATE(Quarter)
    TRANSFORM_DATE(Month)
    TRANSFORM_DATE(Week)
    TRANSFORM_DATE(Day)
#undef TRANSFORM_DATE

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
        static UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone) { return time_zone.add##INTERVAL_KIND##s(t, delta); } \
    };
    ADD_DATE(Year)
    ADD_DATE(Quarter)
    ADD_DATE(Month)
    ADD_DATE(Week)
    ADD_DATE(Day)
#undef ADD_DATE

#define ADD_TIME(INTERVAL_KIND, INTERVAL) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return t + INTERVAL * delta; } \
    };
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
                "Value for column " + interval_column.name + " of function " + function_name + " must be positive.",
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
        else if (const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(column.get()); col_array)
        {
            const ColumnTuple * col_tuple_ = checkAndGetColumn<ColumnTuple>(&col_array->getData());
            if (!col_tuple_)
                throw Exception(
                    "Illegal column for first argument of function " + function_name + ". Must be a Tuple or Array(Tuple)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto & bound_column = col_tuple_->getColumn(index);
            const ColumnUInt32::Container & bound_data = static_cast<const ColumnUInt32 &>(bound_column).getData();
            const auto & column_offsets = col_array->getOffsets();
            auto res = ColumnUInt32::create();
            ColumnUInt32::Container & res_data = res->getData();
            res_data.reserve(column_offsets.size());
            if (index == 0) // lower bound of hop window
            {
                IColumn::Offset current_offset = 0;
                for (size_t i = 0; i < column_offsets.size(); ++i)
                {
                    res_data.push_back(bound_data[current_offset]);
                    current_offset = column_offsets[i];
                }
            }
            else // upper bound of hop window
            {
                for (size_t i = 0; i < column_offsets.size(); ++i)
                    res_data.push_back(bound_data[column_offsets[i] - 1]);
            }
            return res;
        }
        else
        {
            throw Exception(
                "Illegal column for first argument of function " + function_name + ". Must be a Tuple or Array(Tuple)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
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
            if (arguments.size() != 2 && arguments.size() != 3)
            {
                throw Exception(
                    "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 2.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            if (!WhichDataType(arguments[0].type).isDateTime())
                throw Exception(
                    "Illegal type of first argument of function " + function_name + " should be DateTime", ErrorCodes::ILLEGAL_COLUMN);
            if (!WhichDataType(arguments[1].type).isInterval())
                throw Exception(
                    "Illegal type of second argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
            if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Illegal type " + arguments[2].type->getName() + " of argument of function " + function_name
                        + ". This argument is optional and must be a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()});
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
                    return execute_tumble<IntervalKind::Second>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Minute:
                    return execute_tumble<IntervalKind::Minute>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Hour:
                    return execute_tumble<IntervalKind::Hour>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Day:
                    return execute_tumble<IntervalKind::Day>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Week:
                    return execute_tumble<IntervalKind::Week>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Month:
                    return execute_tumble<IntervalKind::Month>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Quarter:
                    return execute_tumble<IntervalKind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone);
                case IntervalKind::Year:
                    return execute_tumble<IntervalKind::Year>(*time_column_vec, std::get<1>(interval), time_zone);
            }
            __builtin_unreachable();
        }

        template <IntervalKind::Kind unit>
        static ColumnPtr execute_tumble(const ColumnUInt32 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
        {
            const auto & time_data = time_column.getData();
            size_t size = time_column.size();
            auto start = ColumnUInt32::create(size);
            auto end = ColumnUInt32::create(size);
            ColumnUInt32::Container & start_data = start->getData();
            ColumnUInt32::Container & end_data = end->getData();
            for (size_t i = 0; i != size; ++i)
            {
                UInt32 wid = static_cast<UInt32>(ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone));
                start_data[i] = wid;
                end_data[i] = AddTime<unit>::execute(wid, num_units, time_zone);
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
                if (!WhichDataType(arguments[0].type).isTuple())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be tuple", ErrorCodes::ILLEGAL_COLUMN);
                return std::make_shared<DataTypeDateTime>();
            }
            else if (arguments.size() == 2 || arguments.size() == 3)
            {
                if (!WhichDataType(arguments[0].type).isDateTime())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be DateTime", ErrorCodes::ILLEGAL_COLUMN);
                if (!WhichDataType(arguments[1].type).isInterval())
                    throw Exception(
                        "Illegal type of second argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
                if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
                    throw Exception(
                        "Illegal type " + arguments[2].type->getName() + " of argument of function " + function_name
                            + ". This argument is optional and must be a constant string with timezone name",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                return std::make_shared<DataTypeDateTime>();
            }
            else
            {
                throw Exception(
                    "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                        + ", should not larger than 2.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
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
            if (arguments.size() != 3 && arguments.size() != 4)
            {
                throw Exception(
                    "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 3.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            if (!WhichDataType(arguments[0].type).isDateTime())
                throw Exception(
                    "Illegal type of first argument of function " + function_name + " should be DateTime", ErrorCodes::ILLEGAL_COLUMN);
            if (!WhichDataType(arguments[1].type).isInterval())
                throw Exception(
                    "Illegal type of second argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
            if (!WhichDataType(arguments[2].type).isInterval())
                throw Exception(
                    "Illegal type of third argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
            if (arguments.size() == 4 && !WhichDataType(arguments[3].type).isString())
                throw Exception(
                    "Illegal type " + arguments[3].type->getName() + " of argument of function " + function_name
                        + ". This argument is optional and must be a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()}));
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

            if (std::get<0>(hop_interval) != std::get<0>(window_interval))
                throw Exception(
                    "Interval type of window and hop column of function " + function_name + ", must be same.", ErrorCodes::ILLEGAL_COLUMN);
            if (std::get<1>(hop_interval) > std::get<1>(window_interval))
                throw Exception(
                    "Value for hop interval of function " + function_name + " must not larger than window interval.",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            switch (std::get<0>(window_interval))
            {
                case IntervalKind::Second:
                    return execute_hop<IntervalKind::Second>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Minute:
                    return execute_hop<IntervalKind::Minute>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Hour:
                    return execute_hop<IntervalKind::Hour>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Day:
                    return execute_hop<IntervalKind::Day>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Week:
                    return execute_hop<IntervalKind::Week>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Month:
                    return execute_hop<IntervalKind::Month>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Quarter:
                    return execute_hop<IntervalKind::Quarter>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
                case IntervalKind::Year:
                    return execute_hop<IntervalKind::Year>(
                        *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            }
            __builtin_unreachable();
        }

        template <IntervalKind::Kind kind>
        static ColumnPtr
        execute_hop(const ColumnUInt32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
        {
            const auto & time_data = time_column.getData();
            size_t size = time_column.size();
            int max_wid_nums = window_num_units / hop_num_units + (window_num_units % hop_num_units != 0);

            auto column_offsets = ColumnArray::ColumnOffsets::create(size);
            IColumn::Offsets & out_offsets = column_offsets->getData();

            auto start = ColumnUInt32::create();
            auto end = ColumnUInt32::create();
            ColumnUInt32::Container & start_data = start->getData();
            ColumnUInt32::Container & end_data = end->getData();
            start_data.reserve(max_wid_nums * size);
            end_data.reserve(max_wid_nums * size);
            out_offsets.reserve(size);
            IColumn::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                UInt32 wstart = static_cast<UInt32>(ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone));
                UInt32 wend = AddTime<kind>::execute(wstart, hop_num_units, time_zone);
                wstart = AddTime<kind>::execute(wend, -1 * window_num_units, time_zone);

                UInt32 wend_ = wend;
                UInt32 wend_latest;

                do
                {
                    wend_latest = wend_;
                    wend_ = AddTime<kind>::execute(wend_, -1 * hop_num_units, time_zone);
                } while (wend_ > time_data[i]);

                UInt32 wstart_ = AddTime<kind>::execute(wend_latest, -1 * window_num_units, time_zone);
                wend_ = wend_latest;

                while (wstart_ <= time_data[i])
                {
                    start_data.push_back(wstart_);
                    end_data.push_back(wend_);
                    wstart_ = AddTime<kind>::execute(wstart_, hop_num_units, time_zone);
                    wend_ = AddTime<kind>::execute(wstart_, window_num_units, time_zone);
                    ++current_offset;
                }
                out_offsets[i] = current_offset;
            }
            MutableColumns tuple_columns;
            tuple_columns.emplace_back(std::move(start));
            tuple_columns.emplace_back(std::move(end));
            return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(column_offsets));
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
                if (!type_.isTuple() && !type_.isArray())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be tuple or array",
                        ErrorCodes::ILLEGAL_COLUMN);
                return std::make_shared<DataTypeDateTime>();
            }
            else if (arguments.size() == 3 || arguments.size() == 4)
            {
                if (!WhichDataType(arguments[0].type).isDateTime())
                    throw Exception(
                        "Illegal type of first argument of function " + function_name + " should be DateTime", ErrorCodes::ILLEGAL_COLUMN);
                if (!WhichDataType(arguments[1].type).isInterval())
                    throw Exception(
                        "Illegal type of second argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
                if (!WhichDataType(arguments[2].type).isInterval())
                    throw Exception(
                        "Illegal type of third argument of function " + function_name + " should be Interval", ErrorCodes::ILLEGAL_COLUMN);
                if (arguments.size() == 4 && !WhichDataType(arguments[3].type).isString())
                    throw Exception(
                        "Illegal type " + arguments[3].type->getName() + " of argument of function " + function_name
                            + ". This argument is optional and must be a constant string with timezone name",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                return std::make_shared<DataTypeDateTime>();
            }
            else
            {
                throw Exception(
                    "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 1 or 3.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
        }

        [[maybe_unused]] static ColumnPtr
        dispatchForColumns(Block & block, const ColumnNumbers & arguments, const String & function_name)
        {
            const auto & time_column = block.getByPosition(arguments[0]);
            const auto which_type = WhichDataType(time_column.type);
            ColumnPtr result_column_;
            if (which_type.isDateTime())
                result_column_ = WindowImpl<HOP>::dispatchForColumns(block, arguments, function_name);
            else
                result_column_ = block.getByPosition(arguments[0]).column;
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
            if (which_type.isDateTime())
                result_column_ = WindowImpl<HOP>::dispatchForColumns(block, arguments, function_name);
            else
                result_column_ = block.getByPosition(arguments[0]).column;
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
using FunctionHopStart = FunctionWindow<HOP_START>;
using FunctionHopEnd = FunctionWindow<HOP_END>;
}
