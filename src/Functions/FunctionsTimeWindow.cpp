#include <numeric>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
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
                "Must be a Tuple(DataTime, DataTime)", function_name);
        return col_tuple->getColumnPtr(index);
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal column for first argument of function {}. "
        "Must be Tuple",
        function_name);
}

void checkFirstArgument(const ColumnWithTypeAndName & argument, const String & function_name)
{
    if (!isDateTime(argument.type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
            "Should be a date with time", argument.type->getName(), function_name);
}

void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name, IntervalKind & interval_kind, bool & result_type_is_date)
{
    const auto * interval_type = checkAndGetDataType<DataTypeInterval>(argument.type.get());
    if (!interval_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
            "Should be an interval of time", argument.type->getName(), function_name);
    interval_kind = interval_type->getKind();
    result_type_is_date = (interval_type->getKind() == IntervalKind::Kind::Year) || (interval_type->getKind() == IntervalKind::Kind::Quarter)
        || (interval_type->getKind() == IntervalKind::Kind::Month) || (interval_type->getKind() == IntervalKind::Kind::Week);
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
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
            "This argument is optional and must be a constant string with timezone name",
            argument.type->getName(), function_name);
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

enum TimeWindowFunctionName
{
    TUMBLE,
    TUMBLE_START,
    TUMBLE_END,
    HOP,
    HOP_START,
    HOP_END,
    WINDOW_ID
};

template <TimeWindowFunctionName type>
struct TimeWindowImpl
{
    static constexpr auto name = "UNKNOWN";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name);

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count);
};

template <TimeWindowFunctionName type>
class FunctionTimeWindow : public IFunction
{
public:
    static constexpr auto name = TimeWindowImpl<type>::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeWindow>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override;
};

using FunctionTumble = FunctionTimeWindow<TUMBLE>;
using FunctionTumbleStart = FunctionTimeWindow<TUMBLE_START>;
using FunctionTumbleEnd = FunctionTimeWindow<TUMBLE_END>;
using FunctionHop = FunctionTimeWindow<HOP>;
using FunctionWindowId = FunctionTimeWindow<WINDOW_ID>;
using FunctionHopStart = FunctionTimeWindow<HOP_START>;
using FunctionHopEnd = FunctionTimeWindow<HOP_END>;


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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                function_name, arguments.size());
        }

        DataTypePtr data_type = nullptr;
        if (result_type_is_date)
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
            {
                const auto & tuple_elems = std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElements();
                if (tuple_elems.empty())
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Tuple passed to {} should not be empty", function_name);
                return tuple_elems[0];
            }
            if (type.isUInt32())
                return std::make_shared<DataTypeDateTime>();
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type of first argument of function {} should be DateTime, Tuple or UInt32",
                function_name);
        }

        return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<TUMBLE>::getReturnType(arguments, function_name))
            ->getElement(0);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            //isTuple
            result_column = time_column.column;
        }
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
        return TimeWindowImpl<TUMBLE_START>::getReturnType(arguments, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String& function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            //isTuple
            result_column = time_column.column;
        }
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
                function_name, arguments.size());
        }

        if (interval_kind_1 != interval_kind_2)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal type of window and hop column of function {}, must be same",
                function_name);

        DataTypePtr data_type = nullptr;
        if (result_type_is_date)
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
            ToType wstart = ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone);
            ToType wend = AddTime<kind>::execute(wstart, hop_num_units, time_zone);
            wstart = AddTime<kind>::execute(wend, -window_num_units, time_zone);
            ToType wend_latest;

            if (wstart > wend)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time overflow in function {}", name);

            do
            {
                wend_latest = wend;
                wend = AddTime<kind>::execute(wend, -hop_num_units, time_zone);
            } while (wend > time_data[i]);

            end_data[i] = wend_latest;
            start_data[i] = AddTime<kind>::execute(wend_latest, -window_num_units, time_zone);
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

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
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
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal type of window and hop column of function {}, must be same",
                        function_name);
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2, 3 or 4",
                function_name, arguments.size());
        }

        if (result_type_is_date)
            return std::make_shared<DataTypeUInt16>();
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr dispatchForHopColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
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
                return executeHopSlice<UInt32, IntervalKind::Kind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Minute:
                return executeHopSlice<UInt32, IntervalKind::Kind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Hour:
                return executeHopSlice<UInt32, IntervalKind::Kind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Day:
                return executeHopSlice<UInt32, IntervalKind::Kind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Week:
                return executeHopSlice<UInt16, IntervalKind::Kind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Month:
                return executeHopSlice<UInt16, IntervalKind::Kind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Quarter:
                return executeHopSlice<UInt16, IntervalKind::Kind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            case IntervalKind::Kind::Year:
                return executeHopSlice<UInt16, IntervalKind::Kind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone, input_rows_count);
            default:
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Fraction seconds are unsupported by windows yet");
        }
        UNREACHABLE();
    }

    template <typename ToType, IntervalKind::Kind kind>
    static ColumnPtr
    executeHopSlice(const ColumnDateTime & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone, size_t input_rows_count)
    {
        Int64 gcd_num_units = std::gcd(hop_num_units, window_num_units);

        const auto & time_data = time_column.getData();

        auto end = ColumnVector<ToType>::create();
        auto & end_data = end->getData();
        end_data.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ToType wstart = ToStartOfTransform<kind>::execute(time_data[i], hop_num_units, time_zone);
            ToType wend = AddTime<kind>::execute(wstart, hop_num_units, time_zone);
            ToType wend_latest;

            if (wstart > wend)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time overflow in function {}", name);

            do
            {
                wend_latest = wend;
                wend = AddTime<kind>::execute(wend, -gcd_num_units, time_zone);
            } while (wend > time_data[i]);

            end_data[i] = wend_latest;
        }
        return end;
    }

    static ColumnPtr dispatchForTumbleColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        ColumnPtr column = TimeWindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name, input_rows_count);
        return executeWindowBound(column, 1, function_name);
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        if (arguments.size() == 2)
            return dispatchForTumbleColumns(arguments, function_name, input_rows_count);

        const auto & third_column = arguments[2];
        if (arguments.size() == 3 && WhichDataType(third_column.type).isString())
            return dispatchForTumbleColumns(arguments, function_name, input_rows_count);
        return dispatchForHopColumns(arguments, function_name, input_rows_count);
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
            {
                const auto & tuple_elems = std::static_pointer_cast<const DataTypeTuple>(arguments[0].type)->getElements();
                if (tuple_elems.empty())
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Tuple passed to {} should not be empty", function_name);
                return tuple_elems[0];
            }
            if (type.isUInt32())
                return std::make_shared<DataTypeDateTime>();
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type of first argument of function {} should be DateTime, Tuple or UInt32",
                function_name);
        }

        return std::static_pointer_cast<const DataTypeTuple>(TimeWindowImpl<HOP>::getReturnType(arguments, function_name))->getElement(0);
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            //isTuple
            result_column = time_column.column;
        }
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
        return TimeWindowImpl<HOP_START>::getReturnType(arguments, function_name);
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name, size_t input_rows_count)
    {
        const auto & time_column = arguments[0];
        const auto which_type = WhichDataType(time_column.type);
        ColumnPtr result_column;
        if (arguments.size() == 1)
        {
            if (which_type.isUInt32())
                return time_column.column;
            //isTuple
            result_column = time_column.column;
        }
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
    factory.registerFunction<FunctionTumble>();
    factory.registerFunction<FunctionHop>();
    factory.registerFunction<FunctionTumbleStart>();
    factory.registerFunction<FunctionTumbleEnd>();
    factory.registerFunction<FunctionHopStart>();
    factory.registerFunction<FunctionHopEnd>();
    factory.registerFunction<FunctionWindowId>();
}
}
