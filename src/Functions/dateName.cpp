#include <Common/DateLUTImpl.h>

#include <Core/DecimalFunctions.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>

#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename DataType> struct DataTypeToTimeTypeMap {};

template <> struct DataTypeToTimeTypeMap<DataTypeDate>
{
    using TimeType = UInt16;
};

template <> struct DataTypeToTimeTypeMap<DataTypeDateTime>
{
    using TimeType = UInt32;
};

template <> struct DataTypeToTimeTypeMap<DataTypeDateTime64>
{
    using TimeType = Int64;
};

template <typename DataType>
using DateTypeToTimeType = typename DataTypeToTimeTypeMap<DataType>::TimeType;

class FunctionDateNameImpl : public IFunction
{
public:
    static constexpr auto name = "dateName";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateNameImpl>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        if (!WhichDataType(arguments[0].type).isString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1 argument of function {}. Must be string",
                arguments[0].type->getName(),
                getName());

        WhichDataType first_argument_type(arguments[1].type);

        if (!(first_argument_type.isDate() || first_argument_type.isDateTime() || first_argument_type.isDateTime64()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2 argument of function {}. Must be a date or a date with time",
                arguments[1].type->getName(),
                getName());

        if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 3 argument of function {}. Must be string",
                arguments[2].type->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        [[maybe_unused]] size_t input_rows_count) const override
    {
        ColumnPtr res;

        if (!((res = executeType<DataTypeDate>(arguments, result_type))
            || (res = executeType<DataTypeDateTime>(arguments, result_type))
            || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of function {}, must be Date or DateTime.",
                arguments[1].column->getName(),
                getName());

        return res;
    }

    template <typename DataType>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
    {
        auto * times = checkAndGetColumn<typename DataType::ColumnType>(arguments[1].column.get());
        if (!times)
            return nullptr;

        const ColumnConst * date_part_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!date_part_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first ('datepart') argument of function {}. Must be constant string.",
                arguments[0].column->getName(),
                getName());

        String date_part = date_part_column->getValue<String>();

        const DateLUTImpl * time_zone_tmp;
        if (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeDateTime>)
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 1);
        else
            time_zone_tmp = &DateLUT::instance();

        const auto & times_data = times->getData();
        const DateLUTImpl & time_zone = *time_zone_tmp;

        UInt32 scale [[maybe_unused]] = 0;
        if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
        {
            scale = times->getScale();
        }

        auto result_column = ColumnString::create();
        auto & result_column_data = result_column->getChars();
        auto & result_column_offsets = result_column->getOffsets();

        /* longest possible word 'Wednesday' with zero terminator */
        static constexpr size_t longest_word_length = 9 + 1;

        result_column_data.resize_fill(times_data.size() * longest_word_length);
        result_column_offsets.resize(times_data.size());

        auto * begin = reinterpret_cast<char *>(result_column_data.data());

        WriteBufferWithoutFinalize buffer(begin, result_column_data.size());

        using TimeType = DateTypeToTimeType<DataType>;
        callOnDatePartWriter<TimeType>(date_part, [&](const auto & writer)
        {
            for (size_t i = 0; i < times_data.size(); ++i)
            {
                if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
                {
                    const auto components = DecimalUtils::split(times_data[i], scale);
                    writer.write(buffer, static_cast<Int64>(components.whole), time_zone);
                }
                else
                {
                    writer.write(buffer, times_data[i], time_zone);
                }

                /// Null terminator
                ++buffer.position();
                result_column_offsets[i] = buffer.position() - begin;
            }
        });

        result_column_data.resize(buffer.position() - begin);

        return result_column;
    }

private:

    template <typename Time>
    struct YearWriter
    {
        static void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToYearImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct QuarterWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToQuarterImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct MonthWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            const auto month = ToMonthImpl::execute(source, timezone);
            static constexpr std::string_view month_names[] =
            {
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December"
            };

            writeText(month_names[month - 1], buffer);
        }
    };

    template <typename Time>
    struct WeekWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToISOWeekImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct DayOfYearWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToDayOfYearImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct DayWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToDayOfMonthImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct WeekDayWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            const auto day = ToDayOfWeekImpl::execute(source, timezone);
            static constexpr std::string_view day_names[] =
            {
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday"
            };

            writeText(day_names[day - 1], buffer);
        }
    };

    template <typename Time>
    struct HourWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToHourImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct MinuteWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToMinuteImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time>
    struct SecondWriter
    {
        static inline void write(WriteBuffer & buffer, Time source, const DateLUTImpl & timezone)
        {
            writeText(ToSecondImpl::execute(source, timezone), buffer);
        }
    };

    template <typename Time, typename Call>
    void callOnDatePartWriter(const String & date_part, Call && call) const
    {
        if (date_part == "year")
            std::forward<Call>(call)(YearWriter<Time>());
        else if (date_part == "quarter")
            std::forward<Call>(call)(QuarterWriter<Time>());
        else if (date_part == "month")
            std::forward<Call>(call)(MonthWriter<Time>());
        else if (date_part == "week")
            std::forward<Call>(call)(WeekWriter<Time>());
        else if (date_part == "dayofyear")
            std::forward<Call>(call)(DayOfYearWriter<Time>());
        else if (date_part == "day")
            std::forward<Call>(call)(DayWriter<Time>());
        else if (date_part == "weekday")
            std::forward<Call>(call)(WeekDayWriter<Time>());
        else if (date_part == "hour")
            std::forward<Call>(call)(HourWriter<Time>());
        else if (date_part == "minute")
            std::forward<Call>(call)(MinuteWriter<Time>());
        else if (date_part == "second")
            std::forward<Call>(call)(SecondWriter<Time>());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid date part {} for function {}", date_part, getName());
    }

};

}

void registerFunctionDateName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDateNameImpl>(FunctionFactory::CaseInsensitive);
}

}
