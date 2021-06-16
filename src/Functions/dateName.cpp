#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>

#include <Core/DecimalFunctions.h>
#include <common/DateLUTImpl.h>

#include <type_traits>

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

template <typename DataType> struct ActionValueTypeMap {};
template <> struct ActionValueTypeMap<DataTypeDate> { using ActionValueType = UInt16; };
template <> struct ActionValueTypeMap<DataTypeDateTime>      { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeDateTime64>    { using ActionValueType = Int64; };

class FunctionDateNameImpl : public IFunction
{
public:
    static constexpr auto name = "dateName";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateNameImpl>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

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
        if (!WhichDataType(arguments[1].type).isDateOrDateTime())
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

        if (!((res = executeType<DataTypeDate>(arguments, result_type)) || (res = executeType<DataTypeDateTime>(arguments, result_type))
              || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of function {], must be Date or DateTime.",
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

        const ColumnConst * datepart_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!datepart_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first ('datepart') argument of function {}. Must be constant string.",
                arguments[0].column->getName(),
                getName());

        using T = typename ActionValueTypeMap<DataType>::ActionValueType;
        auto datepart_writer = DatePartWriter<T>();
        String datepart = datepart_column->getValue<String>();

        if (!datepart_writer.isCorrectDatePart(datepart))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Illegal value {} of first ('format') argument of function {}. Check documentation",
                datepart,
                getName());

        const DateLUTImpl * time_zone_tmp;
        if (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeDateTime>)
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 1);
        else
            time_zone_tmp = &DateLUT::instance();

        const auto & vec = times->getData();
        const DateLUTImpl & time_zone = *time_zone_tmp;

        UInt32 scale [[maybe_unused]] = 0;
        if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
        {
            scale = vec.getScale();
        }

        auto col_res = ColumnString::create();
        auto & dst_data = col_res->getChars();
        auto & dst_offsets = col_res->getOffsets();
        dst_data.resize(vec.size() * (9 /* longest possible word 'Wednesday' */ + 1 /* zero terminator */));
        dst_offsets.resize(vec.size());

        auto * begin = reinterpret_cast<char *>(dst_data.data());
        auto * pos = begin;

        for (size_t i = 0; i < vec.size(); ++i)
        {
            if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
            {
                // since right now LUT does not support Int64-values and not format instructions for subsecond parts,
                // treat DatTime64 values just as DateTime values by ignoring fractional and casting to UInt32.
                const auto c = DecimalUtils::split(vec[i], scale);
                datepart_writer.writeDatePart(pos, datepart, static_cast<UInt32>(c.whole), time_zone);
            }
            else
            {
                datepart_writer.writeDatePart(pos, datepart, vec[i], time_zone);
            }
            dst_offsets[i] = pos - begin;
            ++pos;
        }
        dst_data.resize(pos - begin);
        return col_res;
    }

private:
    template <typename Time>
    class DatePartWriter
    {
    public:
        void writeDatePart(char *& target, const String & datepart, Time source, const DateLUTImpl & timezone)
        {
            datepart_functions.at(datepart)(target, source, timezone);
        }

        bool isCorrectDatePart(const String & datepart) { return datepart_functions.find(datepart) != datepart_functions.end(); }

    private:
        const std::unordered_map<String, void (*)(char *&, Time, const DateLUTImpl &)> datepart_functions = {
            {"year", writeYear},
            {"quarter", writeQuarter},
            {"month", writeMonth},
            {"dayofyear", writeDayOfYear},
            {"day", writeDay},
            {"week", writeWeek},
            {"weekday", writeWeekday},
            {"hour", writeHour},
            {"minute", writeMinute},
            {"second", writeSecond},
        };

        static inline void writeYear(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToYearImpl::execute(source, timezone));
        }

        static inline void writeQuarter(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToQuarterImpl::execute(source, timezone));
        }

        static inline void writeMonth(char *& target, Time source, const DateLUTImpl & timezone)
        {
            const auto month = ToMonthImpl::execute(source, timezone);
            static constexpr std::string_view monthnames[]
                = {"January",
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
                   "December"};
            writeString(target, monthnames[month - 1]);
        }

        static inline void writeDayOfYear(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToDayOfYearImpl::execute(source, timezone));
        }

        static inline void writeDay(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToDayOfMonthImpl::execute(source, timezone));
        }

        static inline void writeWeek(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToISOWeekImpl::execute(source, timezone));
        }

        static inline void writeWeekday(char *& target, Time source, const DateLUTImpl & timezone)
        {
            const auto day = ToDayOfWeekImpl::execute(source, timezone);
            static constexpr std::string_view daynames[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
            writeString(target, daynames[day - 1]);
        }

        static inline void writeHour(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToHourImpl::execute(source, timezone));
        }

        static inline void writeMinute(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToMinuteImpl::execute(source, timezone));
        }

        static inline void writeSecond(char *& target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber(target, ToSecondImpl::execute(source, timezone));
        }

        static inline void writeString(char *& target, const std::string_view & value)
        {
            size_t size = value.size() + 1; /// With zero terminator
            memcpy(target, value.data(), size);
            target += size;
        }

        template <typename T>
        static inline void writeNumber(char *& target, T value)
        {
            if (value < 10)
            {
                *target = value + '0';
                target += 2;
                *target = '\0';
            }
            else if (value < 100)
            {
                writeNumber2(target, value);
                target += 3;
                *target = '\0';
            }
            else if (value < 1000)
            {
                writeNumber3(target, value);
                target += 4;
                *target = '\0';
            }
            else if (value < 10000)
            {
                writeNumber4(target, value);
                target += 5;
                *target = '\0';
            }
            else
            {
                throw Exception(
                    "Illegal value of second ('datetime') argument of function dateName. Check documentation.",
                    ErrorCodes::BAD_ARGUMENTS);
            }
        }

        template <typename T>
        static inline void writeNumber2(char * p, T v)
        {
            memcpy(p, &digits100[v * 2], 2);
        }

        template <typename T>
        static inline void writeNumber3(char * p, T v)
        {
            writeNumber2(p, v / 10);
            p[2] = v % 10 + '0';
        }

        template <typename T>
        static inline void writeNumber4(char * p, T v)
        {
            writeNumber2(p, v / 100);
            writeNumber2(p + 2, v % 100);
        }
    };
};
}

void registerFunctionDateName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDateNameImpl>(FunctionFactory::CaseInsensitive);
}
}
