#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnString.h>

#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>

#include <common/DateLUTImpl.h>
#include <common/find_symbols.h>
#include <Core/DecimalFunctions.h>

#include <type_traits>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename DataType> struct ActionValueTypeMap {};
template <> struct ActionValueTypeMap<DataTypeInt8>       { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeUInt8>      { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeInt16>      { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeUInt16>     { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeInt32>      { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeUInt32>     { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeInt64>      { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeUInt64>     { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeDate>       { using ActionValueType = UInt16; };
template <> struct ActionValueTypeMap<DataTypeDateTime>   { using ActionValueType = UInt32; };
// TODO(vnemkov): to add sub-second format instruction, make that DateTime64 and do some math in Action<T>.
template <> struct ActionValueTypeMap<DataTypeDateTime64> { using ActionValueType = Int64; };


/** formatDateTime(time, 'pattern')
  * Performs formatting of time, according to provided pattern.
  *
  * This function is optimized with an assumption, that the resulting strings are fixed width.
  * (This assumption is fulfilled for currently supported formatting options).
  *
  * It is implemented in two steps.
  * At first step, it creates a pattern of zeros, literal characters, whitespaces, etc.
  *  and quickly fills resulting character array (string column) with this pattern.
  * At second step, it walks across the resulting character array and modifies/replaces specific characters,
  *  by calling some functions by pointers and shifting cursor by specified amount.
  *
  * Advantages:
  * - memcpy is mostly unrolled;
  * - low number of arithmetic ops due to pre-filled pattern;
  * - for somewhat reason, function by pointer call is faster than switch/case.
  *
  * Possible further optimization options:
  * - slightly interleave first and second step for better cache locality
  *   (but it has no sense when character array fits in L1d cache);
  * - avoid indirect function calls and inline functions with JIT compilation.
  *
  * Performance on Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz:
  *
  * WITH formatDateTime(now() + number, '%H:%M:%S') AS x SELECT count() FROM system.numbers WHERE NOT ignore(x);
  * - 97 million rows per second per core;
  *
  * WITH formatDateTime(toDateTime('2018-01-01 00:00:00') + number, '%F %T') AS x SELECT count() FROM system.numbers WHERE NOT ignore(x)
  * - 71 million rows per second per core;
  *
  * select count() from (select formatDateTime(t, '%m/%d/%Y %H:%M:%S') from (select toDateTime('2018-01-01 00:00:00')+number as t from numbers(100000000)));
  * - 53 million rows per second per core;
  *
  * select count() from (select formatDateTime(t, 'Hello %Y World') from (select toDateTime('2018-01-01 00:00:00')+number as t from numbers(100000000)));
  * - 138 million rows per second per core;
  *
  * PS. We can make this function to return FixedString. Currently it returns String.
  */
template <typename Name, bool support_integer>
class FunctionFormatDateTimeImpl : public IFunction
{
private:
    /// Time is either UInt32 for DateTime or UInt16 for Date.
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeUInt8,
            DataTypeInt16,
            DataTypeUInt16,
            DataTypeInt32,
            DataTypeUInt32,
            DataTypeInt64,
            DataTypeUInt64>(type, std::forward<F>(f));
    }

    template <typename Time>
    class Action
    {
    public:
        using Func = void (*)(char *, Time, const DateLUTImpl &);

        Func func;
        size_t shift;

        explicit Action(Func func_, size_t shift_ = 0) : func(func_), shift(shift_) {}

        void perform(char *& target, Time source, const DateLUTImpl & timezone)
        {
            func(target, source, timezone);
            target += shift;
        }

    private:
        template <typename T>
        static inline void writeNumber2(char * p, T v)
        {
            memcpy(p, &digits100[v * 2], 2);
        }

        template <typename T>
        static inline void writeNumber3(char * p, T v)
        {
            writeNumber2(p, v / 10);
            p[2] += v % 10;
        }

        template <typename T>
        static inline void writeNumber4(char * p, T v)
        {
            writeNumber2(p, v / 100);
            writeNumber2(p + 2, v % 100);
        }

    public:
        static void noop(char *, Time, const DateLUTImpl &)
        {
        }

        static void century(char * target, Time source, const DateLUTImpl & timezone)
        {
            auto year = ToYearImpl::execute(source, timezone);
            auto century = year / 100;
            writeNumber2(target, century);
        }

        static void dayOfMonth(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToDayOfMonthImpl::execute(source, timezone));
        }

        static void americanDate(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToMonthImpl::execute(source, timezone));
            writeNumber2(target + 3, ToDayOfMonthImpl::execute(source, timezone));
            writeNumber2(target + 6, ToYearImpl::execute(source, timezone) % 100);
        }

        static void dayOfMonthSpacePadded(char * target, Time source, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfMonthImpl::execute(source, timezone);
            if (day < 10)
                target[1] += day;
            else
                writeNumber2(target, day);
        }

        static void ISO8601Date(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber4(target, ToYearImpl::execute(source, timezone));
            writeNumber2(target + 5, ToMonthImpl::execute(source, timezone));
            writeNumber2(target + 8, ToDayOfMonthImpl::execute(source, timezone));
        }

        static void dayOfYear(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber3(target, ToDayOfYearImpl::execute(source, timezone));
        }

        static void month(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToMonthImpl::execute(source, timezone));
        }

        static void dayOfWeek(char * target, Time source, const DateLUTImpl & timezone)
        {
            *target += ToDayOfWeekImpl::execute(source, timezone);
        }

        static void dayOfWeek0To6(char * target, Time source, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfWeekImpl::execute(source, timezone);
            *target += (day == 7 ? 0 : day);
        }

        static void ISO8601Week(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber2(target, ToISOWeekImpl::execute(source, timezone));
        }

        static void ISO8601Year2(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber2(target, ToISOYearImpl::execute(source, timezone) % 100);
        }

        static void ISO8601Year4(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber4(target, ToISOYearImpl::execute(source, timezone));
        }

        static void year2(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToYearImpl::execute(source, timezone) % 100);
        }

        static void year4(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber4(target, ToYearImpl::execute(source, timezone));
        }

        static void hour24(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToHourImpl::execute(source, timezone));
        }

        static void hour12(char * target, Time source, const DateLUTImpl & timezone)
        {
            auto x = ToHourImpl::execute(source, timezone);
            writeNumber2(target, x == 0 ? 12 : (x > 12 ? x - 12 : x));
        }

        static void minute(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToMinuteImpl::execute(source, timezone));
        }

        static void AMPM(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            auto hour = ToHourImpl::execute(source, timezone);
            if (hour >= 12)
                *target = 'P';
        }

        static void hhmm24(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToHourImpl::execute(source, timezone));
            writeNumber2(target + 3, ToMinuteImpl::execute(source, timezone));
        }

        static void second(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToSecondImpl::execute(source, timezone));
        }

        static void ISO8601Time(char * target, Time source, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber2(target, ToHourImpl::execute(source, timezone));
            writeNumber2(target + 3, ToMinuteImpl::execute(source, timezone));
            writeNumber2(target + 6, ToSecondImpl::execute(source, timezone));
        }

        static void quarter(char * target, Time source, const DateLUTImpl & timezone)
        {
            *target += ToQuarterImpl::execute(source, timezone);
        }
    };

public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatDateTimeImpl>(); }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (support_integer)
        {
            if (arguments.size() != 1 && arguments.size() != 2 && arguments.size() != 3)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 1, 2 or 3",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            if (arguments.size() == 1 && !isInteger(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                        + " when arguments size is 1. Should be integer",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (arguments.size() > 1 && !(isInteger(arguments[0].type) || isDate(arguments[0].type) || isDateTime(arguments[0].type) || isDateTime64(arguments[0].type)))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                        + " when arguments size is 2 or 3. Should be a integer or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
        {
            if (arguments.size() != 2 && arguments.size() != 3)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 2 or 3",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            if (!isDate(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (arguments.size() == 2 && !WhichDataType(arguments[1].type).isString())
            throw Exception(
                "Illegal type " + arguments[1].type->getName() + " of 2 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
            throw Exception(
                "Illegal type " + arguments[2].type->getName() + " of 3 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 1)
            return std::make_shared<DataTypeDateTime>();
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, [[maybe_unused]] size_t input_rows_count) const override
    {
        ColumnPtr res;
        if constexpr (support_integer)
        {
            if (arguments.size() == 1)
            {
                if (!castType(arguments[0].type.get(), [&](const auto & type)
                    {
                        using FromDataType = std::decay_t<decltype(type)>;
                        res = ConvertImpl<FromDataType, DataTypeDateTime, Name>::execute(arguments, result_type, input_rows_count);
                        return true;
                    }))
                {
                    throw Exception(
                        "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                            + ", must be Integer or DateTime when arguments size is 1.",
                        ErrorCodes::ILLEGAL_COLUMN);
                }
            }
            else
            {
                if (!castType(arguments[0].type.get(), [&](const auto & type)
                    {
                        using FromDataType = std::decay_t<decltype(type)>;
                        if (!(res = executeType<FromDataType>(arguments, result_type)))
                            throw Exception(
                                "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                                    + ", must be Integer or DateTime.",
                                ErrorCodes::ILLEGAL_COLUMN);
                        return true;
                    }))
                {
                    if (!((res = executeType<DataTypeDate>(arguments, result_type))
                        || (res = executeType<DataTypeDateTime>(arguments, result_type))
                        || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
                        throw Exception(
                            "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                                + ", must be Integer or DateTime.",
                            ErrorCodes::ILLEGAL_COLUMN);
                }
            }
        }
        else
        {
            if (!((res = executeType<DataTypeDate>(arguments, result_type))
                || (res = executeType<DataTypeDateTime>(arguments, result_type))
                || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
                throw Exception(
                    "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                        + ", must be Date or DateTime.",
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return res;
    }

    template <typename DataType>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
    {
        auto * times = checkAndGetColumn<typename DataType::ColumnType>(arguments[0].column.get());
        if (!times)
            return nullptr;

        const ColumnConst * pattern_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!pattern_column)
            throw Exception("Illegal column " + arguments[1].column->getName()
                            + " of second ('format') argument of function " + getName()
                            + ". Must be constant string.",
                            ErrorCodes::ILLEGAL_COLUMN);

        String pattern = pattern_column->getValue<String>();

        using T = typename ActionValueTypeMap<DataType>::ActionValueType;
        std::vector<Action<T>> instructions;
        String pattern_to_fill = parsePattern(pattern, instructions);
        size_t result_size = pattern_to_fill.size();

        const DateLUTImpl * time_zone_tmp = nullptr;
        if (castType(arguments[0].type.get(), [&]([[maybe_unused]] const auto & type) { return true; }))
        {
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        }
        else if (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeDateTime>)
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        else
            time_zone_tmp = &DateLUT::instance();

        const DateLUTImpl & time_zone = *time_zone_tmp;
        const auto & vec = times->getData();

        UInt32 scale [[maybe_unused]] = 0;
        if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
        {
            scale = vec.getScale();
        }

        auto col_res = ColumnString::create();
        auto & dst_data = col_res->getChars();
        auto & dst_offsets = col_res->getOffsets();
        dst_data.resize(vec.size() * (result_size + 1));
        dst_offsets.resize(vec.size());

        /// Fill result with literals.
        {
            UInt8 * begin = dst_data.data();
            UInt8 * end = begin + dst_data.size();
            UInt8 * pos = begin;

            if (pos < end)
            {
                memcpy(pos, pattern_to_fill.data(), result_size + 1);   /// With zero terminator.
                pos += result_size + 1;
            }

            /// Fill by copying exponential growing ranges.
            while (pos < end)
            {
                size_t bytes_to_copy = std::min(pos - begin, end - pos);
                memcpy(pos, begin, bytes_to_copy);
                pos += bytes_to_copy;
            }
        }

        auto * begin = reinterpret_cast<char *>(dst_data.data());
        auto * pos = begin;

        for (size_t i = 0; i < vec.size(); ++i)
        {
            if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
            {
                for (auto & instruction : instructions)
                {
                    const auto c = DecimalUtils::split(vec[i], scale);
                    instruction.perform(pos, static_cast<Int64>(c.whole), time_zone);
                }
            }
            else
            {
                for (auto & instruction : instructions)
                    instruction.perform(pos, vec[i], time_zone);
            }

            dst_offsets[i] = pos - begin;
        }

        dst_data.resize(pos - begin);
        return col_res;
    }

    template <typename T>
    String parsePattern(const String & pattern, std::vector<Action<T>> & instructions) const
    {
        String result;

        const char * pos = pattern.data();
        const char * end = pos + pattern.size();

        /// Add shift to previous action; or if there were none, add noop action with shift.
        auto add_shift = [&](size_t amount)
        {
            if (instructions.empty())
                instructions.emplace_back(&Action<T>::noop);
            instructions.back().shift += amount;
        };

        /// If the argument was DateTime, add instruction for printing. If it was date, just shift (the buffer is pre-filled with default values).
        auto add_instruction_or_shift = [&](typename Action<T>::Func func [[maybe_unused]], size_t shift)
        {
            if constexpr (std::is_same_v<T, UInt32>)
                instructions.emplace_back(func, shift);
            else if constexpr (std::is_same_v<T, Int64>)
                instructions.emplace_back(func, shift);
            else
                add_shift(shift);
        };

        while (true)
        {
            const char * percent_pos = find_first_symbols<'%'>(pos, end);

            if (percent_pos < end)
            {
                if (pos < percent_pos)
                {
                    result.append(pos, percent_pos);
                    add_shift(percent_pos - pos);
                }

                pos = percent_pos + 1;

                if (pos >= end)
                    throw Exception("Sign '%' is the last in pattern, if you need it, use '%%'", ErrorCodes::BAD_ARGUMENTS);

                switch (*pos)
                {
                    // Year, divided by 100, zero-padded
                    case 'C':
                        instructions.emplace_back(&Action<T>::century, 2);
                        result.append("00");
                        break;

                    // Day of month, zero-padded (01-31)
                    case 'd':
                        instructions.emplace_back(&Action<T>::dayOfMonth, 2);
                        result.append("00");
                        break;

                    // Short MM/DD/YY date, equivalent to %m/%d/%y
                    case 'D':
                        instructions.emplace_back(&Action<T>::americanDate, 8);
                        result.append("00/00/00");
                        break;

                    // Day of month, space-padded ( 1-31)  23
                    case 'e':
                        instructions.emplace_back(&Action<T>::dayOfMonthSpacePadded, 2);
                        result.append(" 0");
                        break;

                    // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                    case 'F':
                        instructions.emplace_back(&Action<T>::ISO8601Date, 10);
                        result.append("0000-00-00");
                        break;

                    // Last two digits of year of ISO 8601 week number (see %G)
                    case 'g':
                      instructions.emplace_back(&Action<T>::ISO8601Year2, 2);
                      result.append("00");
                      break;

                    // Year of ISO 8601 week number (see %V)
                    case 'G':
                      instructions.emplace_back(&Action<T>::ISO8601Year4, 4);
                      result.append("0000");
                      break;

                    // Day of the year (001-366)   235
                    case 'j':
                        instructions.emplace_back(&Action<T>::dayOfYear, 3);
                        result.append("000");
                        break;

                    // Month as a decimal number (01-12)
                    case 'm':
                        instructions.emplace_back(&Action<T>::month, 2);
                        result.append("00");
                        break;

                    // ISO 8601 weekday as number with Monday as 1 (1-7)
                    case 'u':
                        instructions.emplace_back(&Action<T>::dayOfWeek, 1);
                        result.append("0");
                        break;

                    // ISO 8601 week number (01-53)
                    case 'V':
                        instructions.emplace_back(&Action<T>::ISO8601Week, 2);
                        result.append("00");
                        break;

                    // Weekday as a decimal number with Sunday as 0 (0-6)  4
                    case 'w':
                        instructions.emplace_back(&Action<T>::dayOfWeek0To6, 1);
                        result.append("0");
                        break;

                    // Two digits year
                    case 'y':
                        instructions.emplace_back(&Action<T>::year2, 2);
                        result.append("00");
                        break;

                    // Four digits year
                    case 'Y':
                        instructions.emplace_back(&Action<T>::year4, 4);
                        result.append("0000");
                        break;

                    // Quarter (1-4)
                    case 'Q':
                        instructions.template emplace_back(&Action<T>::quarter, 1);
                        result.append("0");
                        break;

                    /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                    // Minute (00-59)
                    case 'M':
                        add_instruction_or_shift(&Action<T>::minute, 2);
                        result.append("00");
                        break;

                    // AM or PM
                    case 'p':
                        add_instruction_or_shift(&Action<T>::AMPM, 2);
                        result.append("AM");
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%M 14:55
                    case 'R':
                        add_instruction_or_shift(&Action<T>::hhmm24, 5);
                        result.append("00:00");
                        break;

                    // Seconds
                    case 'S':
                        add_instruction_or_shift(&Action<T>::second, 2);
                        result.append("00");
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S 14:55:02
                    case 'T':
                        add_instruction_or_shift(&Action<T>::ISO8601Time, 8);
                        result.append("00:00:00");
                        break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        add_instruction_or_shift(&Action<T>::hour24, 2);
                        result.append("00");
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        add_instruction_or_shift(&Action<T>::hour12, 2);
                        result.append("12");
                        break;

                    /// Escaped literal characters.
                    case '%':
                        result += '%';
                        add_shift(1);
                        break;
                    case 't':
                        result += '\t';
                        add_shift(1);
                        break;
                    case 'n':
                        result += '\n';
                        add_shift(1);
                        break;

                    // Unimplemented
                    case 'U': [[fallthrough]];
                    case 'W':
                        throw Exception("Wrong pattern '" + pattern + "', symbol '" + *pos + " is not implemented ' for function " + getName(),
                            ErrorCodes::NOT_IMPLEMENTED);

                    default:
                        throw Exception(
                            "Wrong pattern '" + pattern + "', unexpected symbol '" + *pos + "' for function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
                }

                ++pos;
            }
            else
            {
                result.append(pos, end);
                add_shift(end + 1 - pos); /// including zero terminator
                break;
            }
        }

        return result;
    }
};

struct NameFormatDateTime
{
    static constexpr auto name = "formatDateTime";
};

struct NameFromUnixTime
{
    static constexpr auto name = "FROM_UNIXTIME";
};

using FunctionFormatDateTime = FunctionFormatDateTimeImpl<NameFormatDateTime, false>;
using FunctionFROM_UNIXTIME = FunctionFormatDateTimeImpl<NameFromUnixTime, true>;

}

void registerFunctionFormatDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatDateTime>();
    factory.registerFunction<FunctionFROM_UNIXTIME>();
    factory.registerAlias("fromUnixTimestamp", "FROM_UNIXTIME");
}

}
