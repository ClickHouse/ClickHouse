#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnString.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
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
// in private namespace to avoid GCC 9 error: "explicit specialization in non-namespace scope"
template <typename DataType> struct ActionaValueTypeMap {};
template <> struct ActionaValueTypeMap<DataTypeDate>       { using ActionValueType = UInt16; };
template <> struct ActionaValueTypeMap<DataTypeDateTime>   { using ActionValueType = UInt32; };
// TODO(vnemkov): once there is support for Int64 in LUT, make that Int64.
// TODO(vnemkov): to add sub-second format instruction, make that DateTime64 and do some math in Action<T>.
template <> struct ActionaValueTypeMap<DataTypeDateTime64> { using ActionValueType = UInt32; };
}

/** formatDateTime(time, 'pattern')
  * Performs formatting of time, according to provided pattern.
  *
  * This function is optimized with an assumption, that the resulting strings are fixed width.
  * (This assumption is fulfilled for currently supported formatting options).
  *
  * It is implemented in two steps.
  * At first step, it creates a pattern of zeros, literal characters, whitespaces, etc.
  *  and quickly fills resulting character array (string column) with this pattern.
  * At second step, it walks across the resulting character array and modifies/replaces specific charaters,
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
class FunctionFormatDateTime : public IFunction
{
private:
    /// Time is either UInt32 for DateTime or UInt16 for Date.
    template <typename Time>
    class Action
    {
    public:
        using Func = void (*)(char *, Time, const DateLUTImpl &);

        Func func;
        size_t shift;

        Action(Func func_, size_t shift_ = 0) : func(func_), shift(shift_) {}

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

        static void ISO8601Date(char * target, Time source, const DateLUTImpl & timezone)
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

        static void ISO8601Week(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToISOWeekImpl::execute(source, timezone));
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

        static void AMPM(char * target, Time source, const DateLUTImpl & timezone)
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

        static void ISO8601Time(char * target, Time source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToHourImpl::execute(source, timezone));
            writeNumber2(target + 3, ToMinuteImpl::execute(source, timezone));
            writeNumber2(target + 6, ToSecondImpl::execute(source, timezone));
        }
    };

public:
    static constexpr auto name = "formatDateTime";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFormatDateTime>(); }

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
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(arguments.size()) + ", should be 2 or 3",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!WhichDataType(arguments[0].type).isDateOrDateTime())
            throw Exception("Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName() +
                            ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1].type).isString())
            throw Exception("Illegal type " + arguments[1].type->getName() + " of 2 argument of function " + getName() + ". Must be String.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3)
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception("Illegal type " + arguments[2].type->getName() + " of 3 argument of function " + getName() + ". Must be String.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (!executeType<DataTypeDate>(block, arguments, result)
            && !executeType<DataTypeDateTime>(block, arguments, result)
            && !executeType<DataTypeDateTime64>(block, arguments, result))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + " of function " + getName() + ", must be Date or DateTime",
                            ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename DataType>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        auto * times = checkAndGetColumn<typename DataType::ColumnType>(block.getByPosition(arguments[0]).column.get());
        if (!times)
            return false;

        const ColumnConst * pattern_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get());
        if (!pattern_column)
            throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                            + " of second ('format') argument of function " + getName()
                            + ". Must be constant string.",
                            ErrorCodes::ILLEGAL_COLUMN);

        String pattern = pattern_column->getValue<String>();

        using T = typename ActionaValueTypeMap<DataType>::ActionValueType;
        std::vector<Action<T>> instructions;
        String pattern_to_fill = parsePattern(pattern, instructions);
        size_t result_size = pattern_to_fill.size();

        const DateLUTImpl * time_zone_tmp = nullptr;
        if (arguments.size() == 3)
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);
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

        auto begin = reinterpret_cast<char *>(dst_data.data());
        auto pos = begin;

        for (size_t i = 0; i < vec.size(); ++i)
        {
            if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
            {
                for (auto & instruction : instructions)
                {
                    // since right now LUT does not support Int64-values and not format instructions for subsecond parts,
                    // treat DatTime64 values just as DateTime values by ignoring fractional and casting to UInt32.
                    const auto c = DecimalUtils::split(vec[i], scale);
                    instruction.perform(pos, static_cast<UInt32>(c.whole), time_zone);
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
        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    template <typename T>
    String parsePattern(const String & pattern, std::vector<Action<T>> & instructions) const
    {
        String result;

        const char * pos = pattern.data();
        const char * end = pos + pattern.size();

        /// Add shift to previous action; or if there were none, add noop action with shift.
        auto addShift = [&](size_t amount)
        {
            if (instructions.empty())
                instructions.emplace_back(&Action<T>::noop);
            instructions.back().shift += amount;
        };

        /// If the argument was DateTime, add instruction for printing. If it was date, just shift (the buffer is pre-filled with default values).
        auto addInstructionOrShift = [&](typename Action<T>::Func func [[maybe_unused]], size_t shift)
        {
            if constexpr (std::is_same_v<T, UInt32>)
                instructions.emplace_back(func, shift);
            else
                addShift(shift);
        };

        while (true)
        {
            const char * percent_pos = find_first_symbols<'%'>(pos, end);

            if (percent_pos < end)
            {
                if (pos < percent_pos)
                {
                    result.append(pos, percent_pos);
                    addShift(percent_pos - pos);
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

                    /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                    // Minute (00-59)
                    case 'M':
                        addInstructionOrShift(&Action<T>::minute, 2);
                        result.append("00");
                        break;

                    // AM or PM
                    case 'p':
                        addInstructionOrShift(&Action<T>::AMPM, 2);
                        result.append("AM");
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%M 14:55
                    case 'R':
                        addInstructionOrShift(&Action<T>::hhmm24, 5);
                        result.append("00:00");
                        break;

                    // Seconds
                    case 'S':
                        addInstructionOrShift(&Action<T>::second, 2);
                        result.append("00");
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S 14:55:02
                    case 'T':
                        addInstructionOrShift(&Action<T>::ISO8601Time, 8);
                        result.append("00:00:00");
                        break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        addInstructionOrShift(&Action<T>::hour24, 2);
                        result.append("00");
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        addInstructionOrShift(&Action<T>::hour12, 2);
                        result.append("12");
                        break;

                    /// Escaped literal characters.
                    case '%':
                        result += '%';
                        addShift(1);
                        break;
                    case 't':
                        result += '\t';
                        addShift(1);
                        break;
                    case 'n':
                        result += '\n';
                        addShift(1);
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
                addShift(end + 1 - pos); /// including zero terminator
                break;
            }
        }

        return result;
    }
};

void registerFunctionFormatDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatDateTime>();
}

}
