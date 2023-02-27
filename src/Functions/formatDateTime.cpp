#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/NumberTraits.h>
#include <Columns/ColumnString.h>

#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>

#include <Common/DateLUTImpl.h>
#include <base/find_symbols.h>
#include <Core/DecimalFunctions.h>

#include <type_traits>
#include <concepts>


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

struct FormatDateTimeTraits
{
    enum class SupportInteger
    {
        Yes,
        No
    };

    enum class FormatSyntax
    {
        MySQL,
        Joda
    };
};


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
template <> struct ActionValueTypeMap<DataTypeDate32>     { using ActionValueType = Int32; };
template <> struct ActionValueTypeMap<DataTypeDateTime>   { using ActionValueType = UInt32; };
template <> struct ActionValueTypeMap<DataTypeDateTime64> { using ActionValueType = Int64; };

/// Counts the number of literal characters in Joda format string until the next closing literal
/// sequence single quote. Returns -1 if no literal single quote was found.
/// In Joda format string(https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)
/// literal content must be quoted with single quote. and two single quote means literal with one single quote.
/// For example:
/// Format string: "'aaaa'", unescaped literal: "aaaa";
/// Format string: "'aa''aa'", unescaped literal: "aa'aa";
/// Format string: "'aaa''aa" is not valid because of missing of end single quote.
Int64 numLiteralChars(const char * cur, const char * end)
{
    bool found = false;
    Int64 count = 0;
    while (cur < end)
    {
        if (*cur == '\'')
        {
            if (cur + 1 < end && *(cur + 1) == '\'')
            {
                count += 2;
                cur += 2;
            }
            else
            {
                found = true;
                break;
            }
        }
        else
        {
            ++count;
            ++cur;
        }
    }
    return found ? count : -1;
}

/// Cast value from integer to string, making sure digits number in result string is no less than total_digits by padding leading '0'.
String padValue(UInt32 val, size_t min_digits)
{
    String str = std::to_string(val);
    auto length = str.size();
    if (length >= min_digits)
        return str;

    String paddings(min_digits - length, '0');
    return str.insert(0, paddings);
}

constexpr std::string_view weekdaysFull[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

constexpr std::string_view weekdaysShort[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

constexpr std::string_view monthsFull[]
    = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

constexpr std::string_view monthsShort[]
    = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

/** formatDateTime(time, 'format')
  * Performs formatting of time, according to provided format.
  *
  * This function is optimized with an assumption, that the resulting strings are fixed width.
  * (This assumption is fulfilled for currently supported formatting options).
  *
  * It is implemented in two steps.
  * At first step, it creates a template of zeros, literal characters, whitespaces, etc.
  *  and quickly fills resulting character array (string column) with this format.
  * At second step, it walks across the resulting character array and modifies/replaces specific characters,
  *  by calling some functions by pointers and shifting cursor by specified amount.
  *
  * Advantages:
  * - memcpy is mostly unrolled;
  * - low number of arithmetic ops due to pre-filled template;
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
template <typename Name, FormatDateTimeTraits::SupportInteger support_integer, FormatDateTimeTraits::FormatSyntax format_syntax>
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
        /// Using std::function will cause performance degradation in MySQL format by 0.45x.
        /// But std::function is required for Joda format to capture extra variables.
        /// This is the reason why we use raw function pointer in MySQL format and std::function
        /// in Joda format.
        using Func = std::conditional_t<
            format_syntax == FormatDateTimeTraits::FormatSyntax::MySQL,
            size_t (*)(char *, Time, UInt64, UInt32, const DateLUTImpl &),
            std::function<size_t(char *, Time, UInt64, UInt32, const DateLUTImpl &)>>;

        Func func;

        /// extra_shift is only used in MySQL format syntax. It is always 0 in Joda format syntax.
        size_t extra_shift = 0;

        /// Action for appending date/time related number in specified format.
        explicit Action(Func && func_) : func(std::move(func_)) {}

        void perform(char *& dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            auto shift = func(dest, source, fractional_second, scale, timezone);
            dest += shift + extra_shift;
        }

    private:
        template <typename T>
        static size_t writeNumber2(char * p, T v)
        {
            memcpy(p, &digits100[v * 2], 2);
            return 2;
        }

        template <typename T>
        static size_t writeNumber3(char * p, T v)
        {
            writeNumber2(p, v / 10);
            p[2] = '0' + v % 10;
            return 3;
        }

        template <typename T>
        static size_t writeNumber4(char * p, T v)
        {
            writeNumber2(p, v / 100);
            writeNumber2(p + 2, v % 100);
            return 4;
        }

        /// Cast content from integer to string, and append result string to buffer.
        /// Make sure digits number in result string is no less than total_digits by padding leading '0'
        /// Notice: '-' is not counted as digit.
        /// For example:
        /// val = -123, total_digits = 2 => dest = "-123"
        /// val = -123, total_digits = 3 => dest = "-123"
        /// val = -123, total_digits = 4 => dest = "-0123"
        static size_t writeNumberWithPadding(char * dest, std::integral auto val, size_t min_digits)
        {
            using T = decltype(val);
            using WeightType = typename NumberTraits::Construct<is_signed_v<T>, /*is_floating*/ false, sizeof(T)>::Type;
            WeightType w = 1;
            WeightType n = val;
            size_t digits = 0;
            while (n)
            {
                w *= 10;
                n /= 10;
                ++digits;
            }

            /// Possible sign
            size_t pos = 0;
            n = val;
            if constexpr (is_signed_v<T>)
                if (val < 0)
                {
                    n = (~n) + 1;
                    dest[pos] = '-';
                    ++pos;
                }

            /// Possible leading paddings
            if (min_digits > digits)
            {
                memset(dest, '0', min_digits - digits);
                pos += min_digits - digits;
            }

            /// Digits
            while (w >= 100)
            {
                w /= 100;

                writeNumber2(dest + pos, n / w);
                pos += 2;

                n = n % w;
            }
            if (n)
            {
                dest[pos] = '0' + n;
                ++pos;
            }

            return pos;
        }
    public:
        static size_t mysqlNoop(char *, Time, UInt64, UInt32, const DateLUTImpl &) { return 0; }

        static size_t mysqlCentury(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto year = ToYearImpl::execute(source, timezone);
            auto century = year / 100;
            return writeNumber2(dest, century);
        }

        static size_t mysqlDayOfMonth(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToDayOfMonthImpl::execute(source, timezone));
        }

        static size_t mysqlAmericanDate(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            writeNumber2(dest, ToMonthImpl::execute(source, timezone));
            writeNumber2(dest + 3, ToDayOfMonthImpl::execute(source, timezone));
            writeNumber2(dest + 6, ToYearImpl::execute(source, timezone) % 100);
            return 8;
        }

        static size_t mysqlDayOfMonthSpacePadded(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfMonthImpl::execute(source, timezone);
            if (day < 10)
                dest[1] = '0' + day;
            else
                writeNumber2(dest, day);
            return 2;
        }

        static size_t mysqlISO8601Date(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber4(dest, ToYearImpl::execute(source, timezone));
            writeNumber2(dest + 5, ToMonthImpl::execute(source, timezone));
            writeNumber2(dest + 8, ToDayOfMonthImpl::execute(source, timezone));
            return 10;
        }

        static size_t mysqlDayOfYear(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber3(dest, ToDayOfYearImpl::execute(source, timezone));
        }

        static size_t mysqlMonth(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToMonthImpl::execute(source, timezone));
        }

        static size_t monthOfYearText(char * dest, Time source, bool abbreviate, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto month = ToMonthImpl::execute(source, timezone);
            std::string_view str_view = abbreviate ? monthsShort[month - 1] : monthsFull[month - 1];
            memcpy(dest, str_view.data(), str_view.size());
            return str_view.size();
        }

        static size_t mysqlMonthOfYearTextShort(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            return monthOfYearText(dest, source, true, fractional_second, scale, timezone);
        }

        static size_t mysqlMonthOfYearTextLong(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            return monthOfYearText(dest, source, false, fractional_second, scale, timezone);
        }

        static size_t mysqlDayOfWeek(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            *dest = '0' + ToDayOfWeekImpl::execute(source, 0, timezone);
            return 1;
        }

        static size_t dayOfWeekText(char * dest, Time source, bool abbreviate, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto week_day = ToDayOfWeekImpl::execute(source, 0, timezone);
            if (week_day == 7)
                week_day = 0;

            std::string_view str_view = abbreviate ? weekdaysShort[week_day] : weekdaysFull[week_day];
            memcpy(dest, str_view.data(), str_view.size());
            return str_view.size();
        }

        static size_t mysqlDayOfWeekTextShort(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            return dayOfWeekText(dest, source, true, fractional_second, scale, timezone);
        }

        static size_t mysqlDayOfWeekTextLong(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            return dayOfWeekText(dest, source, false, fractional_second, scale, timezone);
        }

        static size_t mysqlDayOfWeek0To6(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfWeekImpl::execute(source, 0, timezone);
            *dest = '0' + (day == 7 ? 0 : day);
            return 1;
        }

        static size_t mysqlISO8601Week(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            return writeNumber2(dest, ToISOWeekImpl::execute(source, timezone));
        }

        static size_t mysqlISO8601Year2(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            return writeNumber2(dest, ToISOYearImpl::execute(source, timezone) % 100);
        }

        static size_t mysqlISO8601Year4(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            return writeNumber4(dest, ToISOYearImpl::execute(source, timezone));
        }

        static size_t mysqlYear2(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToYearImpl::execute(source, timezone) % 100);
        }

        static size_t mysqlYear4(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber4(dest, ToYearImpl::execute(source, timezone));
        }

        static size_t mysqlHour24(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToHourImpl::execute(source, timezone));
        }

        static size_t mysqlHour12(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto x = ToHourImpl::execute(source, timezone);
            return writeNumber2(dest, x == 0 ? 12 : (x > 12 ? x - 12 : x));
        }

        static size_t mysqlMinute(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToMinuteImpl::execute(source, timezone));
        }

        static size_t mysqlAMPM(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            auto hour = ToHourImpl::execute(source, timezone);
            dest[0] = hour >= 12 ? 'P' : 'A';
            dest[1] = 'M';
            return 2;
        }

        static size_t mysqlHHMM24(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            writeNumber2(dest, ToHourImpl::execute(source, timezone));
            writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
            return 5;
        }

        static size_t mysqlHHMM12(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone);
            writeNumber2(dest, hour == 0 ? 12 : (hour > 12 ? hour - 12 : hour));
            writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));

            dest[6] = hour >= 12 ? 'P' : 'A';
            return 8;
        }

        static size_t mysqlSecond(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            return writeNumber2(dest, ToSecondImpl::execute(source, timezone));
        }

        static size_t
        mysqlFractionalSecond(char * dest, Time /*source*/, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & /*timezone*/)
        {
            if (scale == 0)
                scale = 1;

            for (Int64 i = scale, value = fractional_second; i > 0; --i)
            {
                dest[i - 1] += value % 10;
                value /= 10;
            }
            return scale;
        }

        static size_t mysqlISO8601Time(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
        {
            writeNumber2(dest, ToHourImpl::execute(source, timezone));
            writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
            writeNumber2(dest + 6, ToSecondImpl::execute(source, timezone));
            return 8;
        }

        static size_t mysqlTimezoneOffset(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto offset = TimezoneOffsetImpl::execute(source, timezone);
            if (offset < 0)
            {
                *dest = '-';
                offset = -offset;
            }

            writeNumber2(dest + 1, offset / 3600);
            writeNumber2(dest + 3, offset % 3600 / 60);
            return 5;
        }

        static size_t mysqlQuarter(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            *dest = '0' + ToQuarterImpl::execute(source, timezone);
            return 1;
        }

        template <typename Literal>
        static size_t jodaLiteral(const Literal & literal, char * dest, Time, UInt64, UInt32, const DateLUTImpl &)
        {
            memcpy(dest, literal.data(), literal.size());
            return literal.size();
        }

        static size_t jodaEra(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto year = static_cast<Int32>(ToYearImpl::execute(source, timezone));
            String res;
            if (min_represent_digits <= 3)
                res = static_cast<Int32>(year) > 0 ? "AD" : "BC";
            else
                res = static_cast<Int32>(year) > 0 ? "Anno Domini" : "Before Christ";

            memcpy(dest, res.data(), res.size());
            return res.size();
        }

        static size_t jodaCenturyOfEra(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto year = static_cast<Int32>(ToYearImpl::execute(source, timezone));
            year = (year < 0 ? -year : year);
            return writeNumberWithPadding(dest, year / 100, min_represent_digits);
        }

        static size_t jodaYearOfEra(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto year = static_cast<Int32>(ToYearImpl::execute(source, timezone));
            if (min_represent_digits == 2)
                return writeNumberWithPadding(dest, std::abs(year) % 100, 2);
            else
            {
                year = year <= 0 ? std::abs(year - 1) : year;
                return writeNumberWithPadding(dest, year, min_represent_digits);
            }
        }

        static size_t jodaDayOfWeek1Based(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto week_day = ToDayOfWeekImpl::execute(source, 0, timezone);
            return writeNumberWithPadding(dest, week_day, min_represent_digits);
        }

        static size_t jodaDayOfWeekText(size_t min_represent_digits, char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            bool abbreviate = min_represent_digits <= 3;
            return dayOfWeekText(dest, source, abbreviate, fractional_second, scale, timezone);
        }

        static size_t jodaYear(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto year = static_cast<Int32>(ToYearImpl::execute(source, timezone));
            if (min_represent_digits == 2)
            {
                year = std::abs(year);
                auto two_digit_year = year % 100;
                return writeNumberWithPadding(dest, two_digit_year, 2);
            }
            else
                return writeNumberWithPadding(dest, year, min_represent_digits);
        }

        static size_t jodaWeekYear(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto week_year = ToWeekYearImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, week_year, min_represent_digits);
        }

        static size_t jodaWeekOfWeekYear(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto week_of_weekyear = ToWeekOfWeekYearImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, week_of_weekyear, min_represent_digits);
        }

        static size_t jodaDayOfYear(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto day_of_year = ToDayOfYearImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, day_of_year, min_represent_digits);
        }

        static size_t jodaMonthOfYear(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto month_of_year = ToMonthImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, month_of_year, min_represent_digits);
        }

        static size_t jodaMonthOfYearText(size_t min_represent_digits, char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            bool abbreviate = min_represent_digits <= 3;
            return monthOfYearText(dest, source, abbreviate, fractional_second, scale, timezone);
        }

        static size_t jodaDayOfMonth(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto day_of_month = ToDayOfMonthImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, day_of_month, min_represent_digits);
        }

        static size_t jodaHalfDayOfDay(
            size_t /*min_represent_digits*/, char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
        {
            return mysqlAMPM(dest, source, fractional_second, scale, timezone);
        }

        static size_t jodaHourOfHalfDay(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone) % 12;
            return writeNumberWithPadding(dest, hour, min_represent_digits);
        }

        static size_t jodaClockHourOfHalfDay(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone) ;
            hour = (hour + 11) % 12 + 1;
            return writeNumberWithPadding(dest, hour, min_represent_digits);
        }

        static size_t jodaHourOfDay(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone) ;
            return writeNumberWithPadding(dest, hour, min_represent_digits);
        }

        static size_t jodaClockHourOfDay(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone);
            hour = (hour + 23) % 24 + 1;
            return writeNumberWithPadding(dest, hour, min_represent_digits);
        }

        static size_t jodaMinuteOfHour(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto minute_of_hour = ToMinuteImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, minute_of_hour, min_represent_digits);
        }

        static size_t jodaSecondOfMinute(size_t min_represent_digits, char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            auto second_of_minute = ToSecondImpl::execute(source, timezone);
            return writeNumberWithPadding(dest, second_of_minute, min_represent_digits);
        }

        static size_t jodaFractionOfSecond(size_t min_represent_digits, char * dest, Time /*source*/, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & /*timezone*/)
        {
            if (min_represent_digits > 9)
                min_represent_digits = 9;
            if (fractional_second == 0)
            {
                for (UInt64 i = 0; i < min_represent_digits; ++i)
                    dest[i] = '0';
                return min_represent_digits;
            }
            auto str = toString(fractional_second);
            if (min_represent_digits > scale)
            {
                for (UInt64 i = 0; i < min_represent_digits - scale; ++i)
                    str += '0';
            }
            else if (min_represent_digits < scale)
            {
                str = str.substr(0, min_represent_digits);
            }
            memcpy(dest, str.data(), str.size());
            return min_represent_digits;
        }

        static size_t jodaTimezone(size_t min_represent_digits, char * dest, Time /*source*/, UInt64, UInt32, const DateLUTImpl & timezone)
        {
            if (min_represent_digits <= 3)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Short name time zone is not yet supported");

            auto str = timezone.getTimeZone();
            memcpy(dest, str.data(), str.size());
            return str.size();
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (support_integer == FormatDateTimeTraits::SupportInteger::Yes)
        {
            if (arguments.size() != 1 && arguments.size() != 2 && arguments.size() != 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3",
                    getName(), arguments.size());
            if (arguments.size() == 1 && !isInteger(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {} when arguments size is 1. Should be integer",
                    arguments[0].type->getName(), getName());
            if (arguments.size() > 1 && !(isInteger(arguments[0].type) || isDate(arguments[0].type) || isDateTime(arguments[0].type) || isDate32(arguments[0].type) || isDateTime64(arguments[0].type)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of first argument of function {} when arguments size is 2 or 3. "
                                "Should be a integer or a date with time",
                                arguments[0].type->getName(), getName());
        }
        else
        {
            if (arguments.size() != 2 && arguments.size() != 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                    getName(), arguments.size());
            if (!isDate(arguments[0].type) && !isDateTime(arguments[0].type) && !isDate32(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}. Should be a date or a date with time",
                    arguments[0].type->getName(), getName());
        }

        if (arguments.size() == 2 && !WhichDataType(arguments[1].type).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}. Must be String.",
                arguments[1].type->getName(), getName());

        if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}. Must be String.",
                arguments[2].type->getName(), getName());

        if (arguments.size() == 1)
            return std::make_shared<DataTypeDateTime>();
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, [[maybe_unused]] size_t input_rows_count) const override
    {
        ColumnPtr res;
        if constexpr (support_integer == FormatDateTimeTraits::SupportInteger::Yes)
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
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                    "Illegal column {} of function {}, must be Integer, Date, Date32, DateTime "
                                    "or DateTime64 when arguments size is 1.",
                                    arguments[0].column->getName(), getName());
                }
            }
            else
            {
                if (!castType(arguments[0].type.get(), [&](const auto & type)
                    {
                        using FromDataType = std::decay_t<decltype(type)>;
                        if (!(res = executeType<FromDataType>(arguments, result_type)))
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Illegal column {} of function {}, must be Integer, Date, Date32, DateTime or DateTime64.",
                                arguments[0].column->getName(), getName());
                        return true;
                    }))
                {
                    if (!((res = executeType<DataTypeDate>(arguments, result_type))
                        || (res = executeType<DataTypeDate32>(arguments, result_type))
                        || (res = executeType<DataTypeDateTime>(arguments, result_type))
                        || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
                        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of function {}, must be Integer or DateTime.",
                            arguments[0].column->getName(), getName());
                }
            }
        }
        else
        {
            if (!((res = executeType<DataTypeDate>(arguments, result_type))
                || (res = executeType<DataTypeDate32>(arguments, result_type))
                || (res = executeType<DataTypeDateTime>(arguments, result_type))
                || (res = executeType<DataTypeDateTime64>(arguments, result_type))))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of function {}, must be Date or DateTime.",
                    arguments[0].column->getName(), getName());
        }

        return res;
    }

    template <typename DataType>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
    {
        auto * times = checkAndGetColumn<typename DataType::ColumnType>(arguments[0].column.get());
        if (!times)
            return nullptr;

        const ColumnConst * format_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!format_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second ('format') argument of function {}. Must be constant string.",
                arguments[1].column->getName(), getName());

        String format = format_column->getValue<String>();

        UInt32 scale [[maybe_unused]] = 0;
        if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
            scale = times->getScale();

        using T = typename ActionValueTypeMap<DataType>::ActionValueType;
        std::vector<Action<T>> instructions;
        String out_template;
        auto result_size = parseFormat(format, instructions, scale, out_template);

        const DateLUTImpl * time_zone_tmp = nullptr;
        if (castType(arguments[0].type.get(), [&]([[maybe_unused]] const auto & type) { return true; }))
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        else if (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeDateTime>)
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        else
            time_zone_tmp = &DateLUT::instance();

        const DateLUTImpl & time_zone = *time_zone_tmp;
        const auto & vec = times->getData();

        auto col_res = ColumnString::create();
        auto & dst_data = col_res->getChars();
        auto & dst_offsets = col_res->getOffsets();
        dst_data.resize(vec.size() * (result_size + 1));
        dst_offsets.resize(vec.size());

        if constexpr (format_syntax == FormatDateTimeTraits::FormatSyntax::MySQL)
        {
            /// Fill result with literals.
            {
                UInt8 * begin = dst_data.data();
                UInt8 * end = begin + dst_data.size();
                UInt8 * pos = begin;

                if (pos < end)
                {
                    memcpy(pos, out_template.data(), result_size + 1); /// With zero terminator.
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
        }

        auto * begin = reinterpret_cast<char *>(dst_data.data());
        auto * pos = begin;
        for (size_t i = 0; i < vec.size(); ++i)
        {
            if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
            {
                const auto c = DecimalUtils::split(vec[i], scale);
                for (auto & instruction : instructions)
                {
                    instruction.perform(pos, static_cast<Int64>(c.whole), c.fractional, scale, time_zone);
                }
            }
            else
            {
                for (auto & instruction : instructions)
                    instruction.perform(pos, static_cast<UInt32>(vec[i]), 0, 0, time_zone);
            }
            *pos++ = '\0';

            dst_offsets[i] = pos - begin;
        }

        dst_data.resize(pos - begin);
        return col_res;
    }

    template <typename T>
    size_t parseFormat(const String & format, std::vector<Action<T>> & instructions, UInt32 scale, String & out_template) const
    {
        if constexpr (format_syntax == FormatDateTimeTraits::FormatSyntax::MySQL)
            return parseMySQLFormat(format, instructions, scale, out_template);
        else if constexpr (format_syntax == FormatDateTimeTraits::FormatSyntax::Joda)
            return parseJodaFormat(format, instructions, scale, out_template);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unknown datetime format style {} in function {}",
                magic_enum::enum_name(format_syntax),
                getName());
    }

    template <typename T>
    size_t parseMySQLFormat(const String & format, std::vector<Action<T>> & instructions, UInt32 scale, String & out_template) const
    {
        auto add_extra_shift = [&](size_t amount)
        {
            if (instructions.empty())
                instructions.emplace_back(&Action<T>::mysqlNoop);
            instructions.back().extra_shift += amount;
        };

        auto add_instruction_or_extra_shift = [&](auto * func [[maybe_unused]], size_t amount [[maybe_unused]])
        {
            if constexpr (std::is_same_v<T, UInt32> || std::is_same_v<T, Int64>)
                instructions.emplace_back(std::move(func));
            else
                add_extra_shift(amount);
        };

        const char * pos = format.data();
        const char * const end = pos + format.size();

        while (true)
        {
            const char * percent_pos = find_first_symbols<'%'>(pos, end);

            if (percent_pos < end)
            {
                if (pos < percent_pos)
                {
                    add_extra_shift(percent_pos - pos);
                    out_template += String(pos, percent_pos - pos);
                }

                pos = percent_pos + 1;
                if (pos >= end)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sign '%' is the last in format, if you need it, use '%%'");

                switch (*pos)
                {
                    // Abbreviated weekday [Mon...Sun]
                    case 'a':
                        instructions.emplace_back(&Action<T>::mysqlDayOfWeekTextShort);
                        out_template += "Mon";
                        break;

                    // Abbreviated month [Jan...Dec]
                    case 'b':
                        instructions.emplace_back(&Action<T>::mysqlMonthOfYearTextShort);
                        out_template += "Jan";
                        break;

                    // Month as a decimal number (01-12)
                    case 'c':
                        instructions.emplace_back(&Action<T>::mysqlMonth);
                        out_template += "00";
                        break;

                    // Year, divided by 100, zero-padded
                    case 'C':
                        instructions.emplace_back(&Action<T>::mysqlCentury);
                        out_template += "00";
                        break;

                    // Day of month, zero-padded (01-31)
                    case 'd':
                        instructions.emplace_back(&Action<T>::mysqlDayOfMonth);
                        out_template += "00";
                        break;

                    // Short MM/DD/YY date, equivalent to %m/%d/%y
                    case 'D':
                        instructions.emplace_back(&Action<T>::mysqlAmericanDate);
                        out_template += "00/00/00";
                        break;

                    // Day of month, space-padded ( 1-31)  23
                    case 'e':
                        instructions.emplace_back(&Action<T>::mysqlDayOfMonthSpacePadded);
                        out_template += " 0";
                        break;

                    // Fractional seconds
                    case 'f':
                    {
                        /// If the time data type has no fractional part, then we print '0' as the fractional part.
                        instructions.emplace_back(&Action<T>::mysqlFractionalSecond);
                        out_template += String(std::max<UInt32>(1, scale), '0');
                        break;
                    }

                    // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                    case 'F':
                        instructions.emplace_back(&Action<T>::mysqlISO8601Date);
                        out_template += "0000-00-00";
                        break;

                    // Last two digits of year of ISO 8601 week number (see %G)
                    case 'g':
                        instructions.emplace_back(&Action<T>::mysqlISO8601Year2);
                        out_template += "00";
                        break;

                    // Year of ISO 8601 week number (see %V)
                    case 'G':
                        instructions.emplace_back(&Action<T>::mysqlISO8601Year4);
                        out_template += "0000";
                        break;

                    // Day of the year (001-366)   235
                    case 'j':
                        instructions.emplace_back(&Action<T>::mysqlDayOfYear);
                        out_template += "000";
                        break;

                    // Month as a decimal number (01-12)
                    case 'm':
                        instructions.emplace_back(&Action<T>::mysqlMonth);
                        out_template += "00";
                        break;

                    // ISO 8601 weekday as number with Monday as 1 (1-7)
                    case 'u':
                        instructions.emplace_back(&Action<T>::mysqlDayOfWeek);
                        out_template += "0";
                        break;

                    // ISO 8601 week number (01-53)
                    case 'V':
                        instructions.emplace_back(&Action<T>::mysqlISO8601Week);
                        out_template += "00";
                        break;

                    // Weekday as a decimal number with Sunday as 0 (0-6)  4
                    case 'w':
                        instructions.emplace_back(&Action<T>::mysqlDayOfWeek0To6);
                        out_template += "0";
                        break;

                    // Full weekday [Monday...Sunday]
                    case 'W':
                        instructions.emplace_back(&Action<T>::mysqlDayOfWeekTextLong);
                        out_template += "Monday";
                        break;

                    // Two digits year
                    case 'y':
                        instructions.emplace_back(&Action<T>::mysqlYear2);
                        out_template += "00";
                        break;

                    // Four digits year
                    case 'Y':
                        instructions.emplace_back(&Action<T>::mysqlYear4);
                        out_template += "0000";
                        break;

                    // Quarter (1-4)
                    case 'Q':
                        instructions.template emplace_back(&Action<T>::mysqlQuarter);
                        out_template += "0";
                        break;

                    // Offset from UTC timezone as +hhmm or -hhmm
                    case 'z':
                        instructions.emplace_back(&Action<T>::mysqlTimezoneOffset);
                        out_template += "+0000";
                        break;

                    /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                    // Minute (00-59)
                    case 'M':
                        add_instruction_or_extra_shift(&Action<T>::mysqlMinute, 2);
                        out_template += "00";
                        break;

                    // AM or PM
                    case 'p':
                        add_instruction_or_extra_shift(&Action<T>::mysqlAMPM, 2);
                        out_template += "AM";
                        break;

                    // 12-hour HH:MM time, equivalent to %h:%i %p 2:55 PM
                    case 'r':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHHMM12, 8);
                        out_template += "12:00 AM";
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%i 14:55
                    case 'R':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHHMM24, 5);
                        out_template += "00:00";
                        break;

                    // Seconds
                    case 's':
                        add_instruction_or_extra_shift(&Action<T>::mysqlSecond, 2);
                        out_template += "00";
                        break;

                    // Seconds
                    case 'S':
                        add_instruction_or_extra_shift(&Action<T>::mysqlSecond, 2);
                        out_template += "00";
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S 14:55:02
                    case 'T':
                        add_instruction_or_extra_shift(&Action<T>::mysqlISO8601Time, 8);
                        out_template += "00:00:00";
                        break;

                    // Hour in 12h format (01-12)
                    case 'h':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHour12, 2);
                        out_template += "12";
                        break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHour24, 2);
                        out_template += "00";
                        break;

                    // Minute of hour range [0, 59]
                    case 'i':
                        add_instruction_or_extra_shift(&Action<T>::mysqlMinute, 2);
                        out_template += "00";
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHour12, 2);
                        out_template += "12";
                        break;

                    // Hour in 24h format (00-23)
                    case 'k':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHour24, 2);
                        out_template += "00";
                        break;

                    // Hour in 12h format (01-12)
                    case 'l':
                        add_instruction_or_extra_shift(&Action<T>::mysqlHour12, 2);
                        out_template += "12";
                        break;

                    case 't':
                        add_extra_shift(1);
                        out_template += "\t";
                        break;

                    case 'n':
                        add_extra_shift(1);
                        out_template += "\n";
                        break;

                    // Escaped literal characters.
                    case '%':
                        add_extra_shift(1);
                        out_template += "%";
                        break;

                    // Unimplemented
                    case 'U':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for WEEK (Sun-Sat)");
                    case 'v':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for WEEK (Mon-Sun)");
                    case 'x':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for YEAR for week (Mon-Sun)");
                    case 'X':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for YEAR for week (Sun-Sat)");

                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Incorrect syntax '{}', symbol is not supported '{}' for function {}",
                            format,
                            *pos,
                            getName());
                }

                ++pos;
            }
            else
            {
                add_extra_shift(end - pos);
                out_template += String(pos, end - pos);
                break;
            }
        }

        return out_template.size();
    }

    template <typename T>
    size_t parseJodaFormat(const String & format, std::vector<Action<T>> & instructions, UInt32, String &) const
    {
        /// If the argument was DateTime, add instruction for printing. If it was date, just append default literal
        auto add_instruction = [&](auto && func [[maybe_unused]], const String & default_literal [[maybe_unused]])
        {
            if constexpr (std::is_same_v<T, UInt32> || std::is_same_v<T, Int64>)
                instructions.emplace_back(func);
            else
                instructions.emplace_back(std::bind_front(&Action<T>::template jodaLiteral<String>, default_literal));
        };

        size_t reserve_size = 0;
        const char * pos = format.data();
        const char * end = pos + format.size();

        while (pos < end)
        {
            const char * cur_token = pos;

            // Literal case
            if (*cur_token == '\'')
            {
                // Case 1: 2 consecutive single quote
                if (pos + 1 < end && *(pos + 1) == '\'')
                {
                    std::string_view literal(cur_token, 1);
                    instructions.emplace_back(std::bind_front(&Action<T>::template jodaLiteral<decltype(literal)>, literal));
                    ++reserve_size;
                    pos += 2;
                }
                else
                {
                    // Case 2: find closing single quote
                    Int64 count = numLiteralChars(cur_token + 1, end);
                    if (count == -1)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No closing single quote for literal");
                    else
                    {
                        for (Int64 i = 1; i <= count; i++)
                        {
                            std::string_view literal(cur_token + i, 1);
                            instructions.emplace_back(
                                std::bind_front(&Action<T>::template jodaLiteral<decltype(literal)>, literal));
                            ++reserve_size;
                            if (*(cur_token + i) == '\'')
                                i += 1;
                        }
                        pos += count + 2;
                    }
                }
            }
            else
            {
                int repetitions = 1;
                ++pos;
                while (pos < end && *cur_token == *pos)
                {
                    ++repetitions;
                    ++pos;
                }
                switch (*cur_token)
                {
                    case 'G':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaEra, repetitions));
                        reserve_size += repetitions <= 3 ? 2 : 13;
                        break;
                    case 'C':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaCenturyOfEra, repetitions));
                        /// Year range [1900, 2299]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'Y':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaYearOfEra, repetitions));
                        /// Year range [1900, 2299]
                        reserve_size += repetitions == 2 ? 2 : std::max(repetitions, 4);
                        break;
                    case 'x':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaWeekYear, repetitions));
                        /// weekyear range [1900, 2299]
                        reserve_size += std::max(repetitions, 4);
                        break;
                    case 'w':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaWeekOfWeekYear, repetitions));
                        /// Week of weekyear range [1, 52]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'e':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaDayOfWeek1Based, repetitions));
                        /// Day of week range [1, 7]
                        reserve_size += std::max(repetitions, 1);
                        break;
                    case 'E':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaDayOfWeekText, repetitions));
                        /// Maximum length of short name is 3, maximum length of full name is 9.
                        reserve_size += repetitions <= 3 ? 3 : 9;
                        break;
                    case 'y':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaYear, repetitions));
                        /// Year range [1900, 2299]
                        reserve_size += repetitions == 2 ? 2 : std::max(repetitions, 4);
                        break;
                    case 'D':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaDayOfYear, repetitions));
                        /// Day of year range [1, 366]
                        reserve_size += std::max(repetitions, 3);
                        break;
                    case 'M':
                        if (repetitions <= 2)
                        {
                            instructions.emplace_back(std::bind_front(&Action<T>::jodaMonthOfYear, repetitions));
                            /// Month of year range [1, 12]
                            reserve_size += 2;
                        }
                        else
                        {
                            instructions.emplace_back(std::bind_front(&Action<T>::jodaMonthOfYearText, repetitions));
                            /// Maximum length of short name is 3, maximum length of full name is 9.
                            reserve_size += repetitions <= 3 ? 3 : 9;
                        }
                        break;
                    case 'd':
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaDayOfMonth, repetitions));
                        /// Day of month range [1, 3]
                        reserve_size += std::max(repetitions, 3);
                        break;
                    case 'a':
                        /// Default half day of day is "AM"
                        add_instruction(std::bind_front(&Action<T>::jodaHalfDayOfDay, repetitions), "AM");
                        reserve_size += 2;
                        break;
                    case 'K':
                        /// Default hour of half day is 0
                        add_instruction(
                            std::bind_front(&Action<T>::jodaHourOfHalfDay, repetitions), padValue(0, repetitions));
                        /// Hour of half day range [0, 11]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'h':
                        /// Default clock hour of half day is 12
                        add_instruction(
                            std::bind_front(&Action<T>::jodaClockHourOfHalfDay, repetitions),
                            padValue(12, repetitions));
                        /// Clock hour of half day range [1, 12]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'H':
                        /// Default hour of day is 0
                        add_instruction(std::bind_front(&Action<T>::jodaHourOfDay, repetitions), padValue(0, repetitions));
                        /// Hour of day range [0, 23]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'k':
                        /// Default clock hour of day is 24
                        add_instruction(std::bind_front(&Action<T>::jodaClockHourOfDay, repetitions), padValue(24, repetitions));
                        /// Clock hour of day range [1, 24]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'm':
                        /// Default minute of hour is 0
                        add_instruction(std::bind_front(&Action<T>::jodaMinuteOfHour, repetitions), padValue(0, repetitions));
                        /// Minute of hour range [0, 59]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 's':
                        /// Default second of minute is 0
                        add_instruction(std::bind_front(&Action<T>::jodaSecondOfMinute, repetitions), padValue(0, repetitions));
                        /// Second of minute range [0, 59]
                        reserve_size += std::max(repetitions, 2);
                        break;
                    case 'S':
                        /// Default fraction of second is 0
                        instructions.emplace_back(std::bind_front(&Action<T>::jodaFractionOfSecond, repetitions));
                        /// 'S' repetitions range [0, 9]
                        reserve_size += repetitions <= 9 ? repetitions : 9;
                        break;
                    case 'z':
                        if (repetitions <= 3)
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Short name time zone is not yet supported");

                        instructions.emplace_back(std::bind_front(&Action<T>::jodaTimezone, repetitions));
                        /// Longest length of full name of time zone is 32.
                        reserve_size += 32;
                        break;
                    case 'Z':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for TIMEZONE_OFFSET_ID");
                    default:
                        if (isalpha(*cur_token))
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for {}", String(cur_token, repetitions));

                        std::string_view literal(cur_token, pos - cur_token);
                        instructions.emplace_back(std::bind_front(&Action<T>::template jodaLiteral<decltype(literal)>, literal));
                        reserve_size += pos - cur_token;
                        break;
                }
            }
        }
        return reserve_size;
    }
};

struct NameFormatDateTime
{
    static constexpr auto name = "formatDateTime";
};

struct NameFromUnixTime
{
    static constexpr auto name = "fromUnixTimestamp";
};

struct NameFormatDateTimeInJodaSyntax
{
    static constexpr auto name = "formatDateTimeInJodaSyntax";
};

struct NameFromUnixTimeInJodaSyntax
{
    static constexpr auto name = "fromUnixTimestampInJodaSyntax";
};


using FunctionFormatDateTime = FunctionFormatDateTimeImpl<NameFormatDateTime, FormatDateTimeTraits::SupportInteger::No, FormatDateTimeTraits::FormatSyntax::MySQL>;
using FunctionFromUnixTimestamp = FunctionFormatDateTimeImpl<NameFromUnixTime, FormatDateTimeTraits::SupportInteger::Yes, FormatDateTimeTraits::FormatSyntax::MySQL>;
using FunctionFormatDateTimeInJodaSyntax = FunctionFormatDateTimeImpl<NameFormatDateTimeInJodaSyntax, FormatDateTimeTraits::SupportInteger::No, FormatDateTimeTraits::FormatSyntax::Joda>;
using FunctionFromUnixTimestampInJodaSyntax = FunctionFormatDateTimeImpl<NameFromUnixTimeInJodaSyntax, FormatDateTimeTraits::SupportInteger::Yes, FormatDateTimeTraits::FormatSyntax::Joda>;

}

REGISTER_FUNCTION(FormatDateTime)
{
    factory.registerFunction<FunctionFormatDateTime>();
    factory.registerAlias("DATE_FORMAT", FunctionFormatDateTime::name);

    factory.registerFunction<FunctionFromUnixTimestamp>();
    factory.registerAlias("FROM_UNIXTIME", "fromUnixTimestamp");

    factory.registerFunction<FunctionFormatDateTimeInJodaSyntax>();
    factory.registerFunction<FunctionFromUnixTimestampInJodaSyntax>();
}
}
