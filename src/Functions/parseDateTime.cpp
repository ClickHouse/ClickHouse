#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

#include <IO/WriteHelpers.h>
#include <base/types.h>

namespace DB
{

namespace
{
using Pos = const char *;

constexpr std::string_view weekdaysShort[] = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"};
constexpr std::string_view weekdaysFull[] = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"};
constexpr std::string_view monthsShort[] = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};

constexpr Int32 leapDays[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr Int32 normalDays[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

constexpr Int32 cumulativeLeapDays[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
constexpr Int32 cumulativeDays[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
constexpr Int32 cumulativeYearDays[]
    = {0,     365,   730,   1096,  1461,  1826,  2191,  2557,  2922,  3287,  3652,  4018,  4383,  4748,  5113,  5479,  5844,  6209,
       6574,  6940,  7305,  7670,  8035,  8401,  8766,  9131,  9496,  9862,  10227, 10592, 10957, 11323, 11688, 12053, 12418, 12784,
       13149, 13514, 13879, 14245, 14610, 14975, 15340, 15706, 16071, 16436, 16801, 17167, 17532, 17897, 18262, 18628, 18993, 19358,
       19723, 20089, 20454, 20819, 21184, 21550, 21915, 22280, 22645, 23011, 23376, 23741, 24106, 24472, 24837, 25202, 25567, 25933,
       26298, 26663, 27028, 27394, 27759, 28124, 28489, 28855, 29220, 29585, 29950, 30316, 30681, 31046, 31411, 31777, 32142, 32507,
       32872, 33238, 33603, 33968, 34333, 34699, 35064, 35429, 35794, 36160, 36525, 36890, 37255, 37621, 37986, 38351, 38716, 39082,
       39447, 39812, 40177, 40543, 40908, 41273, 41638, 42004, 42369, 42734, 43099, 43465, 43830, 44195, 44560, 44926, 45291, 45656,
       46021, 46387, 46752, 47117, 47482, 47847, 48212, 48577, 48942, 49308, 49673};


constexpr Int32 minYear = 1970;
constexpr Int32 maxYear = 2106;

struct Date
{
    Int32 year = 1970;
    Int32 month = 1;
    Int32 day = 1;
    bool is_ad = true; // AD -> true, BC -> false.

    Int32 week = 1; // Week of year based on ISO week date, e.g: 27
    Int32 day_of_week = 1; // Day of week, Monday:1, Tuesday:2, ..., Sunday:7
    bool week_date_format = false;

    Int32 day_of_year = 1;
    bool day_of_year_format = false;

    bool century_format = false;

    bool is_year_of_era = false; // Year of era cannot be zero or negative.
    bool has_year = false; // Whether year was explicitly specified.

    Int32 hour = 0;
    Int32 minute = 0;
    Int32 second = 0;
    // Int32 microsecond = 0;
    bool is_am = true; // AM -> true, PM -> false
    std::optional<Int64> time_zone_offset;

    bool is_clock_hour = false; // Whether most recent hour specifier is clockhour
    bool is_hour_of_half_day = false; // Whether most recent hour specifier is of half day.

    std::vector<Int32> day_of_month_values;
    std::vector<Int32> day_of_year_values;

    /// For debug
    [[maybe_unused]] String toString() const
    {
        String res;
        res += "year:" + std::to_string(year);
        res += ",";
        res += "month:" + std::to_string(month);
        res += ",";
        res += "day:" + std::to_string(day);
        res += ",";
        res += "hour:" + std::to_string(hour);
        res += ",";
        res += "minute:" + std::to_string(minute);
        res += ",";
        res += "second:" + std::to_string(second);
        return res;
    }

    static bool isLeapYear(Int32 year_) { return year_ % 4 == 0 && (year_ % 100 != 0 || year_ % 400 == 0); }

    static bool isDateValid(Int32 year_, Int32 month_, Int32 day_)
    {
        if (month_ < 1 || month_ > 12)
            return false;

        if (year_ < minYear || year_ > maxYear)
            return false;

        bool leap = isLeapYear(year_);
        if (day_ < 1)
            return false;

        if (leap && day_ > leapDays[month_])
            return false;

        if (!leap && day_ > normalDays[month_])
            return false;
        return true;
    }

    static bool isDayOfYearValid(Int32 year_, Int32 day_of_year_)
    {
        if (year_ < minYear || year_ > maxYear)
            return false;

        if (day_of_year_ < 1 || day_of_year_ > 365 + (isLeapYear(year_) ? 1 : 0))
            return false;

        return true;
    }

    static bool isWeekDateValid(Int32 week_year_, Int32 week_of_year_, Int32 day_of_week_)
    {
        if (day_of_week_ < 1 || day_of_week_ > 7)
            return false;

        if (week_of_year_ < 1 || week_of_year_ > 52)
            return false;

        if (week_year_ < minYear || week_year_ > maxYear)
            return false;

        return true;
    }

    static Int32 extractISODayOfTheWeek(Int32 days_since_epoch)
    {
        if (days_since_epoch < 0)
        {
            // negative date: start off at 4 and cycle downwards
            return (7 - ((-int64_t(days_since_epoch) + 3) % 7));
        }
        else
        {
            // positive date: start off at 4 and cycle upwards
            return ((int64_t(days_since_epoch) + 3) % 7) + 1;
        }
    }

    static Int32 daysSinceEpochFromWeekDate(int32_t week_year_, int32_t week_of_year_, int32_t day_of_week_)
    {
        if (!isWeekDateValid(week_year_, week_of_year_, day_of_week_))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid week date");

        Int32 days_since_epoch_of_jan_fourth = daysSinceEpochFromDate(week_year_, 1, 4);
        Int32 first_day_of_week_year = extractISODayOfTheWeek(days_since_epoch_of_jan_fourth);
        return days_since_epoch_of_jan_fourth - (first_day_of_week_year - 1) + 7 * (week_of_year_ - 1) + day_of_week_ - 1;
    }

    static Int32 daysSinceEpochFromDayOfYear(Int32 year_, Int32 day_of_year_)
    {
        if (!isDayOfYearValid(year_, day_of_year_))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of year");

        Int32 res = daysSinceEpochFromDate(year_, 1, 1);
        res += day_of_year_ - 1;
        return res;
    }

    static Int32 daysSinceEpochFromDate(Int32 year_, Int32 month_, Int32 day_)
    {
        if (!isDateValid(year_, month_, day_))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid date");

        Int32 res = cumulativeYearDays[year_ - 1970];
        res += isLeapYear(year_) ? cumulativeLeapDays[month_ - 1] : cumulativeDays[month_ - 1];
        res += day_ - 1;
        return res;
    }


    Int64 checkAndGetDateTime(const DateLUTImpl & time_zone)
    {
        /// Era is BC and year of era is provided
        if (is_year_of_era && !is_ad)
            year = -1 * (year - 1);

        if (is_hour_of_half_day && !is_am)
            hour += 12;


        /// Ensure all day of year values are valid for ending year value
        for (const auto d : day_of_month_values)
        {
            if (!isDateValid(year, month, d))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of month.");
        }

        // Ensure all day of year values are valid for ending year value
        for (const auto d : day_of_year_values)
        {
            if (!isDayOfYearValid(year, d))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of year.");
        }

        // Convert the parsed date/time into a timestamp.
        Int32 days_since_epoch;
        if (week_date_format)
            days_since_epoch = daysSinceEpochFromWeekDate(year, week, day_of_week);
        else if (day_of_year_format)
            days_since_epoch = daysSinceEpochFromDayOfYear(year, day_of_year);
        else
            days_since_epoch = daysSinceEpochFromDate(year, month, day);

        Int64 seconds_since_epoch = days_since_epoch * 86400 + hour * 3600 + minute * 60 + second;

        /// Time zone is not specified, use local time zone
        if (!time_zone_offset)
            *time_zone_offset = time_zone.getOffsetAtStartOfEpoch();

        // std::cout << "timezonename:" << time_zone.getTimeZone() << std::endl;
        // std::cout << "time_zone_offset:" << *time_zone_offset << time_zone.getOffsetAtStartOfEpoch() << std::endl;
        // std::cout << "before timestamp:" << seconds_since_epoch << std::endl;
        /// Time zone is specified in format string.
        seconds_since_epoch -= *time_zone_offset;
        // std::cout << "after timestamp:" << seconds_since_epoch << std::endl;
        return seconds_since_epoch;
    }
};

class Action
{
private:
    using Func = Pos (*)(Pos cur, Pos end, Date & date);
    Func func{nullptr};
    std::string func_name;

    std::string literal;

public:
    explicit Action(Func func_, const char * func_name_) : func(func_), func_name(func_name_) { }

    template <typename Literal>
    explicit Action(const Literal & literal_) : literal(literal_)
    {
    }

    /// For debug
    [[maybe_unused]] String toString()const
    {
        if (func)
            return "func:" + func_name;
        else
            return "literal:" + literal;
    }

    Pos perform(Pos cur, Pos end, Date & date) const
    {
        if (func)
            return func(cur, end, date);
        else
        {
            ensureSpace(cur, end, literal.size(), "requires size >= " + std::to_string(literal.size()));
            if (std::string_view(cur, literal.size()) != literal)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Expect literal {} but {} provided", literal, std::string_view(cur, literal.size()));
            cur += literal.size();
            return cur;
        }
    }

    template <typename T>
    static Pos readNumber2(Pos cur, Pos end, T & res)
    {
        ensureSpace(cur, end, 2, "readNumber2 requires size >= 2");
        res = (*cur - '0') * 10;
        ++cur;
        res += *cur - '0';
        ++cur;
        return cur;
    }

    template <typename T>
    static Pos readNumber3(Pos cur, Pos end, T & res)
    {
        cur = readNumber2(cur, end, res);

        ensureSpace(cur, end, 1, "readNumber3 requires size >= 3");
        res = res * 10 + (*cur - '0');
        ++cur;
        return cur;
    }

    template <typename T>
    static Pos readNumber4(Pos cur, Pos end, T & res)
    {
        cur = readNumber2(cur, end, res);

        T tmp;
        cur = readNumber2(cur, end, tmp);
        res = res * 100 + tmp;
        return cur;
    }

    static ALWAYS_INLINE void ensureSpace(Pos cur, Pos end, size_t len, const String & msg)
    {
        if (cur > end || cur + len > end)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because {}", msg);
    }

    static ALWAYS_INLINE Pos assertChar(Pos cur, Pos end, char ch)
    {
        ensureSpace(cur, end, 1, "assertChar requires size >= 1");

        if (*cur != ch)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expect char {}, but {} provided", String(ch, 1), String(*cur, 1));

        ++cur;
        return cur;
    }

    static Pos mysqlDayOfWeekTextShort(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 3, "Parsing DayOfWeekTextShort requires size >= 3");

        String str(cur, 3);
        Poco::toLower(str);
        Int32 i = 0;
        for (; i < 7; ++i)
            if (str == weekdaysShort[i])
                break;

        if (i == 7)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because unknown short week text");

        date.day_of_week = i + 1;
        date.week_date_format = true;
        date.day_of_year_format = false;
        if (!date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        cur += 3;
        return cur;
    }

    static Pos mysqlMonthOfYearTextShort(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 3, "Parsing MonthOfYearTextShort requires size >= 3");

        String str(cur, 3);
        Poco::toLower(str);

        Int32 i = 0;
        for (; i < 12; ++i)
            if (str == monthsShort[i])
                break;

        if (i == 12)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because unknown short month text");

        date.month = i + 1;
        cur += 3;
        return cur;
    }

    static Pos mysqlMonth(Pos cur, Pos end, Date & date) { return readNumber2(cur, end, date.month); }

    static Pos mysqlCentury(Pos cur, Pos end, Date & date)
    {
        Int32 centuray;
        cur = readNumber2(cur, end, centuray);
        date.century_format = true;
        date.year = centuray * 100;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlDayOfMonth(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.day);
        date.day_of_month_values.push_back(date.day);
        date.week_date_format = false;
        date.day_of_year_format = false;
        if (!date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        return cur;
    }

    static Pos mysqlAmericanDate(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.month);
        cur = assertChar(cur, end, '/');

        cur = readNumber2(cur, end, date.day);
        cur = assertChar(cur, end, '/');

        cur = readNumber2(cur, end, date.year);
        cur = assertChar(cur, end, '/');

        date.week_date_format = false;
        date.day_of_year_format = false;
        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }


    static Pos mysqlDayOfMonthSpacePadded(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 2, "mysqlDayOfMonthSpacePadded requires size >= 2");

        date.day = *cur == ' ' ? 0 : (*cur - '0');
        ++cur;

        date.day = 10 * date.day + (*cur - '0');
        ++cur;

        date.week_date_format = false;
        date.day_of_year_format = false;
        if (!date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        return cur;
    }

    static Pos mysqlISO8601Date(Pos cur, Pos end, Date & date)
    {
        cur = readNumber4(cur, end, date.year);
        cur = assertChar(cur, end, '-');
        cur = readNumber2(cur, end, date.month);
        cur = assertChar(cur, end, '-');
        cur = readNumber2(cur, end, date.day);

        date.week_date_format = false;
        date.day_of_year_format = false;

        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlISO8601Year2(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.year);
        date.year += 2000;
        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlISO8601Year4(Pos cur, Pos end, Date & date)
    {
        cur = readNumber4(cur, end, date.year);
        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlDayOfYear(Pos cur, Pos end, Date & date)
    {
        cur = readNumber3(cur, end, date.day_of_year);

        date.day_of_year_values.push_back(date.day_of_year);
        date.day_of_year_format = true;
        date.week_date_format = false;
        if (!date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        return cur;
    }

    static Pos mysqlDayOfWeek(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1");

        date.day_of_week = *cur - '0';
        date.week_date_format = true;
        date.day_of_year_format = false;
        if (!date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        return cur;
    }

    static Pos mysqlISO8601Week(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.week);
        date.week_date_format = true;
        date.day_of_year_format = false;
        if (date.has_year)
        {
            date.has_year = true;
            date.year = 2000;
        }
        return cur;
    }

    static Pos mysqlDayOfWeek0To6(Pos cur, Pos end, Date & date)
    {
        cur = mysqlDayOfWeek(cur, end, date);
        if (date.day_of_week == 0)
            date.day_of_week = 7;

        return cur;
    }

    static Pos mysqlDayOfWeekTextLong(Pos cur, Pos end, Date & date)
    {
        mysqlDayOfWeekTextShort(cur, end, date);
        auto expect_text = weekdaysFull[date.day_of_week - 1];

        ensureSpace(cur, end, expect_text.size(), "mysqlDayOfWeekTextLong requires size >= " + std::to_string(expect_text.size()));
        std::string_view text(cur, expect_text.size());
        if (text != expect_text)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because unknown full day of week text {}", expect_text);

        cur += expect_text.size();
        return cur;
    }

    static Pos mysqlYear2(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.year);
        date.year += 2000;
        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlYear4(Pos cur, Pos end, Date & date)
    {
        cur = readNumber4(cur, end, date.year);
        date.century_format = false;
        date.is_year_of_era = false;
        date.has_year = true;
        return cur;
    }

    static Pos mysqlTimezoneOffset(Pos cur, Pos end, Date & date)
    {
        /// TODO figure out what timezone_id mean
        ensureSpace(cur, end, 1, "Parse mysqlTimezoneOffset failed");
        Int32 sign = 1;
        if (*cur == '-')
            sign = -1;
        ++cur;

        Int32 hour;
        cur = readNumber2(cur, end, hour);

        Int32 minute;
        cur = readNumber2(cur, end, minute);

        *date.time_zone_offset = sign * (hour * 3600 + minute * 60);
        return cur;
    }

    static Pos mysqlMinute(Pos cur, Pos end, Date & date) { return readNumber2(cur, end, date.minute); }

    static Pos mysqlAMPM(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 2, "mysqlAMPM requires size >= 2");

        std::string text(cur, 2);
        Poco::toUpper(text);
        if (text == "PM")
            date.is_am = true;
        else if (text == "AM")
            date.is_am = false;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Text should be AM or PM, but {} provided", text);

        cur += 2;
        return cur;
    }

    static Pos mysqlHHMM12(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        date.is_clock_hour = false;
        date.is_hour_of_half_day = true;

        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.minute);
        cur = assertChar(cur, end, ' ');
        cur = mysqlAMPM(cur, end, date);
        return cur;
    }

    static Pos mysqlHHMM24(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        date.is_clock_hour = false;
        date.is_hour_of_half_day = false;

        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.minute);
        return cur;
    }

    static Pos mysqlSecond(Pos cur, Pos end, Date & date) { return readNumber2(cur, end, date.second); }

    static Pos mysqlISO8601Time(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.minute);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.second);

        date.is_clock_hour = false;
        date.is_hour_of_half_day = false;
        return cur;
    }

    static Pos mysqlHour12(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        date.is_hour_of_half_day = true;
        date.is_clock_hour = false;
        return cur;
    }

    static Pos mysqlHour24(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        date.is_hour_of_half_day = false;
        date.is_clock_hour = false;
        return cur;
    }
};


struct ParseDateTimeTraits
{
    enum class ParseSyntax
    {
        MySQL,
        Joda
    };
};

#define ACTION_ARGS(func) &func, #func


/// _FUNC_(str[, format, timezone])
template <typename Name, /*ParseDateTimeTraits::SupportInteger support_integer, */ ParseDateTimeTraits::ParseSyntax parse_syntax>
class FunctionParseDateTimeImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParseDateTimeImpl>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3",
                getName(),
                arguments.size());

        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {} when arguments size is 1. Should be string",
                arguments[0].type->getName(),
                getName());

        if (arguments.size() > 1 && !isString(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {} when arguments size is 1. Should be string",
                arguments[0].type->getName(),
                getName());

        if (arguments.size() > 2 && !isString(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {} when arguments size is 1. Should be string",
                arguments[0].type->getName(),
                getName());

        String time_zone_name = getTimeZone(arguments).second;
        return std::make_shared<DataTypeDateTime>(time_zone_name);
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first ('str') argument of function {}. Must be string.",
                arguments[0].column->getName(),
                getName());

        String format = getFormat(arguments);
        const auto * time_zone = getTimeZone(arguments).first;
        // std::cout << "timezonename:" << getTimeZone(arguments).second << std::endl;

        std::vector<Action> instructions;
        parseFormat(format, instructions);

        auto col_res = ColumnDateTime::create();
        col_res->reserve(input_rows_count);
        auto & data_res = col_res->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef str_ref = col_str->getDataAt(i);
            Pos cur = str_ref.data;
            Pos end = str_ref.data + str_ref.size;
            Date date;
            for (const auto & instruction : instructions)
            {
                cur = instruction.perform(cur, end, date);
                // std::cout << "instruction:" << instruction.toString() << std::endl;
                // std::cout << "date:" << date.toString() << std::endl;
            }

            // Ensure all input was consumed.
            if (cur < end)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invalid format input {} is malformed at {}",
                    str_ref.toView(),
                    std::string_view(cur, end - cur));

            Int64 time = date.checkAndGetDateTime(*time_zone);
            data_res.push_back(static_cast<UInt32>(time));
        }

        return col_res;
    }


private:
    ALWAYS_INLINE void parseFormat(const String & format, std::vector<Action> & instructions) const
    {
        if constexpr (parse_syntax == ParseDateTimeTraits::ParseSyntax::MySQL)
            parseMysqlFormat(format, instructions);
        else if constexpr (parse_syntax == ParseDateTimeTraits::ParseSyntax::Joda)
            parseJodaFormat(format, instructions);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unknown datetime format style {} in function {}",
                magic_enum::enum_name(parse_syntax),
                getName());
    }

    ALWAYS_INLINE void parseMysqlFormat(const String & format, std::vector<Action> & instructions) const
    {
        Pos pos = format.data();
        Pos end = pos + format.size();
        while (true)
        {
            Pos percent_pos = find_first_symbols<'%'>(pos, end);
            if (percent_pos < end)
            {
                if (pos < percent_pos)
                    instructions.emplace_back(std::string_view(pos, percent_pos - pos));

                pos = percent_pos + 1;
                if (pos >= end)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sign '%' is the last in format, if you need it, use '%%'");

                switch (*pos)
                {
                    // Abbreviated weekday [Mon...Sun]
                    case 'a':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfWeekTextShort));
                        break;

                    // Abbreviated month [Jan...Dec]
                    case 'b':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlMonthOfYearTextShort));
                        break;

                    // Month as a decimal number (01-12)
                    case 'c':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlMonth));
                        break;

                    // Year, divided by 100, zero-padded
                    case 'C':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlCentury));
                        break;

                    // Day of month, zero-padded (01-31)
                    case 'd':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfMonth));
                        break;

                    // Short MM/DD/YY date, equivalent to %m/%d/%y
                    case 'D':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlAmericanDate));
                        break;

                    // Day of month, space-padded ( 1-31)  23
                    case 'e':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfMonthSpacePadded));
                        break;

                    // Fractional seconds
                    case 'f':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for fractional seconds");

                    // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                    case 'F':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlISO8601Date));
                        break;

                    // Last two digits of year of ISO 8601 week number (see %G)
                    case 'g':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlISO8601Year2));
                        break;

                    // Year of ISO 8601 week number (see %V)
                    case 'G':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlISO8601Year4));
                        break;

                    // Day of the year (001-366)   235
                    case 'j':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfYear));
                        break;

                    // Month as a decimal number (01-12)
                    case 'm':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlMonth));
                        break;

                    // ISO 8601 weekday as number with Monday as 1 (1-7)
                    case 'u':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfWeek));
                        break;

                    // ISO 8601 week number (01-53)
                    case 'V':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlISO8601Week));
                        break;

                    // Weekday as a decimal number with Sunday as 0 (0-6)  4
                    case 'w':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfWeek0To6));
                        break;

                    // Full weekday [Monday...Sunday]
                    case 'W':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlDayOfWeekTextLong));
                        break;

                    // Two digits year
                    case 'y':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlYear2));
                        break;

                    // Four digits year
                    case 'Y':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlYear4));
                        break;

                    // Quarter (1-4)
                    case 'Q':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for quarter");
                        break;

                    // Offset from UTC timezone as +hhmm or -hhmm
                    case 'z':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlTimezoneOffset));
                        break;

                    /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                    // Minute (00-59)
                    case 'M':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlMinute));
                        break;

                    // AM or PM
                    case 'p':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlAMPM));
                        break;

                    // 12-hour HH:MM time, equivalent to %h:%i %p 2:55 PM
                    case 'r':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHHMM12));
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%i 14:55
                    case 'R':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHHMM24));
                        break;

                    // Seconds
                    case 's':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlSecond));
                        break;

                    // Seconds
                    case 'S':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlSecond));
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S 14:55:02
                    case 'T':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlISO8601Time));
                        break;

                    // Hour in 12h format (01-12)
                    case 'h':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHour12));
                        break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHour24));
                        break;

                    // Minute of hour range [0, 59]
                    case 'i':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlMinute));
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHour12));
                        break;

                    // Hour in 24h format (00-23)
                    case 'k':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHour24));
                        break;

                    // Hour in 12h format (01-12)
                    case 'l':
                        instructions.emplace_back(ACTION_ARGS(Action::mysqlHour12));
                        break;

                    case 't':
                        instructions.emplace_back("\t");
                        break;

                    case 'n':
                        instructions.emplace_back("\n");
                        break;

                    // Escaped literal characters.
                    case '%':
                        instructions.emplace_back("\n");
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
                if (pos < end)
                    instructions.emplace_back(std::string_view(pos, end - pos));
                break;
            }
        }
    }

    void parseJodaFormat(const String & /*format*/, std::vector<Action> & /*instructions*/) const { }


    ALWAYS_INLINE String getFormat(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() < 2)
        {
            if constexpr (parse_syntax == ParseDateTimeTraits::ParseSyntax::Joda)
                return "yyyy-MM-dd HH:mm:ss";
            else
                return "%Y-%m-%d %H:%M:%S";
        }

        const auto * format_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!format_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second ('format') argument of function {}. Must be constant string.",
                arguments[1].column->getName(),
                getName());
        return format_column->getValue<String>();
    }

    ALWAYS_INLINE std::pair<const DateLUTImpl *, String> getTimeZone(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() < 3)
            return {&DateLUT::instance(), ""};

        const auto * col = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
        if (!col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of third ('timezone') argument of function {}. Must be constant string.",
                arguments[2].column->getName(),
                getName());

        String time_zone = col->getValue<String>();
        if (time_zone.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Provided time zone must be non-empty and be a valid time zone");
        return {&DateLUT::instance(time_zone), time_zone};
    }
};

struct NameParseDateTime
{
    static constexpr auto name = "parseDateTime";
};

using FunctionParseDateTime = FunctionParseDateTimeImpl<NameParseDateTime, ParseDateTimeTraits::ParseSyntax::MySQL>;
}

REGISTER_FUNCTION(ParseDateTime)
{
    factory.registerFunction<FunctionParseDateTime>();
    factory.registerAlias("TO_UNIXTIME", "parseDateTime");
}


}
