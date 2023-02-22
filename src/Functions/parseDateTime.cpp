#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace
{

struct Date
{
    Int32 year = 1970;
    Int32 month = 1;
    Int32 day = 1;
    bool is_ad = true;                  // AD -> true, BC -> false.

    Int32 week = 1;                     // Week of year based on ISO week date, e.g: 27
    Int32 day_of_week = 1;              // Day of week, Monday:1, Tuesday:2, ..., Sunday:7
    bool week_date_format = false;

    Int32 day_of_year = 1;
    bool day_of_year_format = false;

    bool century_format = false;

    bool is_year_of_era = false;        // Year of era cannot be zero or negative.
    bool has_year = false;              // Whether year was explicitly specified.

    Int32 hour = 0;
    Int32 minute = 0;
    Int32 second = 0;
    // Int32 microsecond = 0;
    bool is_am = true; // AM -> true, PM -> false
    Int64 timezone_id = -1;

    bool is_clock_hour = false; // Whether most recent hour specifier is clockhour
    bool is_hour_of_half_day = false; // Whether most recent hour specifier is of half day.

    std::vector<Int32> day_of_month_values;
    std::vector<Int32> day_of_year_values;
};

constexpr std::string_view weekdaysShort[] = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"};
constexpr std::string_view weekdaysFull[] = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"};

constexpr std::string_view monthsShort[]
    = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};

constexpr char digits100[201] = "00010203040506070809"
                                "10111213141516171819"
                                "20212223242526272829"
                                "30313233343536373839"
                                "40414243444546474849"
                                "50515253545556575859"
                                "60616263646566676869"
                                "70717273747576777879"
                                "80818283848586878889"
                                "90919293949596979899";

using Pos = const char *;

class Action
{
private:
    using Func = Pos (*)(Pos cur, Pos end, Date & date);
    Func func;
    std::string literal;

public:
    explicit Action(Func && func_) : func(std::move(func_)) {}

    template <typename Literal>
    explicit Action(const Literal & literal_) : literal(literal_)
    {
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
        res += *cur;
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

    static Pos mysqlMonth(Pos cur, Pos end, Date & date)
    {
        return readNumber2(cur, end, date.month);
    }

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

    /*
    static Pos mysqlFractionalSecond(Pos, Pos, Date &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for fractional second");
    }
    */

    static Pos mysqlISO8601Date(Pos cur, Pos end, Date & date)
    {
        cur = readNumber4(cur, end, date.year);
        cur = readNumber2(cur, end, date.month);
        cur = readNumber2(cur, end, date.day);
        return cur;
    }

    static Pos mysqlISO8601Year2(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.year);
        date.year += 2000;
        return cur;
    }

    static Pos mysqlISO8601Year4(Pos cur, Pos end, Date & date)
    {
        cur = readNumber4(cur, end, date.year);
        return cur;
    }

    static Pos mysqlDayOfYear(Pos cur, Pos end, Date & date)
    {
        cur = readNumber3(cur, end, date.day_of_year);
        date.day_of_year_format = true;
        return cur;
    }

    static Pos mysqlDayOfWeek(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1");

        date.day_of_week = *cur - '0';
        return cur;
    }

    static Pos mysqlISO8601Week(Pos cur, Pos end, Date & date)
    {
        return readNumber2(cur, end, date.week);
    }

    static Pos mysqlDayOfWeek0To6(Pos cur, Pos end, Date & date)
    {
        Pos res = mysqlDayOfWeek(cur, end, date);

        if (date.day_of_week == 7)
            date.day_of_week = 0;
        return res;
    }

    static Pos mysqlDayOfWeekTextLong(Pos cur, Pos end, Date & date)
    {
        mysqlDayOfWeekTextShort(cur, end, date);
        auto expect_text = weekdaysFull[date.day_of_week];

        ensureSpace(cur, end, expect_text.size(), "mysqlDayOfWeekTextLong requires size >= " + std::to_string(expect_text.size()));
        std::string_view text(cur, expect_text.size());
        if (text != expect_text)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because unknown full day of week text {}", expect_text);

        cur += expect_text.size();
        return cur;
    }

    static Pos mysqlYear2(Pos cur, Pos end, Date & date)
    {
        Pos res = readNumber2(cur, end, date.year);
        date.year += 2000;
        return res;
    }


    static Pos mysqlYear4(Pos cur, Pos end, Date & date)
    {
        return readNumber4(cur, end, date.year);
    }

    /*
    static Pos mysqlQuarter(Pos cur, Pos end, Date & date)
    {
        /// TODO
    }
    */

    static Pos mysqlTimezoneOffset(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 1, "Parse mysqlTimezoneOffset failed");
        Int32 sign = 1;
        if (*cur == '-')
            sign = -1;
        ++cur;

        Int32 hour;
        cur = readNumber2(cur, end, hour);

        Int32 minute;
        cur = readNumber2(cur, end, minute);

        date.timezone_id = sign * (hour * 3600 + minute);
        return cur;
    }

    static Pos mysqlMinute(Pos cur, Pos end, Date & date)
    {
        return readNumber2(cur, end, date.minute);
    }

    static Pos mysqlAMPM(Pos cur, Pos end, Date & date)
    {
        ensureSpace(cur, end, 2, "mysqlAMPM requires size >= 2");

        std::string_view text(cur, 2);
        if (text == "PM")
            date.is_am = false;
        else if (text == "AM")
            date.is_am = true;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Text should be AM or PM, but {} provided", text);

        cur += 2;
        return cur;
    }

    static Pos mysqlHHMM12(Pos cur, Pos end, Date & date)
    {
        Int32 hour;
        Int32 minute;
        cur = readNumber2(cur, end, hour);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, minute);
        cur = assertChar(cur, end, ' ');
        cur = mysqlAMPM(cur, end, date);

        /// TODO process hour and minute
        return cur;
    }

    static Pos mysqlHHMM24(Pos cur, Pos end, Date & date)
    {
        Int32 hour;
        Int32 minute;

        cur = readNumber2(cur, end, hour);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, minute);

        /// TODO process hour and minute
        return cur;
    }

    static Pos mysqlSecond(Pos cur, Pos end, Date & date)
    {
        return readNumber2(cur, end, date.second);
    }

    static Pos mysqlISO8601Time(Pos cur, Pos end, Date & date)
    {
        cur = readNumber2(cur, end, date.hour);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.minute);
        cur = assertChar(cur, end, ':');
        cur = readNumber2(cur, end, date.second);
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
    /*
    enum class SupportInteger
    {
        Yes,
        No
    };
    */

    enum class ParseSyntax
    {
        MySQL,
        Joda
    };
};


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

        return std::make_shared<DataTypeDateTime>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] size_t input_rows_count) const override
    {
        const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first ('str') argument of function {}. Must be string.",
                arguments[0].column->getName(), getName());

        String format = getFormat(arguments);
        const auto & time_zone = getTimeZone(arguments);

        std::vector<Action> instructions;
    }



private:

    void parseFormat(const String & format, std::vector<Action> & instructions)
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

    void parseMysqlFormat(const String & format, std::vector<Action> & instructions)
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
                        instructions.emplace_back(&Action::mysqlDayOfWeekTextShort);
                        break;

                    // Abbreviated month [Jan...Dec]
                    case 'b':
                        instructions.emplace_back(&Action::mysqlMonthOfYearTextShort);
                        break;

                    // Month as a decimal number (01-12)
                    case 'c':
                        instructions.emplace_back(&Action::mysqlMonth);
                        break;

                    // Year, divided by 100, zero-padded
                    case 'C':
                        instructions.emplace_back(&Action::mysqlCentury);
                        break;

                    // Day of month, zero-padded (01-31)
                    case 'd':
                        instructions.emplace_back(&Action::mysqlDayOfMonth);
                        break;

                    // Short MM/DD/YY date, equivalent to %m/%d/%y
                    case 'D':
                        instructions.emplace_back(&Action::mysqlAmericanDate);
                        break;

                    // Day of month, space-padded ( 1-31)  23
                    case 'e':
                        instructions.emplace_back(&Action::mysqlDayOfMonthSpacePadded);
                        break;

                    // Fractional seconds
                    case 'f':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for fractional seconds");

                    // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                    case 'F':
                        instructions.emplace_back(&Action::mysqlISO8601Date);
                        break;

                    // Last two digits of year of ISO 8601 week number (see %G)
                    case 'g':
                        instructions.emplace_back(&Action::mysqlISO8601Year2);
                        break;

                    // Year of ISO 8601 week number (see %V)
                    case 'G':
                        instructions.emplace_back(&Action::mysqlISO8601Year4);
                        break;

                    // Day of the year (001-366)   235
                    case 'j':
                        instructions.emplace_back(&Action::mysqlDayOfYear);
                        break;

                    // Month as a decimal number (01-12)
                    case 'm':
                        instructions.emplace_back(&Action::mysqlMonth);
                        break;

                    // ISO 8601 weekday as number with Monday as 1 (1-7)
                    case 'u':
                        instructions.emplace_back(&Action::mysqlDayOfWeek);
                        break;

                    // ISO 8601 week number (01-53)
                    case 'V':
                        instructions.emplace_back(&Action::mysqlISO8601Week);
                        break;

                    // Weekday as a decimal number with Sunday as 0 (0-6)  4
                    case 'w':
                        instructions.emplace_back(&Action::mysqlDayOfWeek0To6);
                        break;

                    // Full weekday [Monday...Sunday]
                    case 'W':
                        instructions.emplace_back(&Action::mysqlDayOfWeekTextLong);
                        break;

                    // Two digits year
                    case 'y':
                        instructions.emplace_back(&Action::mysqlYear2);
                        break;

                    // Four digits year
                    case 'Y':
                        instructions.emplace_back(&Action::mysqlYear4);
                        break;

                    // Quarter (1-4)
                    case 'Q':
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for quarter");
                        break;

                    // Offset from UTC timezone as +hhmm or -hhmm
                    case 'z':
                        instructions.emplace_back(&Action::mysqlTimezoneOffset);
                        break;

                    /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                    // Minute (00-59)
                    case 'M':
                        instructions.emplace_back(&Action::mysqlMinute);
                        break;

                    // AM or PM
                    case 'p':
                        instructions.emplace_back(&Action::mysqlAMPM);
                        break;

                    // 12-hour HH:MM time, equivalent to %h:%i %p 2:55 PM
                    case 'r':
                        instructions.emplace_back(&Action::mysqlHHMM12);
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%i 14:55
                    case 'R':
                        instructions.emplace_back(&Action::mysqlHHMM24);
                        break;

                    // Seconds
                    case 's':
                        instructions.emplace_back(&Action::mysqlSecond);
                        break;

                    // Seconds
                    case 'S':
                        instructions.emplace_back(&Action::mysqlSecond);
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S 14:55:02
                    case 'T':
                        instructions.emplace_back(&Action::mysqlISO8601Time);
                        break;

                    // Hour in 12h format (01-12)
                    case 'h':
                        instructions.emplace_back(&Action::mysqlHour12);
                        break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        instructions.emplace_back(&Action::mysqlHour24);
                        break;

                    // Minute of hour range [0, 59]
                    case 'i':
                        instructions.emplace_back(&Action::mysqlMinute);
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        instructions.emplace_back(&Action::mysqlHour12);
                        break;

                    // Hour in 24h format (00-23)
                    case 'k':
                        instructions.emplace_back(&Action::mysqlHour24);
                        break;

                    // Hour in 12h format (01-12)
                    case 'l':
                        instructions.emplace_back(&Action::mysqlHour12);
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
                instructions.emplace_back(std::string_view(pos, end - pos));
                break;
            }
        }
    }

    void parseJodaFormat(const String & format, std::vector<Action> & instructions)
    {
        /// TODO
    }



    String getFormat(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() < 2)
        {
            if constexpr (parse_syntax == ParseDateTimeTraits::ParseSyntax::Joda)
                return "yyyy-MM-dd HH:mm:ss";
            else
                return "%F %T";
        }

        const auto * format_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!format_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second ('format') argument of function {}. Must be constant string.",
                arguments[1].column->getName(), getName());
        return format_column->getValue<String>();
    }

    const DateLUTImpl & getTimeZone(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() < 3)
            return DateLUT::instance();

        const auto * col = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of third ('timezone') argument of function {}. Must be constant string.",
                arguments[2].column->getName(), getName());

        String time_zone = col->getValue<String>();
        if (time_zone.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Provided time zone must be non-empty and be a valid time zone");
        return DateLUT::instance(time_zone);
    }
};

}

}
