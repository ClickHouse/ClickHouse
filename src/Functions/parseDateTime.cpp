#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDateTime.h>
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

    constexpr Int32 minYear = 1970;
    constexpr Int32 maxYear = 2106;

    const std::unordered_map<String, std::pair<String, Int32>> dayOfWeekMap{
        {"mon", {"day", 1}},
        {"tue", {"sday", 2}},
        {"wed", {"nesday", 3}},
        {"thu", {"rsday", 4}},
        {"fri", {"day", 5}},
        {"sat", {"urday", 6}},
        {"sun", {"day", 7}},
    };

    const std::unordered_map<String, std::pair<String, Int32>> monthMap{
        {"jan", {"uary", 1}},
        {"feb", {"ruary", 2}},
        {"mar", {"rch", 3}},
        {"apr", {"il", 4}},
        {"may", {"", 5}},
        {"jun", {"e", 6}},
        {"jul", {"y", 7}},
        {"aug", {"ust", 8}},
        {"sep", {"tember", 9}},
        {"oct", {"ober", 10}},
        {"nov", {"ember", 11}},
        {"dec", {"ember", 12}},
    };

    /// key: month, value: total days of current month if current year is leap year.
    constexpr Int32 leapDays[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /// key: month, value: total days of current month if current year is not leap year.
    constexpr Int32 normalDays[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /// key: month, value: cumulative days from Januray to current month(inclusive) if current year is leap year.
    constexpr Int32 cumulativeLeapDays[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

    /// key: month, value: cumulative days from Januray to current month(inclusive) if current year is not leap year.
    constexpr Int32 cumulativeDays[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};

    /// key: year, value: cumulative days from epoch(1970-01-01) to the first day of current year(exclusive).
    constexpr Int32 cumulativeYearDays[]
        = {0,     365,   730,   1096,  1461,  1826,  2191,  2557,  2922,  3287,  3652,  4018,  4383,  4748,  5113,  5479,  5844,  6209,
           6574,  6940,  7305,  7670,  8035,  8401,  8766,  9131,  9496,  9862,  10227, 10592, 10957, 11323, 11688, 12053, 12418, 12784,
           13149, 13514, 13879, 14245, 14610, 14975, 15340, 15706, 16071, 16436, 16801, 17167, 17532, 17897, 18262, 18628, 18993, 19358,
           19723, 20089, 20454, 20819, 21184, 21550, 21915, 22280, 22645, 23011, 23376, 23741, 24106, 24472, 24837, 25202, 25567, 25933,
           26298, 26663, 27028, 27394, 27759, 28124, 28489, 28855, 29220, 29585, 29950, 30316, 30681, 31046, 31411, 31777, 32142, 32507,
           32872, 33238, 33603, 33968, 34333, 34699, 35064, 35429, 35794, 36160, 36525, 36890, 37255, 37621, 37986, 38351, 38716, 39082,
           39447, 39812, 40177, 40543, 40908, 41273, 41638, 42004, 42369, 42734, 43099, 43465, 43830, 44195, 44560, 44926, 45291, 45656,
           46021, 46387, 46752, 47117, 47482, 47847, 48212, 48577, 48942, 49308, 49673};

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

    struct Date
    {
        Int32 year = 1970;
        Int32 month = 1;
        Int32 day = 1;
        std::vector<Int32> day_of_month_values;
        bool is_ad = true; // AD -> true, BC -> false.

        Int32 week = 1; // Week of year based on ISO week date, e.g: 27
        Int32 day_of_week = 1; // Day of week, Monday:1, Tuesday:2, ..., Sunday:7
        bool week_date_format = false;

        Int32 day_of_year = 1;
        std::vector<Int32> day_of_year_values;
        bool day_of_year_format = false;

        bool century_format = false;

        bool is_year_of_era = false; // Year of era cannot be zero or negative.
        bool has_year = false; // Whether year was explicitly specified.

        Int32 hour = 0;
        Int32 minute = 0;
        Int32 second = 0;

        bool is_am = true; // AM -> true, PM -> false
        bool is_clock_hour = false; // Whether most recent hour specifier is clockhour
        bool is_hour_of_half_day = false; // Whether most recent hour specifier is of half day.

        std::optional<Int64> time_zone_offset;

        void reset()
        {
            year = 1970;
            month = 1;
            day = 1;
            day_of_month_values.clear();
            is_ad = true;

            week = 1;
            day_of_week = 1;
            week_date_format = false;

            day_of_year = 1;
            day_of_year_values.clear();
            day_of_year_format = false;

            century_format = false;

            is_year_of_era = false; // Year of era cannot be zero or negative.
            has_year = false; // Whether year was explicitly specified.

            hour = 0;
            minute = 0;
            second = 0;
            is_am = true; // AM -> true, PM -> false
            is_clock_hour = false; // Whether most recent hour specifier is clockhour
            is_hour_of_half_day = false; // Whether most recent hour specifier is of half day.

            time_zone_offset.reset();
        }

        void setCentrury(Int32 century)
        {
            if (century < 19 || century > 21)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for century must be in the range [19, 21]", century);

            century_format = true;
            year = 100 * century;
            has_year = true;
        }

        void setDayOfWeek(Int32 day_of_week_)
        {
            if (day_of_week_ < 1 || day_of_week_ > 7)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for day of week must be in the range [1, 7]", day_of_week_);

            day_of_week = day_of_week_;
            week_date_format = true;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setMonth(Int32 month_)
        {
            if (month_ < 1 || month_ > 12)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for month of year must be in the range [1, 12]", month_);

            month = month_;
            week_date_format = false;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        ALWAYS_INLINE void appendDayOfMonth(Int32 day_of_month)
        {
            if (day_of_month < 1 || day_of_month > 31)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for day of month must be in the range [1, 31]", day_of_month);

            day_of_month_values.push_back(day_of_month);
            day = day_of_month;
            week_date_format = false;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        ALWAYS_INLINE void appendDayOfYear(Int32 day_of_year_)
        {
            if (day_of_year_ < 1 || day_of_year_ > 366)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for day of year must be in the range [1, 366]", day_of_year_);

            day_of_year_values.push_back(day_of_year_);
            day_of_year = day_of_year_;
            day_of_year_format = true;
            week_date_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setYear2(Int32 year_, bool is_year_of_era_ = false, bool is_week_year = false)
        {
            if (year_ >= 70 && year_ < 100)
                year_ += 1900;
            else if (year_ >= 0 && year_ < 70)
                year_ += 2000;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for year2 must be in the range [0, 99]", year_);

            setYear(year_, is_year_of_era_, is_week_year);
        }

        void setYear(Int32 year_, bool is_year_of_era_ = false, bool is_week_year = false)
        {
            if (year_ < minYear || year_ > maxYear)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for year must be in the range [{}, {}]", year_, minYear, maxYear);

            year = year_;
            century_format = false;
            has_year = true;
            is_year_of_era = is_year_of_era_;
            if (is_week_year)
            {
                week_date_format = true;
                day_of_year_format = false;
            }
        }

        void setWeek(Int32 week_)
        {
            if (week_ < 1 || week_ > 53)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for week of week year must be in the range [1, 53]", week_);

            week = week_;
            week_date_format = true;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setMinute(Int32 minute_)
        {
            if (minute_ < 0 || minute_ > 59)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for minute must be in the range [0, 59]", minute_);

            minute = minute_;
        }

        void setSecond(Int32 second_)
        {
            if (second_ < 0 || second_ > 59)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for second must be in the range [0, 59]", second_);

            second = second_;
        }

        void setEra(String & text)
        {
            Poco::toLowerInPlace(text);
            if (text == "ad")
                is_ad = true;
            else if (text == "bc")
                is_ad = false;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown era {}", text);
        }

        ALWAYS_INLINE void setAMPM(String & text)
        {
            Poco::toLowerInPlace(text);
            if (text == "am")
                is_am = true;
            else if (text == "pm")
                is_am = false;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown half day of day: {}", text);
        }

        ALWAYS_INLINE void setHour(Int32 hour_, bool is_hour_of_half_day_ = false, bool is_clock_hour_ = false)
        {
            Int32 max_hour;
            Int32 min_hour;
            Int32 new_hour = hour_;
            if (!is_hour_of_half_day_ && !is_clock_hour_)
            {
                max_hour = 23;
                min_hour = 0;
            }
            else if (!is_hour_of_half_day_ && is_clock_hour_)
            {
                max_hour = 24;
                min_hour = 1;
                new_hour = hour_ % 24;
            }
            else if (is_hour_of_half_day_ && !is_clock_hour_)
            {
                max_hour = 11;
                min_hour = 0;
            }
            else
            {
                max_hour = 12;
                min_hour = 1;
                new_hour = hour_ % 12;
            }

            if (hour_ < min_hour || hour_ > max_hour)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Value {} for hour must be in the range [{}, {}] if_hour_of_half_day={} and is_clock_hour={}",
                    hour,
                    max_hour,
                    min_hour,
                    is_hour_of_half_day_,
                    is_clock_hour_);

            hour = new_hour;
            is_hour_of_half_day = is_hour_of_half_day_;
            is_clock_hour = is_clock_hour_;
        }

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
            res += ",";
            res += "AM:" + std::to_string(is_am);
            return res;
        }

        static bool isLeapYear(Int32 year_) { return year_ % 4 == 0 && (year_ % 100 != 0 || year_ % 400 == 0); }

        static bool isDateValid(Int32 year_, Int32 month_, Int32 day_)
        {
            /// The range of month[1, 12] and day[1, 31] already checked before
            bool leap = isLeapYear(year_);
            return (year_ >= minYear && year_ <= maxYear) && ((leap && day_ <= leapDays[month_]) || (!leap && day_ <= normalDays[month_]));
        }

        static bool isDayOfYearValid(Int32 year_, Int32 day_of_year_)
        {
            /// The range of day_of_year[1, 366] already checked before
            bool leap = isLeapYear(year_);
            return (year_ >= minYear && year_ <= maxYear) && (day_of_year_ <= 365 + (leap ? 1 : 0));
        }

        static Int32 extractISODayOfTheWeek(Int32 days_since_epoch)
        {
            if (days_since_epoch < 0)
            {
                // negative date: start off at 4 and cycle downwards
                return (7 - ((-days_since_epoch + 3) % 7));
            }
            else
            {
                // positive date: start off at 4 and cycle upwards
                return ((days_since_epoch + 3) % 7) + 1;
            }
        }

        static ALWAYS_INLINE Int32 daysSinceEpochFromWeekDate(int32_t week_year_, int32_t week_of_year_, int32_t day_of_week_)
        {
            /// The range of week_of_year[1, 53], day_of_week[1, 7] already checked before
            if (week_year_ < minYear || week_year_ > maxYear)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid week year {}", week_year_);

            Int32 days_since_epoch_of_jan_fourth = daysSinceEpochFromDate(week_year_, 1, 4);
            Int32 first_day_of_week_year = extractISODayOfTheWeek(days_since_epoch_of_jan_fourth);
            return days_since_epoch_of_jan_fourth - (first_day_of_week_year - 1) + 7 * (week_of_year_ - 1) + day_of_week_ - 1;
        }

        static ALWAYS_INLINE Int32 daysSinceEpochFromDayOfYear(Int32 year_, Int32 day_of_year_)
        {
            if (!isDayOfYearValid(year_, day_of_year_))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of year, year:{} day of year:{}", year_, day_of_year_);

            Int32 res = daysSinceEpochFromDate(year_, 1, 1);
            res += day_of_year_ - 1;
            return res;
        }

        static ALWAYS_INLINE Int32 daysSinceEpochFromDate(Int32 year_, Int32 month_, Int32 day_)
        {
            if (!isDateValid(year_, month_, day_))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid date, year:{} month:{} day:{}", year_, month_, day_);

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
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of month, year:{} month:{} day:{}", year, month, d);
            }

            // Ensure all day of year values are valid for ending year value
            for (const auto d : day_of_year_values)
            {
                if (!isDayOfYearValid(year, d))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of year, year:{} day of year:{}", year, d);
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

            /// Time zone is specified in format string.
            if (seconds_since_epoch >= *time_zone_offset)
                seconds_since_epoch -= *time_zone_offset;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Seconds since epoch is negative");

            return seconds_since_epoch;
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

            std::vector<Action> instructions;
            parseFormat(format, instructions);

            auto col_res = ColumnDateTime::create();
            col_res->reserve(input_rows_count);
            auto & data_res = col_res->getData();
            Date date;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                date.reset();

                StringRef str_ref = col_str->getDataAt(i);
                Pos cur = str_ref.data;
                Pos end = str_ref.data + str_ref.size;
                // Date date;
                for (const auto & instruction : instructions)
                    cur = instruction.perform(cur, end, date);

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
        class Action
        {
        private:
            using Func = std::conditional_t<
                parse_syntax == ParseDateTimeTraits::ParseSyntax::MySQL,
                Pos (*)(Pos, Pos, Date &),
                std::function<Pos(Pos, Pos, Date &)>>;
            Func func{};
            std::string func_name;

            std::string literal;

        public:
            explicit Action(Func && func_, const char * func_name_) : func(std::move(func_)), func_name(func_name_) { }

            explicit Action(const String & literal_) : literal(literal_) { }
            explicit Action(String && literal_) : literal(std::move(literal_)) { }

            /// For debug
            [[maybe_unused]] String toString() const
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
                    ensureSpace(cur, end, literal.size(), "required literal size not matched");
                    if (std::string_view(cur, literal.size()) != literal)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Expect literal {} but {} provided", literal, std::string_view(cur, literal.size()));
                    cur += literal.size();
                    return cur;
                }
            }

            template <typename T, bool check_space = true>
            static Pos readNumber2(Pos cur, Pos end, T & res)
            {
                if constexpr (check_space)
                    ensureSpace(cur, end, 2, "readNumber2 requires size >= 2");

                res = (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                return cur;
            }

            template <typename T, bool check_space = true>
            static Pos readNumber3(Pos cur, Pos end, T & res)
            {
                if constexpr (check_space)
                    ensureSpace(cur, end, 3, "readNumber4 requires size >= 3");

                res = (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                return cur;
            }

            template <typename T, bool check_space = true>
            static Pos readNumber4(Pos cur, Pos end, T & res)
            {
                if constexpr (check_space)
                    ensureSpace(cur, end, 4, "readNumber4 requires size >= 4");

                res = (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                return cur;
            }

            static void ensureSpace(Pos cur, Pos end, size_t len, const String & msg)
            {
                if (cur > end || cur + len > end)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to parse because {}", msg);
            }

            template <bool check_space = true>
            static Pos assertChar(Pos cur, Pos end, char ch)
            {
                if constexpr (check_space)
                    ensureSpace(cur, end, 1, "assertChar requires size >= 1");

                if (*cur != ch)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expect char {}, but {} provided", String(ch, 1), String(*cur, 1));

                ++cur;
                return cur;
            }

            static Pos mysqlDayOfWeekTextShort(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 3, "Parsing DayOfWeekTextShort requires size >= 3");

                String text(cur, 3);
                Poco::toLowerInPlace(text);
                auto it = dayOfWeekMap.find(text);
                if (it == dayOfWeekMap.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown day of week short text {}", text);
                date.setDayOfWeek(it->second.second);
                cur += 3;
                return cur;
            }

            static Pos mysqlMonthOfYearTextShort(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 3, "Parsing MonthOfYearTextShort requires size >= 3");

                String text(cur, 3);
                Poco::toLowerInPlace(text);
                auto it = monthMap.find(text);
                if (it == monthMap.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown month of year short text {}", text);

                date.setMonth(it->second.second);
                cur += 3;
                return cur;
            }

            static Pos mysqlMonth(Pos cur, Pos end, Date & date)
            {
                Int32 month;
                cur = readNumber2(cur, end, month);
                date.setMonth(month);
                return cur;
            }

            static Pos mysqlCentury(Pos cur, Pos end, Date & date)
            {
                Int32 century;
                cur = readNumber2(cur, end, century);
                date.setCentrury(century);
                return cur;
            }

            static Pos mysqlDayOfMonth(Pos cur, Pos end, Date & date)
            {
                Int32 day_of_month;
                cur = readNumber2(cur, end, day_of_month);
                date.appendDayOfMonth(day_of_month);
                return cur;
            }

            static Pos mysqlAmericanDate(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 8, "mysqlAmericanDate requires size >= 8");

                Int32 month;
                cur = readNumber2<Int32, false>(cur, end, month);
                cur = assertChar<false>(cur, end, '/');
                date.setMonth(month);

                Int32 day;
                cur = readNumber2<Int32, false>(cur, end, day);
                cur = assertChar<false>(cur, end, '/');
                date.appendDayOfMonth(day);

                Int32 year;
                cur = readNumber2<Int32, false>(cur, end, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlDayOfMonthSpacePadded(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 2, "mysqlDayOfMonthSpacePadded requires size >= 2");

                Int32 day_of_month = *cur == ' ' ? 0 : (*cur - '0');
                ++cur;

                day_of_month = 10 * day_of_month + (*cur - '0');
                ++cur;

                date.appendDayOfMonth(day_of_month);
                return cur;
            }

            static Pos mysqlISO8601Date(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 10, "mysqlISO8601Date requires size >= 10");

                Int32 year;
                Int32 month;
                Int32 day;
                cur = readNumber4<Int32, false>(cur, end, year);
                cur = assertChar<false>(cur, end, '-');
                cur = readNumber2<Int32, false>(cur, end, month);
                cur = assertChar<false>(cur, end, '-');
                cur = readNumber2<Int32, false>(cur, end, day);

                date.setYear(year);
                date.setMonth(month);
                date.appendDayOfMonth(day);
                return cur;
            }

            static Pos mysqlISO8601Year2(Pos cur, Pos end, Date & date)
            {
                Int32 year2;
                cur = readNumber2(cur, end, year2);
                date.setYear2(year2);
                return cur;
            }

            static Pos mysqlISO8601Year4(Pos cur, Pos end, Date & date)
            {
                Int32 year;
                cur = readNumber4(cur, end, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlDayOfYear(Pos cur, Pos end, Date & date)
            {
                Int32 day_of_year;
                cur = readNumber3(cur, end, day_of_year);
                date.appendDayOfYear(day_of_year);
                return cur;
            }

            static Pos mysqlDayOfWeek(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1");
                date.setDayOfWeek(*cur - '0');
                ++cur;
                return cur;
            }

            static Pos mysqlISO8601Week(Pos cur, Pos end, Date & date)
            {
                Int32 week;
                cur = readNumber2(cur, end, week);
                date.setWeek(week);
                return cur;
            }

            static Pos mysqlDayOfWeek0To6(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1");

                Int32 day_of_week = *cur - '0';
                if (day_of_week == 0)
                    day_of_week = 7;

                date.setDayOfWeek(day_of_week);
                ++cur;
                return cur;
            }

            static Pos mysqlDayOfWeekTextLong(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 6, "jodaDayOfWeekText requires the size >= 6");
                String text1(cur, 3);
                Poco::toLowerInPlace(text1);
                auto it = dayOfWeekMap.find(text1);
                if (it == dayOfWeekMap.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown day of week text: {}", text1);
                cur += 3;

                size_t left_size = it->second.first.size();
                ensureSpace(cur, end, left_size, "jodaDayOfWeekText requires the second parg size >= " + std::to_string(left_size));
                String text2(cur, left_size);
                Poco::toLowerInPlace(text2);
                if (text2 != it->second.first)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown day of week text: {}", text1 + text2);
                cur += left_size;

                date.setDayOfWeek(it->second.second);
                return cur;
            }

            static Pos mysqlYear2(Pos cur, Pos end, Date & date)
            {
                Int32 year2;
                cur = readNumber2(cur, end, year2);
                date.setYear2(year2);
                return cur;
            }

            static Pos mysqlYear4(Pos cur, Pos end, Date & date)
            {
                Int32 year;
                cur = readNumber4(cur, end, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlTimezoneOffset(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 5, "mysqlTimezoneOffset requires size >= 5");

                Int32 sign;
                if (*cur == '-')
                    sign = -1;
                else if (*cur == '+')
                    sign = 1;
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown sign time zone offset: {}", std::string_view(cur, 1));
                ++cur;

                Int32 hour;
                cur = readNumber2<Int32, false>(cur, end, hour);

                Int32 minute;
                cur = readNumber2<Int32, false>(cur, end, minute);

                *date.time_zone_offset = sign * (hour * 3600 + minute * 60);
                return cur;
            }

            static Pos mysqlMinute(Pos cur, Pos end, Date & date)
            {
                Int32 minute;
                cur = readNumber2(cur, end, minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos mysqlAMPM(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 2, "mysqlAMPM requires size >= 2");

                String text(cur, 2);
                date.setAMPM(text);
                cur += 2;
                return cur;
            }

            static Pos mysqlHHMM12(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 8, "mysqlHHMM12 requires size >= 8");

                Int32 hour;
                cur = readNumber2<Int32, false>(cur, end, hour);
                cur = assertChar<false>(cur, end, ':');
                date.setHour(hour, true, true);

                Int32 minute;
                cur = readNumber2<Int32, false>(cur, end, minute);
                cur = assertChar<false>(cur, end, ' ');
                date.setMinute(minute);

                cur = mysqlAMPM(cur, end, date);
                return cur;
            }

            static Pos mysqlHHMM24(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 5, "mysqlHHMM24 requires size >= 5");

                Int32 hour;
                cur = readNumber2<Int32, false>(cur, end, hour);
                cur = assertChar<false>(cur, end, ':');
                date.setHour(hour, false, false);

                Int32 minute;
                cur = readNumber2<Int32, false>(cur, end, minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos mysqlSecond(Pos cur, Pos end, Date & date)
            {
                Int32 second;
                cur = readNumber2(cur, end, second);
                date.setSecond(second);
                return cur;
            }

            static Pos mysqlISO8601Time(Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 8, "mysqlISO8601Time requires size >= 8");

                Int32 hour;
                Int32 minute;
                Int32 second;
                cur = readNumber2<Int32, false>(cur, end, hour);
                cur = assertChar<false>(cur, end, ':');
                cur = readNumber2<Int32, false>(cur, end, minute);
                cur = assertChar<false>(cur, end, ':');
                cur = readNumber2<Int32, false>(cur, end, second);

                date.setHour(hour, false, false);
                date.setMinute(minute);
                date.setSecond(second);
                return cur;
            }

            static Pos mysqlHour12(Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumber2(cur, end, hour);
                date.setHour(hour, true, true);
                return cur;
            }

            static Pos mysqlHour24(Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumber2(cur, end, hour);
                date.setHour(hour, false, false);
                return cur;
            }

            static Pos readNumberWithVariableLength(
                Pos cur,
                Pos end,
                bool allow_negative,
                bool allow_plus_sign,
                bool is_year,
                int repetitions,
                int max_digits_consume,
                Int32 & number)
            {
                bool negative = false;
                if (allow_negative && cur < end && *cur == '-')
                {
                    negative = true;
                    ++cur;
                }
                else if (allow_plus_sign && cur < end && *cur == '+')
                {
                    negative = false;
                    ++cur;
                }

                number = 0;
                Pos start = cur;
                if (is_year && repetitions == 2)
                {
                    // If abbreviated two year digit is provided in format string, try to read
                    // in two digits of year and convert to appropriate full length year The
                    // two-digit mapping is as follows: [00, 69] -> [2000, 2069]
                    //                                  [70, 99] -> [1970, 1999]
                    // If more than two digits are provided, then simply read in full year
                    // normally without conversion
                    int count = 0;
                    while (cur < end && cur < start + max_digits_consume && *cur >= '0' && *cur <= '9')
                    {
                        number = number * 10 + (*cur - '0');
                        ++cur;
                        ++count;
                    }
                    if (count == 2)
                    {
                        if (number >= 70)
                            number += 1900;
                        else if (number >= 0 && number < 70)
                            number += 2000;
                    }
                    else
                    {
                        while (cur < end && cur < start + max_digits_consume && *cur >= '0' && *cur <= '9')
                        {
                            number = number * 10 + (*cur - '0');
                            ++cur;
                        }
                    }
                }
                else
                {
                    while (cur < end && cur < start + max_digits_consume && *cur >= '0' and *cur <= '9')
                    {
                        number = number * 10 + (*cur - '0');
                        ++cur;
                    }
                }

                /// Need to have read at least one digit.
                if (cur <= start)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "read number from {} failed", String(cur, end - cur));

                if (negative)
                    number *= -1;

                return cur;
            }

            static Pos jodaEra(int, Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 2, "jodaEra requires size >= 2");

                String era(cur, 2);
                date.setEra(era);
                cur += 2;
                return cur;
            }

            static Pos jodaCenturyOfEra(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 century;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, repetitions, century);
                date.setCentrury(century);
                return cur;
            }

            static Pos jodaYearOfEra(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 year_of_era;
                cur = readNumberWithVariableLength(cur, end, false, false, true, repetitions, repetitions, year_of_era);
                date.setYear(year_of_era, true);
                return cur;
            }

            static Pos jodaWeekYear(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 week_year;
                cur = readNumberWithVariableLength(cur, end, true, true, true, repetitions, repetitions, week_year);
                date.setYear(week_year, false, true);
                return cur;
            }

            static Pos jodaWeekOfWeekYear(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 week;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), week);
                date.setWeek(week);
                return cur;
            }

            static Pos jodaDayOfWeek1Based(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 day_of_week;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, repetitions, day_of_week);
                if (day_of_week < 1 || day_of_week > 7)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} for day of week 1-based must be in the range [1, 7]", day_of_week);

                date.setDayOfWeek(day_of_week);
                return cur;
            }

            static Pos jodaDayOfWeekText(size_t /*min_represent_digits*/, Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 3, "jodaDayOfWeekText requires size >= 3");

                String text1(cur, 3);
                Poco::toLowerInPlace(text1);
                auto it = dayOfWeekMap.find(text1);
                if (it == dayOfWeekMap.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown day of week text: {}", text1);
                cur += 3;
                date.setDayOfWeek(it->second.second);

                size_t left_size = it->second.first.size();
                if (cur + left_size <= end)
                {
                    String text2(cur, left_size);
                    Poco::toLowerInPlace(text2);
                    if (text2 == it->second.first)
                    {
                        cur += left_size;
                        return cur;
                    }
                }
                return cur;
            }

            static Pos jodaYear(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 year;
                cur = readNumberWithVariableLength(cur, end, true, true, true, repetitions, repetitions, year);
                date.setYear(year);
                return cur;
            }

            static Pos jodaDayOfYear(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 day_of_year;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 3), day_of_year);
                date.appendDayOfYear(day_of_year);
                return cur;
            }

            static Pos jodaMonthOfYear(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 month;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, 2, month);
                date.setMonth(month);
                return cur;
            }

            static Pos jodaMonthOfYearText(int, Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 3, "jodaMonthOfYearText requires size >= 3");
                String text1(cur, 3);
                Poco::toLowerInPlace(text1);
                auto it = monthMap.find(text1);
                if (it == monthMap.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown month of year text: {}", text1);
                cur += 3;
                date.setMonth(it->second.second);

                size_t left_size = it->second.first.size();
                if (cur + left_size <= end)
                {
                    String text2(cur, left_size);
                    Poco::toLowerInPlace(text2);
                    if (text2 == it->second.first)
                    {
                        cur += left_size;
                        return cur;
                    }
                }
                return cur;
            }

            static Pos jodaDayOfMonth(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 day_of_month;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), day_of_month);
                date.appendDayOfMonth(day_of_month);
                return cur;
            }

            static Pos jodaHalfDayOfDay(int, Pos cur, Pos end, Date & date)
            {
                ensureSpace(cur, end, 2, "jodaHalfDayOfDay requires size >= 2");

                String text(cur, 2);
                date.setAMPM(text);
                cur += 2;
                return cur;
            }

            static Pos jodaHourOfHalfDay(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), hour);
                date.setHour(hour, true, false);
                return cur;
            }

            static Pos jodaClockHourOfHalfDay(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), hour);
                date.setHour(hour, true, true);
                return cur;
            }

            static Pos jodaHourOfDay(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), hour);
                date.setHour(hour, false, false);
                return cur;
            }

            static Pos jodaClockHourOfDay(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), hour);
                date.setHour(hour, false, true);
                return cur;
            }

            static Pos jodaMinuteOfHour(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 minute;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos jodaSecondOfMinute(int repetitions, Pos cur, Pos end, Date & date)
            {
                Int32 second;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2), second);
                date.setSecond(second);
                return cur;
            }
        };


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
#define ACTION_ARGS(func) &(func), #func

            Pos pos = format.data();
            Pos end = pos + format.size();
            while (true)
            {
                Pos percent_pos = find_first_symbols<'%'>(pos, end);
                if (percent_pos < end)
                {
                    if (pos < percent_pos)
                        instructions.emplace_back(String(pos, percent_pos - pos));

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
                        instructions.emplace_back(String(pos, end - pos));
                    break;
                }
            }
#undef ACTION_ARGS
        }

        void parseJodaFormat(const String & format, std::vector<Action> & instructions) const
        {
#define ACTION_ARGS_WITH_BIND(func, arg) std::bind_front(&(func), (arg)), #func

            // size_t reserve_size = 0;
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
                        instructions.emplace_back(String(cur_token, 1));
                        // ++reserve_size;
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
                                instructions.emplace_back(String(cur_token + i, 1));
                                // ++reserve_size;
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
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaEra, repetitions));
                            break;
                        case 'C':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaCenturyOfEra, repetitions));
                            break;
                        case 'Y':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaYearOfEra, repetitions));
                            break;
                        case 'x':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaWeekYear, repetitions));
                            break;
                        case 'w':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaWeekOfWeekYear, repetitions));
                            break;
                        case 'e':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaDayOfWeek1Based, repetitions));
                            break;
                        case 'E':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaDayOfWeekText, repetitions));
                            break;
                        case 'y':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaYear, repetitions));
                            break;
                        case 'D':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaDayOfYear, repetitions));
                            break;
                        case 'M':
                            if (repetitions <= 2)
                                instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaMonthOfYear, repetitions));
                            else
                                instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaMonthOfYearText, repetitions));
                            break;
                        case 'd':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaDayOfMonth, repetitions));
                            break;
                        case 'a':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaHalfDayOfDay, repetitions));
                            break;
                        case 'K':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaHourOfHalfDay, repetitions));
                            break;
                        case 'h':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaClockHourOfHalfDay, repetitions));
                            break;
                        case 'H':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaHourOfDay, repetitions));
                            break;
                        case 'k':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaClockHourOfDay, repetitions));
                            break;
                        case 'm':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaMinuteOfHour, repetitions));
                            break;
                        case 's':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Action::jodaSecondOfMinute, repetitions));
                            break;
                        case 'S':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for fractional seconds");
                            break;
                        case 'z':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for timezone");
                            break;
                        case 'Z':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for timezone offset id");
                        default:
                            if (isalpha(*cur_token))
                                throw Exception(
                                    ErrorCodes::NOT_IMPLEMENTED, "format is not supported for {}", String(cur_token, repetitions));

                            instructions.emplace_back(String(cur_token, pos - cur_token));
                            break;
                    }
                }
            }
#undef ACTION_ARGS_WITH_BIND
        }


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

    struct NameParseDateTimeInJodaSyntax
    {
        static constexpr auto name = "parseDateTimeInJodaSyntax";
    };


    using FunctionParseDateTime = FunctionParseDateTimeImpl<NameParseDateTime, ParseDateTimeTraits::ParseSyntax::MySQL>;
    using FunctionParseDateTimeInJodaSyntax
        = FunctionParseDateTimeImpl<NameParseDateTimeInJodaSyntax, ParseDateTimeTraits::ParseSyntax::Joda>;
}

REGISTER_FUNCTION(ParseDateTime)
{
    factory.registerFunction<FunctionParseDateTime>();
    factory.registerAlias("TO_UNIXTIME", "parseDateTime");

    factory.registerFunction<FunctionParseDateTimeInJodaSyntax>();
}


}
