#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Functions/numLiteralChars.h>

#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int NOT_ENOUGH_SPACE;
}

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
        {"mar", {"ch", 3}},
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

    /// key: month, value: cumulative days from January to current month(inclusive) if current year is leap year.
    constexpr Int32 cumulativeLeapDays[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

    /// key: month, value: cumulative days from January to current month(inclusive) if current year is not leap year.
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

    struct DateTime
    {
        /// If both week_date_format and week_date_format is false, date is composed of year, month and day
        Int32 year = 1970; /// year, range [1970, 2106]
        Int32 month = 1; /// month of year, range [1, 12]
        Int32 day = 1; /// day of month, range [1, 31]

        Int32 week = 1; /// ISO week of year, range [1, 53]
        Int32 day_of_week = 1; /// day of week, range [1, 7], 1 represents Monday, 2 represents Tuesday...
        bool week_date_format
            = false; /// If true, date is composed of week year(reuse year), week of year(use week) and day of week(use day_of_week)

        Int32 day_of_year = 1; /// day of year, range [1, 366]
        bool day_of_year_format = false; /// If true, date is composed of year(reuse year), day of year(use day_of_year)

        bool is_year_of_era = false; /// If true, year is calculated from era and year of era, the latter cannot be zero or negative.
        bool has_year = false; /// Whether year was explicitly specified.

        /// If hour_starts_at_1 = true, is_hour_of_half_day = true, hour's range is [1, 12]
        /// If hour_starts_at_1 = true, is_hour_of_half_day = false, hour's range is [1, 24]
        /// If hour_starts_at_1 = false, is_hour_of_half_day = true, hour's range is [0, 11]
        /// If hour_starts_at_1 = false, is_hour_of_half_day = false, hour's range is [0, 23]
        Int32 hour = 0;
        Int32 minute = 0; /// range [0, 59]
        Int32 second = 0; /// range [0, 59]

        bool is_am = true; /// If is_hour_of_half_day = true and is_am = false (i.e. pm) then add 12 hours to the result DateTime
        bool hour_starts_at_1 = false; /// Whether the hour is clockhour
        bool is_hour_of_half_day = false; /// Whether the hour is of half day

        bool has_time_zone_offset = false; /// If true, time zone offset is explicitly specified.
        Int64 time_zone_offset = 0; /// Offset in seconds between current timezone to UTC.

        void reset()
        {
            year = 1970;
            month = 1;
            day = 1;

            week = 1;
            day_of_week = 1;
            week_date_format = false;

            day_of_year = 1;
            day_of_year_format = false;

            is_year_of_era = false;
            has_year = false;

            hour = 0;
            minute = 0;
            second = 0;

            is_am = true;
            hour_starts_at_1 = false;
            is_hour_of_half_day = false;

            has_time_zone_offset = false;
            time_zone_offset = 0;
        }

        /// Input text is expected to be lowered by caller
        void setEra(const String & text) // NOLINT
        {
            if (text == "bc")
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Era BC exceeds the range of DateTime");
            else if (text != "ad")
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Unknown era {} (expected 'ad' or 'bc')", text);
        }

        void setCentury(Int32 century)
        {
            if (century < 19 || century > 21)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for century must be in the range [19, 21]", century);

            year = 100 * century;
            has_year = true;
        }

        void setYear(Int32 year_, bool is_year_of_era_ = false, bool is_week_year = false)
        {
            if (year_ < minYear || year_ > maxYear)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for year must be in the range [{}, {}]", year_, minYear, maxYear);

            year = year_;
            has_year = true;
            is_year_of_era = is_year_of_era_;
            if (is_week_year)
            {
                week_date_format = true;
                day_of_year_format = false;
            }
        }

        void setYear2(Int32 year_)
        {
            if (year_ >= 70 && year_ < 100)
                year_ += 1900;
            else if (year_ >= 0 && year_ < 70)
                year_ += 2000;
            else
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for year2 must be in the range [0, 99]", year_);

            setYear(year_, false, false);
        }

        void setMonth(Int32 month_)
        {
            if (month_ < 1 || month_ > 12)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for month of year must be in the range [1, 12]", month_);

            month = month_;
            week_date_format = false;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setWeek(Int32 week_)
        {
            if (week_ < 1 || week_ > 53)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for week of week year must be in the range [1, 53]", week_);

            week = week_;
            week_date_format = true;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setDayOfYear(Int32 day_of_year_)
        {
            if (day_of_year_ < 1 || day_of_year_ > 366)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for day of year must be in the range [1, 366]", day_of_year_);

            day_of_year = day_of_year_;
            day_of_year_format = true;
            week_date_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setDayOfMonth(Int32 day_of_month)
        {
            if (day_of_month < 1 || day_of_month > 31)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for day of month must be in the range [1, 31]", day_of_month);

            day = day_of_month;
            week_date_format = false;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        void setDayOfWeek(Int32 day_of_week_)
        {
            if (day_of_week_ < 1 || day_of_week_ > 7)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for day of week must be in the range [1, 7]", day_of_week_);

            day_of_week = day_of_week_;
            week_date_format = true;
            day_of_year_format = false;
            if (!has_year)
            {
                has_year = true;
                year = 2000;
            }
        }

        /// Input text is expected to be lowered by caller
        void setAMPM(const String & text)
        {
            if (text == "am")
                is_am = true;
            else if (text == "pm")
                is_am = false;
            else
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Unknown half day of day: {}", text);
        }

        void setHour(Int32 hour_, bool is_hour_of_half_day_ = false, bool hour_starts_at_1_ = false)
        {
            Int32 max_hour;
            Int32 min_hour;
            Int32 new_hour = hour_;
            if (!is_hour_of_half_day_ && !hour_starts_at_1_)
            {
                max_hour = 23;
                min_hour = 0;
            }
            else if (!is_hour_of_half_day_ && hour_starts_at_1_)
            {
                max_hour = 24;
                min_hour = 1;
                new_hour = hour_ % 24;
            }
            else if (is_hour_of_half_day_ && !hour_starts_at_1_)
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
                    ErrorCodes::CANNOT_PARSE_DATETIME,
                    "Value {} for hour must be in the range [{}, {}] if_hour_of_half_day={} and hour_starts_at_1={}",
                    hour,
                    max_hour,
                    min_hour,
                    is_hour_of_half_day_,
                    hour_starts_at_1_);

            hour = new_hour;
            is_hour_of_half_day = is_hour_of_half_day_;
            hour_starts_at_1 = hour_starts_at_1_;
        }

        void setMinute(Int32 minute_)
        {
            if (minute_ < 0 || minute_ > 59)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for minute must be in the range [0, 59]", minute_);

            minute = minute_;
        }

        void setSecond(Int32 second_)
        {
            if (second_ < 0 || second_ > 59)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Value {} for second must be in the range [0, 59]", second_);

            second = second_;
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

        static Int32 daysSinceEpochFromWeekDate(int32_t week_year_, int32_t week_of_year_, int32_t day_of_week_)
        {
            /// The range of week_of_year[1, 53], day_of_week[1, 7] already checked before
            if (week_year_ < minYear || week_year_ > maxYear)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Invalid week year {}", week_year_);

            Int32 days_since_epoch_of_jan_fourth = daysSinceEpochFromDate(week_year_, 1, 4);
            Int32 first_day_of_week_year = extractISODayOfTheWeek(days_since_epoch_of_jan_fourth);
            return days_since_epoch_of_jan_fourth - (first_day_of_week_year - 1) + 7 * (week_of_year_ - 1) + day_of_week_ - 1;
        }

        static Int32 daysSinceEpochFromDayOfYear(Int32 year_, Int32 day_of_year_)
        {
            if (!isDayOfYearValid(year_, day_of_year_))
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Invalid day of year, out of range (year: {} day of year: {})", year_, day_of_year_);

            Int32 res = daysSinceEpochFromDate(year_, 1, 1);
            res += day_of_year_ - 1;
            return res;
        }

        static Int32 daysSinceEpochFromDate(Int32 year_, Int32 month_, Int32 day_)
        {
            if (!isDateValid(year_, month_, day_))
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Invalid date, out of range (year: {} month: {} day_of_month: {})", year_, month_, day_);

            Int32 res = cumulativeYearDays[year_ - 1970];
            res += isLeapYear(year_) ? cumulativeLeapDays[month_ - 1] : cumulativeDays[month_ - 1];
            res += day_ - 1;
            return res;
        }

        Int64 buildDateTime(const DateLUTImpl & time_zone)
        {
            if (is_hour_of_half_day && !is_am)
                hour += 12;

            // Convert the parsed date/time into a timestamp.
            Int32 days_since_epoch;
            if (week_date_format)
                days_since_epoch = daysSinceEpochFromWeekDate(year, week, day_of_week);
            else if (day_of_year_format)
                days_since_epoch = daysSinceEpochFromDayOfYear(year, day_of_year);
            else
                days_since_epoch = daysSinceEpochFromDate(year, month, day);

            Int64 seconds_since_epoch = days_since_epoch * 86400UZ + hour * 3600UZ + minute * 60UZ + second;

            /// Time zone is not specified, use local time zone
            if (!has_time_zone_offset)
                time_zone_offset = time_zone.timezoneOffset(seconds_since_epoch);

            /// Time zone is specified in format string.
            if (seconds_since_epoch >= time_zone_offset)
                seconds_since_epoch -= time_zone_offset;
            else
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Seconds since epoch is negative");

            return seconds_since_epoch;
        }
    };

    enum class ParseSyntax
    {
        MySQL,
        Joda
    };

    enum class ErrorHandling
    {
        Exception,
        Zero,
        Null
    };

    /// _FUNC_(str[, format, timezone])
    template <typename Name, ParseSyntax parse_syntax, ErrorHandling error_handling>
    class FunctionParseDateTimeImpl : public IFunction
    {
    public:
        const bool mysql_M_is_month_name;
        const bool mysql_parse_ckl_without_leading_zeros;

        static constexpr auto name = Name::name;
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionParseDateTimeImpl>(context); }

        explicit FunctionParseDateTimeImpl(ContextPtr context)
            : mysql_M_is_month_name(context->getSettings().formatdatetime_parsedatetime_m_is_month_name)
            , mysql_parse_ckl_without_leading_zeros(context->getSettings().parsedatetime_parse_without_leading_zeros)
        {
        }

        String getName() const override { return name; }

        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            FunctionArgumentDescriptors mandatory_args{
                {"time", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
                {"format", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
            };

            FunctionArgumentDescriptors optional_args{
                {"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"}
            };

            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

            String time_zone_name = getTimeZone(arguments).getTimeZone();
            DataTypePtr date_type = std::make_shared<DataTypeDateTime>(time_zone_name);
            if (error_handling == ErrorHandling::Null)
                return std::make_shared<DataTypeNullable>(date_type);
            else
                return date_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
            if (!col_str)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first ('str') argument of function {}. Must be string.",
                    arguments[0].column->getName(),
                    getName());

            String format = getFormat(arguments);
            const auto & time_zone = getTimeZone(arguments);
            std::vector<Instruction> instructions = parseFormat(format);

            auto col_res = ColumnDateTime::create(input_rows_count);

            ColumnUInt8::MutablePtr col_null_map;
            if constexpr (error_handling == ErrorHandling::Null)
                col_null_map = ColumnUInt8::create(input_rows_count, 0);

            auto & res_data = col_res->getData();

            /// Make datetime fit in a cache line.
            alignas(64) DateTime datetime;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                datetime.reset();
                StringRef str_ref = col_str->getDataAt(i);
                Pos cur = str_ref.data;
                Pos end = str_ref.data + str_ref.size;
                bool error = false;

                for (const auto & instruction : instructions)
                {
                    try
                    {
                        cur = instruction.perform(cur, end, datetime);
                    }
                    catch (...)
                    {
                        if constexpr (error_handling == ErrorHandling::Zero)
                        {
                            res_data[i] = 0;
                            error = true;
                            break;
                        }
                        else if constexpr (error_handling == ErrorHandling::Null)
                        {
                            res_data[i] = 0;
                            col_null_map->getData()[i] = 1;
                            error = true;
                            break;
                        }
                        else
                        {
                            static_assert(error_handling == ErrorHandling::Exception);
                            throw;
                        }
                    }
                }

                if (error)
                    continue;

                try
                {
                    /// Ensure all input was consumed
                    if (cur < end)
                        throw Exception(
                            ErrorCodes::CANNOT_PARSE_DATETIME,
                            "Invalid format input {} is malformed at {}",
                            str_ref.toView(),
                            std::string_view(cur, end - cur));
                    Int64 time = datetime.buildDateTime(time_zone);
                    res_data[i] = static_cast<UInt32>(time);
                }
                catch (...)
                {
                    if constexpr (error_handling == ErrorHandling::Zero)
                        res_data[i] = 0;
                    else if constexpr (error_handling == ErrorHandling::Null)
                    {
                        res_data[i] = 0;
                        col_null_map->getData()[i] = 1;
                    }
                    else
                    {
                        static_assert(error_handling == ErrorHandling::Exception);
                        throw;
                    }
                }
            }

            if constexpr (error_handling == ErrorHandling::Null)
                return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
            else
                return col_res;
            }


    private:
        class Instruction
        {
        private:
            enum class NeedCheckSpace
            {
                Yes,
                No
            };

            using Func = std::conditional_t<
                parse_syntax == ParseSyntax::MySQL,
                Pos (*)(Pos, Pos, const String &, DateTime &),
                std::function<Pos(Pos, Pos, const String &, DateTime &)>>;
            const Func func{};
            const String func_name;
            const String literal; /// Only used when current instruction parses literal
            const String fragment; /// Parsed fragments in MySQL or Joda format string

        public:
            explicit Instruction(Func && func_, const char * func_name_, const std::string_view & fragment_)
                : func(std::move(func_)), func_name(func_name_), fragment(fragment_)
            {
            }

            explicit Instruction(const String & literal_) : literal(literal_), fragment("LITERAL") { }
            explicit Instruction(String && literal_) : literal(std::move(literal_)), fragment("LITERAL") { }

            /// For debug
            [[maybe_unused]] String toString() const
            {
                if (func)
                    return "func:" + func_name + ",fragment:" + fragment;
                else
                    return "literal:" + literal + ",fragment:" + fragment;
            }

            Pos perform(Pos cur, Pos end, DateTime & date) const
            {
                if (func)
                    return func(cur, end, fragment, date);
                else
                {
                    /// literal:
                    checkSpace(cur, end, literal.size(), "insufficient space to parse literal", fragment);
                    if (std::string_view(cur, literal.size()) != literal)
                        throw Exception(
                            ErrorCodes::CANNOT_PARSE_DATETIME,
                            "Unable to parse fragment {} from {} because literal {} is expected but {} provided",
                            fragment,
                            std::string_view(cur, end - cur),
                            literal,
                            std::string_view(cur, literal.size()));
                    cur += literal.size();
                    return cur;
                }
            }

            template <typename T, NeedCheckSpace need_check_space>
            static Pos readNumber2(Pos cur, Pos end, [[maybe_unused]] const String & fragment, T & res)
            {
                if constexpr (need_check_space == NeedCheckSpace::Yes)
                    checkSpace(cur, end, 2, "readNumber2 requires size >= 2", fragment);

                res = (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                return cur;
            }

            template <typename T, NeedCheckSpace need_check_space>
            static Pos readNumber3(Pos cur, Pos end, [[maybe_unused]] const String & fragment, T & res)
            {
                if constexpr (need_check_space == NeedCheckSpace::Yes)
                    checkSpace(cur, end, 3, "readNumber4 requires size >= 3", fragment);

                res = (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                res = res * 10 + (*cur - '0');
                ++cur;
                return cur;
            }

            template <typename T, NeedCheckSpace need_check_space>
            static Pos readNumber4(Pos cur, Pos end, [[maybe_unused]] const String & fragment, T & res)
            {
                if constexpr (need_check_space == NeedCheckSpace::Yes)
                    checkSpace(cur, end, 4, "readNumber4 requires size >= 4", fragment);

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

            static void checkSpace(Pos cur, Pos end, size_t len, const String & msg, const String & fragment)
            {
                if (cur > end || cur + len > end) [[unlikely]]
                    throw Exception(
                        ErrorCodes::NOT_ENOUGH_SPACE,
                        "Unable to parse fragment {} from {} because {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        msg);
            }

            template <NeedCheckSpace need_check_space>
            static Pos assertChar(Pos cur, Pos end, char expected, const String & fragment)
            {
                if constexpr (need_check_space == NeedCheckSpace::Yes)
                    checkSpace(cur, end, 1, "assertChar requires size >= 1", fragment);

                if (*cur != expected) [[unlikely]]
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because char {} is expected but {} provided",
                        fragment,
                        std::string_view(cur, end - cur),
                        String(expected, 1),
                        String(*cur, 1));

                ++cur;
                return cur;
            }

            template <NeedCheckSpace need_check_space>
            static Pos assertNumber(Pos cur, Pos end, const String & fragment)
            {
                if constexpr (need_check_space == NeedCheckSpace::Yes)
                    checkSpace(cur, end, 1, "assertChar requires size >= 1", fragment);

                if (*cur < '0' || *cur > '9') [[unlikely]]
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because {} is not a number",
                        fragment,
                        std::string_view(cur, end - cur),
                        String(*cur, 1));

                ++cur;
                return cur;
            }

            static Pos mysqlDayOfWeekTextShort(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 3, "mysqlDayOfWeekTextShort requires size >= 3", fragment);

                String text(cur, 3);
                boost::to_lower(text);
                auto it = dayOfWeekMap.find(text);
                if (it == dayOfWeekMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because of unknown day of week short text {} ",
                        fragment,
                        std::string_view(cur, end - cur),
                        text);
                date.setDayOfWeek(it->second.second);
                cur += 3;
                return cur;
            }

            static Pos mysqlMonthOfYearTextShort(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 3, "mysqlMonthOfYearTextShort requires size >= 3", fragment);

                String text(cur, 3);
                boost::to_lower(text);
                auto it = monthMap.find(text);
                if (it == monthMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because of unknown month of year short text {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text);

                date.setMonth(it->second.second);
                cur += 3;
                return cur;
            }

            static Pos mysqlMonthOfYearTextLong(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 3, "mysqlMonthOfYearTextLong requires size >= 3", fragment);
                String text1(cur, 3);
                boost::to_lower(text1);
                auto it = monthMap.find(text1);
                if (it == monthMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse first part of fragment {} from {} because of unknown month of year text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1);
                cur += 3;

                size_t expected_remaining_size = it->second.first.size();
                checkSpace(cur, end, expected_remaining_size, "mysqlMonthOfYearTextLong requires the second parg size >= " + std::to_string(expected_remaining_size), fragment);
                String text2(cur, expected_remaining_size);
                boost::to_lower(text2);
                if (text2 != it->second.first)
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse second part of fragment {} from {} because of unknown month of year text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1 + text2);
                cur += expected_remaining_size;

                date.setMonth(it->second.second);
                return cur;
            }

            static Pos mysqlMonth(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 month;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, month);
                date.setMonth(month);
                return cur;
            }

            static Pos mysqlMonthWithoutLeadingZero(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 month;
                cur = readNumberWithVariableLength(cur, end, false, false, false, 1, 2, fragment, month);
                date.setMonth(month);
                return cur;
            }

            static Pos mysqlCentury(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 century;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, century);
                date.setCentury(century);
                return cur;
            }

            static Pos mysqlDayOfMonth(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 day_of_month;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, day_of_month);
                date.setDayOfMonth(day_of_month);
                return cur;
            }

            static Pos mysqlAmericanDate(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 8, "mysqlAmericanDate requires size >= 8", fragment);

                Int32 month;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, month);
                cur = assertChar<NeedCheckSpace::No>(cur, end, '/', fragment);
                date.setMonth(month);

                Int32 day;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, day);
                cur = assertChar<NeedCheckSpace::No>(cur, end, '/', fragment);
                date.setDayOfMonth(day);

                Int32 year;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlDayOfMonthSpacePadded(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 2, "mysqlDayOfMonthSpacePadded requires size >= 2", fragment);

                Int32 day_of_month = *cur == ' ' ? 0 : (*cur - '0');
                ++cur;

                day_of_month = 10 * day_of_month + (*cur - '0');
                ++cur;

                date.setDayOfMonth(day_of_month);
                return cur;
            }

            static Pos mysqlISO8601Date(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 10, "mysqlISO8601Date requires size >= 10", fragment);

                Int32 year;
                Int32 month;
                Int32 day;
                cur = readNumber4<Int32, NeedCheckSpace::No>(cur, end, fragment, year);
                cur = assertChar<NeedCheckSpace::No>(cur, end, '-', fragment);
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, month);
                cur = assertChar<NeedCheckSpace::No>(cur, end, '-', fragment);
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, day);

                date.setYear(year);
                date.setMonth(month);
                date.setDayOfMonth(day);
                return cur;
            }

            static Pos mysqlISO8601Year2(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year2;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, year2);
                date.setYear2(year2);
                return cur;
            }

            static Pos mysqlISO8601Year4(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year;
                cur = readNumber4<Int32, NeedCheckSpace::Yes>(cur, end, fragment, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlDayOfYear(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 day_of_year;
                cur = readNumber3<Int32, NeedCheckSpace::Yes>(cur, end, fragment, day_of_year);
                date.setDayOfYear(day_of_year);
                return cur;
            }

            static Pos mysqlDayOfWeek(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1", fragment);
                date.setDayOfWeek(*cur - '0');
                ++cur;
                return cur;
            }

            static Pos mysqlISO8601Week(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 week;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, week);
                date.setWeek(week);
                return cur;
            }

            static Pos mysqlDayOfWeek0To6(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 1, "mysqlDayOfWeek requires size >= 1", fragment);

                Int32 day_of_week = *cur - '0';
                if (day_of_week == 0)
                    day_of_week = 7;

                date.setDayOfWeek(day_of_week);
                ++cur;
                return cur;
            }

            static Pos mysqlDayOfWeekTextLong(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 6, "mysqlDayOfWeekTextLong requires size >= 6", fragment);
                String text1(cur, 3);
                boost::to_lower(text1);
                auto it = dayOfWeekMap.find(text1);
                if (it == dayOfWeekMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse first part of fragment {} from {} because of unknown day of week text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1);
                cur += 3;

                size_t expected_remaining_size = it->second.first.size();
                checkSpace(cur, end, expected_remaining_size, "mysqlDayOfWeekTextLong requires the second parg size >= " + std::to_string(expected_remaining_size), fragment);
                String text2(cur, expected_remaining_size);
                boost::to_lower(text2);
                if (text2 != it->second.first)
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse second part of fragment {} from {} because of unknown day of week text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1 + text2);
                cur += expected_remaining_size;

                date.setDayOfWeek(it->second.second);
                return cur;
            }

            static Pos mysqlYear2(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year2;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, year2);
                date.setYear2(year2);
                return cur;
            }

            static Pos mysqlYear4(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year;
                cur = readNumber4<Int32, NeedCheckSpace::Yes>(cur, end, fragment, year);
                date.setYear(year);
                return cur;
            }

            static Pos mysqlTimezoneOffset(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 5, "mysqlTimezoneOffset requires size >= 5", fragment);

                Int32 sign;
                if (*cur == '-')
                    sign = -1;
                else if (*cur == '+')
                    sign = 1;
                else
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because of unknown sign time zone offset: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        std::string_view(cur, 1));
                ++cur;

                Int32 hour;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, hour);

                Int32 minute;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, minute);

                date.has_time_zone_offset = true;
                date.time_zone_offset = sign * (hour * 3600 + minute * 60);
                return cur;
            }

            static Pos mysqlMinute(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 minute;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos mysqlAMPM(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 2, "mysqlAMPM requires size >= 2", fragment);

                String text(cur, 2);
                boost::to_lower(text);
                date.setAMPM(text);
                cur += 2;
                return cur;
            }

            static Pos mysqlHHMM12(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 8, "mysqlHHMM12 requires size >= 8", fragment);

                Int32 hour;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, hour);
                cur = assertChar<NeedCheckSpace::No>(cur, end, ':', fragment);
                date.setHour(hour, true, true);

                Int32 minute;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, minute);
                cur = assertChar<NeedCheckSpace::No>(cur, end, ' ', fragment);
                date.setMinute(minute);

                cur = mysqlAMPM(cur, end, fragment, date);
                return cur;
            }

            static Pos mysqlHHMM24(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 5, "mysqlHHMM24 requires size >= 5", fragment);

                Int32 hour;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, hour);
                cur = assertChar<NeedCheckSpace::No>(cur, end, ':', fragment);
                date.setHour(hour, false, false);

                Int32 minute;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos mysqlSecond(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 second;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, second);
                date.setSecond(second);
                return cur;
            }

            static Pos mysqlMicrosecond(Pos cur, Pos end, const String & fragment, DateTime & /*date*/)
            {
                checkSpace(cur, end, 6, "mysqlMicrosecond requires size >= 6", fragment);

                for (size_t i = 0; i < 6; ++i)
                    cur = assertNumber<NeedCheckSpace::No>(cur, end, fragment);

                return cur;
            }

            static Pos mysqlISO8601Time(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 8, "mysqlISO8601Time requires size >= 8", fragment);

                Int32 hour;
                Int32 minute;
                Int32 second;
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, hour);
                cur = assertChar<NeedCheckSpace::No>(cur, end, ':', fragment);
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, minute);
                cur = assertChar<NeedCheckSpace::No>(cur, end, ':', fragment);
                cur = readNumber2<Int32, NeedCheckSpace::No>(cur, end, fragment, second);

                date.setHour(hour, false, false);
                date.setMinute(minute);
                date.setSecond(second);
                return cur;
            }

            static Pos mysqlHour12(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, hour);
                date.setHour(hour, true, true);
                return cur;
            }

            static Pos mysqlHour12WithoutLeadingZero(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, 1, 2, fragment, hour);
                date.setHour(hour, true, true);
                return cur;
            }

            static Pos mysqlHour24(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumber2<Int32, NeedCheckSpace::Yes>(cur, end, fragment, hour);
                date.setHour(hour, false, false);
                return cur;
            }

            static Pos mysqlHour24WithoutLeadingZero(Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, 1, 2, fragment, hour);
                date.setHour(hour, false, false);
                return cur;
            }

            static Pos readNumberWithVariableLength(
                Pos cur,
                Pos end,
                bool allow_negative,
                bool allow_plus_sign,
                bool is_year,
                size_t repetitions,
                size_t max_digits_to_read,
                const String & fragment,
                Int32 & result)
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

                Int64 number = 0;
                const Pos start = cur;

                /// Avoid integer overflow in (*)
                if (max_digits_to_read >= std::numeric_limits<decltype(number)>::digits10) [[unlikely]]
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because max_digits_to_read is too big",
                        fragment,
                        std::string_view(start, cur - start));

                if (is_year && repetitions == 2)
                {
                    // If abbreviated two year digit is provided in format string, try to read
                    // in two digits of year and convert to appropriate full length year The
                    // two-digit mapping is as follows: [00, 69] -> [2000, 2069]
                    //                                  [70, 99] -> [1970, 1999]
                    // If more than two digits are provided, then simply read in full year
                    // normally without conversion
                    size_t count = 0;
                    while (cur < end && cur < start + max_digits_to_read && *cur >= '0' && *cur <= '9')
                    {
                        number = number * 10 + (*cur - '0'); /// (*)
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
                        while (cur < end && cur < start + max_digits_to_read && *cur >= '0' && *cur <= '9')
                        {
                            number = number * 10 + (*cur - '0'); /// (*)
                            ++cur;
                        }
                    }
                }
                else
                {
                    while (cur < end && cur < start + max_digits_to_read && *cur >= '0' && *cur <= '9')
                    {
                        number = number * 10 + (*cur - '0');
                        ++cur;
                    }
                }

                if (negative)
                    number *= -1;

                /// Need to have read at least one digit.
                if (cur == start) [[unlikely]]
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because read number failed",
                        fragment,
                        std::string_view(cur, end - cur));

                /// Check if number exceeds the range of Int32
                if (number < std::numeric_limits<Int32>::min() || number > std::numeric_limits<Int32>::max()) [[unlikely]]
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because number is out of range of Int32",
                        fragment,
                        std::string_view(start, cur - start));

                result = static_cast<Int32>(number);

                return cur;
            }

            static Pos jodaEra(int, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 2, "jodaEra requires size >= 2", fragment);

                String era(cur, 2);
                boost::to_lower(era);
                date.setEra(era);
                cur += 2;
                return cur;
            }

            static Pos jodaCenturyOfEra(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 century;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, repetitions, fragment, century);
                date.setCentury(century);
                return cur;
            }

            static Pos jodaYearOfEra(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year_of_era;
                cur = readNumberWithVariableLength(cur, end, false, false, true, repetitions, repetitions, fragment, year_of_era);
                date.setYear(year_of_era, true);
                return cur;
            }

            static Pos jodaWeekYear(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 week_year;
                cur = readNumberWithVariableLength(cur, end, true, true, true, repetitions, repetitions, fragment, week_year);
                date.setYear(week_year, false, true);
                return cur;
            }

            static Pos jodaWeekOfWeekYear(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 week;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, week);
                date.setWeek(week);
                return cur;
            }

            static Pos jodaDayOfWeek1Based(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 day_of_week;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, repetitions, fragment, day_of_week);
                date.setDayOfWeek(day_of_week);
                return cur;
            }

            static Pos
            jodaDayOfWeekText(size_t /*min_represent_digits*/, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 3, "jodaDayOfWeekText requires size >= 3", fragment);

                String text1(cur, 3);
                boost::to_lower(text1);
                auto it = dayOfWeekMap.find(text1);
                if (it == dayOfWeekMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because of unknown day of week text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1);
                cur += 3;
                date.setDayOfWeek(it->second.second);

                size_t expected_remaining_size = it->second.first.size();
                if (cur + expected_remaining_size <= end)
                {
                    String text2(cur, expected_remaining_size);
                    boost::to_lower(text2);
                    if (text2 == it->second.first)
                    {
                        cur += expected_remaining_size;
                        return cur;
                    }
                }
                return cur;
            }

            static Pos jodaYear(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 year;
                cur = readNumberWithVariableLength(cur, end, true, true, true, repetitions, repetitions, fragment, year);
                date.setYear(year);
                return cur;
            }

            static Pos jodaDayOfYear(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 day_of_year;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 3uz), fragment, day_of_year);
                date.setDayOfYear(day_of_year);
                return cur;
            }

            static Pos jodaMonthOfYear(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 month;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, 2, fragment, month);
                date.setMonth(month);
                return cur;
            }

            static Pos jodaMonthOfYearText(int, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 3, "jodaMonthOfYearText requires size >= 3", fragment);
                String text1(cur, 3);
                boost::to_lower(text1);
                auto it = monthMap.find(text1);
                if (it == monthMap.end())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_DATETIME,
                        "Unable to parse fragment {} from {} because of unknown month of year text: {}",
                        fragment,
                        std::string_view(cur, end - cur),
                        text1);
                cur += 3;
                date.setMonth(it->second.second);

                size_t expected_remaining_size = it->second.first.size();
                if (cur + expected_remaining_size <= end)
                {
                    String text2(cur, expected_remaining_size);
                    boost::to_lower(text2);
                    if (text2 == it->second.first)
                    {
                        cur += expected_remaining_size;
                        return cur;
                    }
                }
                return cur;
            }

            static Pos jodaDayOfMonth(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 day_of_month;
                cur = readNumberWithVariableLength(
                    cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, day_of_month);
                date.setDayOfMonth(day_of_month);
                return cur;
            }

            static Pos jodaHalfDayOfDay(int, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                checkSpace(cur, end, 2, "jodaHalfDayOfDay requires size >= 2", fragment);

                String text(cur, 2);
                boost::to_lower(text);
                date.setAMPM(text);
                cur += 2;
                return cur;
            }

            static Pos jodaHourOfHalfDay(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, hour);
                date.setHour(hour, true, false);
                return cur;
            }

            static Pos jodaClockHourOfHalfDay(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, hour);
                date.setHour(hour, true, true);
                return cur;
            }

            static Pos jodaHourOfDay(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, hour);
                date.setHour(hour, false, false);
                return cur;
            }

            static Pos jodaClockHourOfDay(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 hour;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, hour);
                date.setHour(hour, false, true);
                return cur;
            }

            static Pos jodaMinuteOfHour(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 minute;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, minute);
                date.setMinute(minute);
                return cur;
            }

            static Pos jodaSecondOfMinute(size_t repetitions, Pos cur, Pos end, const String & fragment, DateTime & date)
            {
                Int32 second;
                cur = readNumberWithVariableLength(cur, end, false, false, false, repetitions, std::max(repetitions, 2uz), fragment, second);
                date.setSecond(second);
                return cur;
            }
        };

        std::vector<Instruction> parseFormat(const String & format) const
        {
            static_assert(
                parse_syntax == ParseSyntax::MySQL || parse_syntax == ParseSyntax::Joda,
                "parse syntax must be one of MySQL or Joda");

            if constexpr (parse_syntax == ParseSyntax::MySQL)
                return parseMysqlFormat(format);
            else
                return parseJodaFormat(format);
        }

        std::vector<Instruction> parseMysqlFormat(const String & format) const
        {
#define ACTION_ARGS(func) &(func), #func, std::string_view(pos - 1, 2)

            Pos pos = format.data();
            Pos end = format.data() + format.size();

            std::vector<Instruction> instructions;
            while (true)
            {
                Pos next_percent_pos = find_first_symbols<'%'>(pos, end);

                if (next_percent_pos < end)
                {
                    if (pos < next_percent_pos)
                        instructions.emplace_back(String(pos, next_percent_pos - pos));

                    pos = next_percent_pos + 1;
                    if (pos >= end)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "'%' must not be the last character in the format string, use '%%' instead");

                    switch (*pos)
                    {
                        // Abbreviated weekday [Mon...Sun]
                        case 'a':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfWeekTextShort));
                            break;

                        // Abbreviated month [Jan...Dec]
                        case 'b':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMonthOfYearTextShort));
                            break;

                        // Month as a decimal number:
                        // - if parsedatetime_parse_without_leading_zeros = true: possibly without leading zero, i.e. 1-12
                        // - else: with leading zero required, i.e. 01-12
                        case 'c':
                            if (mysql_parse_ckl_without_leading_zeros)
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMonthWithoutLeadingZero));
                            else
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMonth));
                            break;

                        // Year, divided by 100, zero-padded
                        case 'C':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlCentury));
                            break;

                        // Day of month, zero-padded (01-31)
                        case 'd':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfMonth));
                            break;

                        // Short MM/DD/YY date, equivalent to %m/%d/%y
                        case 'D':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlAmericanDate));
                            break;

                        // Day of month, space-padded ( 1-31)  23
                        case 'e':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfMonthSpacePadded));
                            break;

                        // Fractional seconds
                        case 'f':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMicrosecond));
                            break;

                        // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                        case 'F':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlISO8601Date));
                            break;

                        // Last two digits of year of ISO 8601 week number (see %G)
                        case 'g':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlISO8601Year2));
                            break;

                        // Year of ISO 8601 week number (see %V)
                        case 'G':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlISO8601Year4));
                            break;

                        // Day of the year (001-366)   235
                        case 'j':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfYear));
                            break;

                        // Month as a decimal number (01-12)
                        case 'm':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMonth));
                            break;

                        // ISO 8601 weekday as number with Monday as 1 (1-7)
                        case 'u':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfWeek));
                            break;

                        // ISO 8601 week number (01-53)
                        case 'V':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlISO8601Week));
                            break;

                        // Weekday as a integer number with Sunday as 0 (0-6)  4
                        case 'w':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfWeek0To6));
                            break;

                        // Full weekday [Monday...Sunday]
                        case 'W':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlDayOfWeekTextLong));
                            break;

                        // Two digits year
                        case 'y':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlYear2));
                            break;

                        // Four digits year
                        case 'Y':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlYear4));
                            break;

                        // Quarter (1-4)
                        case 'Q':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for quarter");
                            break;

                        // Offset from UTC timezone as +hhmm or -hhmm
                        case 'z':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlTimezoneOffset));
                            break;

                        // Depending on a setting
                        // - Full month [January...December]
                        // - Minute (00-59) OR
                        case 'M':
                            if (mysql_M_is_month_name)
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMonthOfYearTextLong));
                            else
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMinute));
                            break;

                        // AM or PM
                        case 'p':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlAMPM));
                            break;

                        // 12-hour HH:MM time, equivalent to %h:%i %p 2:55 PM
                        case 'r':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHHMM12));
                            break;

                        // 24-hour HH:MM time, equivalent to %H:%i 14:55
                        case 'R':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHHMM24));
                            break;

                        // Seconds
                        case 's':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlSecond));
                            break;

                        // Seconds
                        case 'S':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlSecond));
                            break;

                        // ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S 14:55:02
                        case 'T':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlISO8601Time));
                            break;

                        // Hour in 12h format (01-12)
                        case 'h':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour12));
                            break;

                        // Hour in 24h format (00-23)
                        case 'H':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour24));
                            break;

                        // Minute of hour range [0, 59]
                        case 'i':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlMinute));
                            break;

                        // Hour in 12h format (01-12)
                        case 'I':
                            instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour12));
                            break;

                        // Hour in 24h format:
                        // - if parsedatetime_parse_without_leading_zeros = true, possibly without leading zero: i.e. 0-23
                        // - else with leading zero required: i.e. 00-23
                        case 'k':
                            if (mysql_parse_ckl_without_leading_zeros)
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour24WithoutLeadingZero));
                            else
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour24));
                            break;

                        // Hour in 12h format:
                        // - if parsedatetime_parse_without_leading_zeros = true: possibly without leading zero, i.e. 0-12
                        // - else with leading zero required: i.e. 00-12
                        case 'l':
                            if (mysql_parse_ckl_without_leading_zeros)
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour12WithoutLeadingZero));
                            else
                                instructions.emplace_back(ACTION_ARGS(Instruction::mysqlHour12));
                            break;

                        case 't':
                            instructions.emplace_back("\t");
                            break;

                        case 'n':
                            instructions.emplace_back("\n");
                            break;

                        // Escaped literal characters.
                        case '%':
                            instructions.emplace_back("%");
                            break;

                        /// Unimplemented

                        /// Fractional seconds
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
                    /// Handle characters after last %
                    if (pos < end)
                        instructions.emplace_back(String(pos, end - pos));
                    break;
                }
            }
            return instructions;
#undef ACTION_ARGS
        }

        std::vector<Instruction> parseJodaFormat(const String & format) const
        {
#define ACTION_ARGS_WITH_BIND(func, arg) std::bind_front(&(func), (arg)), #func, std::string_view(cur_token, repetitions)

            Pos pos = format.data();
            Pos end = format.data() + format.size();

            std::vector<Instruction> instructions;
            while (pos < end)
            {
                Pos cur_token = pos;

                // Literal case
                if (*cur_token == '\'')
                {
                    // Case 1: 2 consecutive single quote
                    if (pos + 1 < end && *(pos + 1) == '\'')
                    {
                        instructions.emplace_back(String(cur_token, 1));
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
                                if (*(cur_token + i) == '\'')
                                    i += 1;
                            }
                            pos += count + 2;
                        }
                    }
                }
                else
                {
                    size_t repetitions = 1;
                    ++pos;
                    while (pos < end && *cur_token == *pos)
                    {
                        ++repetitions;
                        ++pos;
                    }
                    switch (*cur_token)
                    {
                        case 'G':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaEra, repetitions));
                            break;
                        case 'C':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaCenturyOfEra, repetitions));
                            break;
                        case 'Y':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaYearOfEra, repetitions));
                            break;
                        case 'x':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaWeekYear, repetitions));
                            break;
                        case 'w':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaWeekOfWeekYear, repetitions));
                            break;
                        case 'e':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaDayOfWeek1Based, repetitions));
                            break;
                        case 'E':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaDayOfWeekText, repetitions));
                            break;
                        case 'y':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaYear, repetitions));
                            break;
                        case 'D':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaDayOfYear, repetitions));
                            break;
                        case 'M':
                            if (repetitions <= 2)
                                instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaMonthOfYear, repetitions));
                            else
                                instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaMonthOfYearText, repetitions));
                            break;
                        case 'd':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaDayOfMonth, repetitions));
                            break;
                        case 'a':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaHalfDayOfDay, repetitions));
                            break;
                        case 'K':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaHourOfHalfDay, repetitions));
                            break;
                        case 'h':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaClockHourOfHalfDay, repetitions));
                            break;
                        case 'H':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaHourOfDay, repetitions));
                            break;
                        case 'k':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaClockHourOfDay, repetitions));
                            break;
                        case 'm':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaMinuteOfHour, repetitions));
                            break;
                        case 's':
                            instructions.emplace_back(ACTION_ARGS_WITH_BIND(Instruction::jodaSecondOfMinute, repetitions));
                            break;
                        case 'S':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for fractional seconds");
                        case 'z':
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "format is not supported for timezone");
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
            return instructions;
#undef ACTION_ARGS_WITH_BIND
        }


        String getFormat(const ColumnsWithTypeAndName & arguments) const
        {
            const auto * format_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            if (!format_column)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of second ('format') argument of function {}. Must be constant string.",
                    arguments[1].column->getName(),
                    getName());
            return format_column->getValue<String>();
        }

        const DateLUTImpl & getTimeZone(const ColumnsWithTypeAndName & arguments) const
        {
            if (arguments.size() < 3)
                return DateLUT::instance();

            const auto * col = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
            if (!col)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of third ('timezone') argument of function {}. Must be constant String.",
                    arguments[2].column->getName(),
                    getName());

            String time_zone = col->getValue<String>();
            return DateLUT::instance(time_zone);
        }
    };

    struct NameParseDateTime
    {
        static constexpr auto name = "parseDateTime";
    };

    struct NameParseDateTimeOrZero
    {
        static constexpr auto name = "parseDateTimeOrZero";
    };

    struct NameParseDateTimeOrNull
    {
        static constexpr auto name = "parseDateTimeOrNull";
    };

    struct NameParseDateTimeInJodaSyntax
    {
        static constexpr auto name = "parseDateTimeInJodaSyntax";
    };

    struct NameParseDateTimeInJodaSyntaxOrZero
    {
        static constexpr auto name = "parseDateTimeInJodaSyntaxOrZero";
    };

    struct NameParseDateTimeInJodaSyntaxOrNull
    {
        static constexpr auto name = "parseDateTimeInJodaSyntaxOrNull";
    };

    using FunctionParseDateTime = FunctionParseDateTimeImpl<NameParseDateTime, ParseSyntax::MySQL, ErrorHandling::Exception>;
    using FunctionParseDateTimeOrZero = FunctionParseDateTimeImpl<NameParseDateTimeOrZero, ParseSyntax::MySQL, ErrorHandling::Zero>;
    using FunctionParseDateTimeOrNull = FunctionParseDateTimeImpl<NameParseDateTimeOrNull, ParseSyntax::MySQL, ErrorHandling::Null>;
    using FunctionParseDateTimeInJodaSyntax = FunctionParseDateTimeImpl<NameParseDateTimeInJodaSyntax, ParseSyntax::Joda, ErrorHandling::Exception>;
    using FunctionParseDateTimeInJodaSyntaxOrZero = FunctionParseDateTimeImpl<NameParseDateTimeInJodaSyntaxOrZero, ParseSyntax::Joda, ErrorHandling::Zero>;
    using FunctionParseDateTimeInJodaSyntaxOrNull = FunctionParseDateTimeImpl<NameParseDateTimeInJodaSyntaxOrNull, ParseSyntax::Joda, ErrorHandling::Null>;
}

REGISTER_FUNCTION(ParseDateTime)
{
    factory.registerFunction<FunctionParseDateTime>();
    factory.registerAlias("TO_UNIXTIME", FunctionParseDateTime::name, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionParseDateTimeOrZero>();
    factory.registerFunction<FunctionParseDateTimeOrNull>();
    factory.registerAlias("str_to_date", FunctionParseDateTimeOrNull::name, FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionParseDateTimeInJodaSyntax>();
    factory.registerFunction<FunctionParseDateTimeInJodaSyntaxOrZero>();
    factory.registerFunction<FunctionParseDateTimeInJodaSyntaxOrNull>();
}


}
