#include <gtest/gtest.h>

#include <common/DateLUT.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace
{
using namespace DB;

struct DateTime64WithScale
{
    DateTime64 value;
    UInt32 scale;
};

template <typename ValueType>
auto getTypeName(const ValueType &)
{
    if constexpr (std::is_same_v<ValueType, DayNum>)
    {
        return "DayNum";
    }
    else if constexpr (std::is_same_v<ValueType, time_t>)
    {
        return "time_t";
    }
    else if constexpr (std::is_same_v<ValueType, DateTime64WithScale>)
    {
        return "DateTime64WithScale";
    }
    else
    {
        static_assert("unsupported ValueType");
    }
}

std::ostream & dump_datetime(std::ostream & ostr, const DayNum & d)
{
    return ostr << getTypeName(d) << "{" << d.toUnderType() << "}";
}

std::ostream & dump_datetime(std::ostream & ostr, const time_t & dt)
{
    return ostr << getTypeName(dt) << "{" << dt << "}";
}

std::ostream & dump_datetime(std::ostream & ostr, const DateTime64WithScale & dt64)
{
    return ostr << getTypeName(dt64) << "{" << dt64.value.value << ", scale: " << dt64.scale << "}";
}

template <typename ValueType>
struct DateTimeToStringParamTestCase
{
    const char* description;
    const ValueType input;
    const char* expected;
    const char* timezone = "UTC";
};

template <typename T>
std::ostream & operator << (std::ostream & ostr, const DateTimeToStringParamTestCase<T> & test_case)
{
    ostr << "DateTimeToStringParamTestCase<" << getTypeName(test_case.input) << ">{"
                << "\n\t\"" << test_case.description << "\""
                << "\n\tinput : ";
    dump_datetime(ostr, test_case.input)
                << "\n\texpected : " << test_case.expected
                << "\n\ttimezone : " << test_case.timezone
                << "\n}";
    return ostr;
}

}

TEST(DateTimeToStringTest, RFC1123)
{
    using namespace DB;
    WriteBufferFromOwnString out;
    writeDateTimeTextRFC1123(1111111111, out, DateLUT::instance("UTC"));
    ASSERT_EQ(out.str(), "Fri, 18 Mar 2005 01:58:31 GMT");
}

template <typename ValueType>
class DateTimeToStringParamTestBase : public ::testing::TestWithParam<DateTimeToStringParamTestCase<ValueType>>
{
public:
    void test(const DateTimeToStringParamTestCase<ValueType> & param)
    {
        [[maybe_unused]] const auto & [description, input, expected, timezone_name] = param;

        using namespace DB;
        WriteBufferFromOwnString out;

        if constexpr (std::is_same_v<ValueType, DayNum>)
        {
            writeDateText(input, out);
        }
        else if constexpr (std::is_same_v<ValueType, time_t>)
        {
            writeDateTimeText(input, out, DateLUT::instance(timezone_name));
        }
        else if constexpr (std::is_same_v<ValueType, DateTime64WithScale>)
        {
            writeDateTimeText(input.value, input.scale, out, DateLUT::instance(timezone_name));
        }
        else
        {
            static_assert("unsupported ValueType");
        }

        ASSERT_EQ(expected, out.str());
    }
};

class DateTimeToStringParamTestDayNum : public DateTimeToStringParamTestBase<DayNum>
{};

TEST_P(DateTimeToStringParamTestDayNum, writeDateText)
{
    ASSERT_NO_FATAL_FAILURE(test(GetParam()));
}

class DateTimeToStringParamTestTimeT : public DateTimeToStringParamTestBase<time_t>
{};

TEST_P(DateTimeToStringParamTestTimeT, writeDateText)
{
    ASSERT_NO_FATAL_FAILURE(test(GetParam()));
}

class DateTimeToStringParamTestDateTime64 : public DateTimeToStringParamTestBase<DateTime64WithScale>
{};

TEST_P(DateTimeToStringParamTestDateTime64, writeDateText)
{
    ASSERT_NO_FATAL_FAILURE(test(GetParam()));
}

static const Int32 NON_ZERO_TIME_T = 10 * 365 * 3600 * 24 + 123456; /// NOTE This arithmetic is obviously wrong but it's ok for test.

INSTANTIATE_TEST_SUITE_P(DateTimeToString, DateTimeToStringParamTestDayNum,
    ::testing::ValuesIn(std::initializer_list<DateTimeToStringParamTestCase<DayNum>>
    {
        {
            "Zero DayNum pointing to 1970-01-01",
            DayNum(0),
            "1970-01-01"
        },
        {
            "Non-Zero DayNum",
            DayNum(1),
            "1970-01-02"
        },
        {
            "Non-Zero DayNum",
            DayNum(10 * 365),
            "1979-12-30"
        },
        {
            "Negative DayNum value wraps as if it was UInt16 due to LUT limitations and to maintain compatibility with existing code.",
            DayNum(-10 * 365),
            "2106-02-07"
        },
    })
);

INSTANTIATE_TEST_SUITE_P(DateTimeToString, DateTimeToStringParamTestTimeT,
    ::testing::ValuesIn(std::initializer_list<DateTimeToStringParamTestCase<time_t>>
    {
        {
            "Zero time_t pointing to 1970-01-01 00:00:00 in UTC",
            time_t(0),
            "1970-01-01 00:00:00"
        },
        {
            "Non-Zero time_t is a valid date/time",
            time_t{NON_ZERO_TIME_T},
            "1979-12-31 10:17:36"
        },
//        { // Negative time_t value produces (expectedly) bogus results,
//          // and there is no reliable way to verify output values on all platforms and configurations
//          // (since part of stacktrace is printed), so this test case is disabled.
//            "Negative time_t value wraps as if it was UInt32 due to LUT limitations.",
//            time_t(-1LL * 365 * 3600 * 24),
//            "2006-03-03 06:28:16"
//        },
    })
);

INSTANTIATE_TEST_SUITE_P(DateTimeToString, DateTimeToStringParamTestDateTime64,
    ::testing::ValuesIn(std::initializer_list<DateTimeToStringParamTestCase<DateTime64WithScale>>
    {
        /// Inside basic LUT boundaries
        {
            "Zero DateTime64 with scale 0 string representation matches one of zero time_t",
            DateTime64WithScale{0, 0},
            "1970-01-01 00:00:00"
        },
        {
            "Zero DateTime64 with scale 3 string representation matches one of zero time_t with subsecond part",
            DateTime64WithScale{0, 3},
            "1970-01-01 00:00:00.000"
        },
        {
            "Non-Zero DateTime64 with scale 0",
            DateTime64WithScale{NON_ZERO_TIME_T, 0},
            "1979-12-31 10:17:36"
        },
        {
            "Non-Zero DateTime64 with scale 3",
            DateTime64WithScale{NON_ZERO_TIME_T * 1000LL + 123, 3},
            "1979-12-31 10:17:36.123"
        },
//        {
//            "Negative time_t value wraps around as if it was UInt32 due to LUT limitations and to maintain compatibility with existing code",
//            time_t(-10 * 365 * 3600 * 24),
//            "1979-12-30 08:00:00"
//        },
    })
);
