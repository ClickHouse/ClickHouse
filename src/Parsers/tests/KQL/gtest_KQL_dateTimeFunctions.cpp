#include <Parsers/tests/gtest_common.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/applyTableOverride.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserAttachAccessEntity.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <string_view>
#include <regex>
#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}
class ParserDateTimeFuncTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};

TEST_P(ParserDateTimeFuncTest, ParseQuery)
{  const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());
    ASSERT_NE(nullptr, parser);
    if (expected_ast)
    {
        if (std::string(expected_ast).starts_with("throws"))
        {
            EXPECT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
        }
        else
        {
            ASTPtr ast;
            ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
            if (std::string("CREATE USER or ALTER USER query") != parser->getName()
                    && std::string("ATTACH access entity query") != parser->getName())
            {
                EXPECT_EQ(expected_ast, serializeAST(*ast->clone(), false));
            }
            else
            {
                if (input_text.starts_with("ATTACH"))
                {
                    auto salt = (dynamic_cast<const ASTCreateUserQuery *>(ast.get())->auth_data)->getSalt();
                    EXPECT_TRUE(std::regex_match(salt, std::regex(expected_ast)));
                }
                else
                {
                    EXPECT_TRUE(std::regex_match(serializeAST(*ast->clone(), false), std::regex(expected_ast)));
                }
            }
        }
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
    }
}

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserDateTimeFuncTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print week_of_year(datetime(2020-12-31))",
            "SELECT toWeek(toDateTime64('2020-12-31', 9, 'UTC'), 3, 'UTC')"
        },
        {
            "print startofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfWeek(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(-1)"
        },
        {
            "print startofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfMonth(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(-1)"
        },
        {
            "print startofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(-1)"

        },
        {
            "print startofyear(datetime(2017-01-01 10:10:17), -1)",
            "SELECT toDateTime64(toStartOfYear(toDateTime64('2017-01-01 10:10:17', 9, 'UTC'), 'UTC'), 9, 'UTC') + toIntervalYear(-1)"
        },
        {
            "print monthofyear(datetime(2015-12-14))",
            "SELECT toMonth(toDateTime64('2015-12-14', 9, 'UTC'))"
        },
        {
            "print hourofday(datetime(2015-12-14 10:54:00))",
            "SELECT toHour(toDateTime64('2015-12-14 10:54:00', 9, 'UTC'))"
        },
        {
            "print getyear(datetime(2015-10-12))",
            "SELECT toYear(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print getmonth(datetime(2015-10-12))",
            "SELECT toMonth(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofyear(datetime(2015-10-12))",
            "SELECT toDayOfYear(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print dayofmonth(datetime(2015-10-12))",
            "SELECT toDayOfMonth(toDateTime64('2015-10-12', 9, 'UTC'))"
        },
        {
            "print unixtime_seconds_todatetime(1546300899)",
            "SELECT toDateTime64(1546300899, 9, 'UTC')"
        },
        {
            "print dayofweek(datetime(2015-12-20))",
            "SELECT toDayOfWeek(toDateTime64('2015-12-20', 9, 'UTC')) % 7"
        },
        {
            "print now()",
            "SELECT now64(9, 'UTC')"
        },
        {
            "print now(1d)",
            "SELECT now64(9, 'UTC') + 86400."
        },
        {
            "print ago(2d)",
            "SELECT now64(9, 'UTC') - 172800."
        },  
        {
            "print endofday(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofday(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalDay(1 + 1)) - toIntervalMicrosecond(1)"

        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofmonth(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalMonth(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), -1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofweek(datetime(2017-01-01 10:10:17), 1)",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalWeek(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print endofyear(datetime(2017-01-01 10:10:17), -1) ",
            "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalYear(-1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
           "print endofyear(datetime(2017-01-01 10:10:17), 1)" ,
           "SELECT (toDateTime(toStartOfDay(toDateTime64('2017-01-01 10:10:17', 9, 'UTC')), 9, 'UTC') + toIntervalYear(1 + 1)) - toIntervalMicrosecond(1)"
        },
        {
            "print make_datetime(2017,10,01)",
            "SELECT makeDateTime64(2017, 10, 1, 0, 0, 0, 0, 7, 'UTC')"
        },
        {
            "print make_datetime(2017,10,01,12,10)",
            "SELECT makeDateTime64(2017, 10, 1, 12, 10, 0, 0, 7, 'UTC')"
        },
        {
            "print make_datetime(2017,10,01,12,11,0.1234567)",
            "SELECT makeDateTime64(2017, 10, 1, 12, 11, 0, 1234567, 7, 'UTC')"
        },
        {
            "print unixtime_microseconds_todatetime(1546300800000000)",
            "SELECT fromUnixTimestamp64Micro(1546300800000000, 'UTC')"
        },
        {
            "print unixtime_milliseconds_todatetime(1546300800000)",
            "SELECT fromUnixTimestamp64Milli(1546300800000, 'UTC')"
        },
        {
            "print unixtime_nanoseconds_todatetime(1546300800000000000)",
            "SELECT fromUnixTimestamp64Nano(1546300800000000000, 'UTC')"
        },
        {
            "print datetime_diff('year',datetime(2017-01-01),datetime(2000-12-31))",
            "SELECT ABS(dateDiff('year', toDateTime64('2017-01-01', 9, 'UTC'), toDateTime64('2000-12-31', 9, 'UTC')))"
        },
        {
            "print datetime_diff('minute',datetime(2017-10-30 23:05:01),datetime(2017-10-30 23:00:59))",
            "SELECT ABS(dateDiff('minute', toDateTime64('2017-10-30 23:05:01', 9, 'UTC'), toDateTime64('2017-10-30 23:00:59', 9, 'UTC')))"
        }

})));   
