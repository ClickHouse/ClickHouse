#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DataType, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print dynamic(null)",
            "SELECT NULL"
        },
        {
            "print dynamic(1)",
            "SELECT 1"
        },
        {
            "print dynamic(datetime(1))",
            "SELECT parseDateTime64BestEffortOrNull('1', 9, 'UTC')"
        },
        {
            "print dynamic(timespan(1d))",
            "SELECT 86400."
        },
        {
            "print dynamic(parse_ipv4('127.0.0.1'))",
            "throws"
        },
        {
            "print dynamic({ \"a\": 9 })",
            "throws"
        },
        {
            "print dynamic([1, 2, 3])",
            "SELECT [1, 2, 3]"
        },
        {
            "print dynamic([1, [2], 3])",
            "SELECT [1, [2], 3]"
        },
        {
            "print dynamic([[1], [2], [3]])",
            "SELECT [[1], [2], [3]]"
        },
        {
            "print dynamic(['a', \"b\", 'c'])",
            "SELECT ['a', 'b', 'c']"
        },
        {
            "print dynamic([1, 'a'])",
            "SELECT [1, 'a']"
        },
        {
            "print dynamic([datetime(1), timespan(1d), 1, 2])",
            "SELECT [parseDateTime64BestEffortOrNull('1', 9, 'UTC'), 86400., 1, 2]"
        }
})));
