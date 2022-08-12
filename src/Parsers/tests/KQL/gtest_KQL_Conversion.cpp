#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(
    ParserKQLQuery_Conversion,
    ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print tobool(A)",
            "SELECT multiIf(toString(A) = 'true', true, toString(A) = 'false', false, toInt64OrNull(toString(A)) != 0)"
        },
        {
            "print toboolean(A)",
            "SELECT multiIf(toString(A) = 'true', true, toString(A) = 'false', false, toInt64OrNull(toString(A)) != 0)"
        },
        {
            "print todouble(A)",
            "SELECT toFloat64OrNull(toString(A))"
        },
        {
            "print toint(A)",
            "SELECT toInt32OrNull(toString(A))"
        },
        {
            "print toreal(A)",
            "SELECT toFloat64OrNull(toString(A))"
        },
        {
            "print tostring(A)",
            "SELECT ifNull(toString(A), '')"
        }
})));
