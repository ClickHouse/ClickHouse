#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(
    ParserKQLQuery_Conversion,
    ParserKQLTest,
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
            "print tolong(A)",
            "SELECT toInt64OrNull(toString(A))"
        },
        {
            "print toreal(A)",
            "SELECT toFloat64OrNull(toString(A))"
        },
        {
            "print tostring(A)",
            "SELECT ifNull(toString(A), '')"
        },
        {
            "print decimal(123.345)",
            "SELECT toDecimal128(CAST('123.345', 'String'), 32)"
        },
        {
            "print decimal(NULL)",
            "SELECT NULL"
        },
        {
            "print todecimal('123.45')",
            "SELECT toDecimal128(CAST('123.45', 'String'), 32)"
        },
        {
            "print todecimal(NULL)",
            "SELECT NULL"
        },
        {
            "print todecimal(123456.3456)",
            "SELECT toDecimal128(CAST('123456.3456', 'String'), 12)"
        },
        {
            "print todecimal('abc')",
            "SELECT NULL"
        },
        {
            "print todecimal('1e5')",
            "SELECT toDecimal128(CAST('1e5', 'String'), 5)"
        },
        {
            "print decimal(1e-5)",
            "SELECT toDecimal128(CAST('1e-5', 'String'), 5)"
        },
        {
            "print time(9nanoseconds)",
            "SELECT CAST('9e-09', 'Float64')"
        },
        {
            "print time(1tick)",
            "SELECT CAST('1e-07', 'Float64')"
        }

})));
