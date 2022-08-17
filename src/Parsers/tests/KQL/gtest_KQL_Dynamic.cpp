#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Dynamic, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print array_iff(A, B, C)",
            "SELECT arrayMap(x -> if((x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
        },
        {
            "print array_iif(A, B, C)",
            "SELECT arrayMap(x -> if((x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
        }
})));
