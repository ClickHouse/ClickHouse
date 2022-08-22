#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DynamicExactMatch, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print array_concat(A, B)",
            "SELECT arrayConcat(A, B)"
        },
        {
            "print array_concat(A, B, C, D)",
            "SELECT arrayConcat(A, B, C, D)"
        },
        {
            "print array_iff(A, B, C)",
            "SELECT arrayMap(x -> if((x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
        },
        {
            "print array_iif(A, B, C)",
            "SELECT arrayMap(x -> if((x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
        },
        {
            "print output = array_index_of(dynamic([1, 2, 3]), 2)",
            "SELECT indexOf([1, 2, 3], 2) - 1 AS output"
        },
        {
            "print output = array_index_of(dynamic(['a', 'b', 'c']), 'b')",
            "SELECT indexOf(['a', 'b', 'c'], 'b') - 1 AS output"
        },
        {
            "print output = array_index_of(dynamic(['John', 'Denver', 'Bob', 'Marley']), 'Marley')",
            "SELECT indexOf(['John', 'Denver', 'Bob', 'Marley'], 'Marley') - 1 AS output"
        },
        
        {
            "print output = array_length(dynamic([1, 2, 3]))",
            "SELECT length([1, 2, 3]) AS output"
        },
        {
            "print output = array_length(dynamic(['John', 'Denver', 'Bob', 'Marley']))",
            "SELECT length(['John', 'Denver', 'Bob', 'Marley']) AS output"
        },
        {
            "print output = array_sum(dynamic([2, 5, 3]))",
            "SELECT arraySum([2, 5, 3]) AS output"
        },
        {
            "print output = array_sum(dynamic([2.5, 5.5, 3]))",
            "SELECT arraySum([2.5, 5.5, 3]) AS output"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DynamicRegex, ParserRegexTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print array_slice(A, B, C)",
            "SELECT arraySlice\\(A, 1 \\+ if\\(B >= 0, B, toInt64\\(max2\\(-length\\(A\\), B\\)\\) \\+ length\\(A\\)\\) AS offset_\\d+, \\(\\(1 \\+ if\\(C >= 0, C, toInt64\\(max2\\(-length\\(A\\), C\\)\\) \\+ length\\(A\\)\\)\\) - offset_\\d+\\) \\+ 1\\)"
        },
        {
            "print array_split(A, B)",
            "SELECT if\\(empty\\(arrayMap\\(x -> if\\(x >= 0, x, toInt64\\(max2\\(0, x \\+ length\\(A\\)\\)\\)\\), flatten\\(\\[B\\]\\)\\) AS indices_\\d+\\), \\[A\\], arrayConcat\\(\\[arraySlice\\(A, 1, indices_\\d+\\[1\\]\\)\\], arrayMap\\(i -> arraySlice\\(A, \\(indices_\\d+\\[i\\]\\) \\+ 1, if\\(i = length\\(indices_\\d+\\), CAST\\(length\\(A\\), 'Int64'\\), CAST\\(indices_\\d+\\[i \\+ 1\\], 'Int64'\\)\\) - \\(indices_\\d+\\[i\\]\\)\\), range\\(1, length\\(indices_\\d+\\) \\+ 1\\)\\)\\)\\)"
        }
})));
