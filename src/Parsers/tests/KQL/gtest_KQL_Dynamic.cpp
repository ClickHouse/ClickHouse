#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DynamicExactMatch, ParserKQLTest,
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
            "SELECT arrayMap(x -> multiIf(toTypeName(x.1) = 'String', NULL, toInt64(x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
        },
        {
            "print array_iif(A, B, C)",
            "SELECT arrayMap(x -> multiIf(toTypeName(x.1) = 'String', NULL, toInt64(x.1) != 0, x.2, x.3), arrayZip(A, arrayResize(B, length(A), NULL), arrayResize(C, length(A), NULL)))"
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
            "print array_reverse(A)",
            "SELECT if(throwIf(NOT startsWith(toTypeName(A), 'Array'), 'Only arrays are supported'), [], reverse(A))"
        },
        {
            "print array_rotate_left(A, B)",
            "SELECT arrayMap(x -> (A[moduloOrZero((x + length(A)) + moduloOrZero(B, toInt64(length(A))), length(A)) + 1]), range(0, length(A)))"
        },
        {
            "print array_rotate_right(A, B)",
            "SELECT arrayMap(x -> (A[moduloOrZero((x + length(A)) + moduloOrZero(-1 * B, toInt64(length(A))), length(A)) + 1]), range(0, length(A)))"
        },
        {
            "print output = array_sum(dynamic([2, 5, 3]))",
            "SELECT arraySum([2, 5, 3]) AS output"
        },
        {
            "print output = array_sum(dynamic([2.5, 5.5, 3]))",
            "SELECT arraySum([2.5, 5.5, 3]) AS output"
        },
        {
            "print jaccard_index(A, B)",
            "SELECT length(arrayIntersect(A, B)) / length(arrayDistinct(arrayConcat(A, B)))"
        },
        {
            "print pack_array(A, B, C, D)",
            "SELECT [A, B, C, D]"
        },
        {
            "print set_difference(A, B)",
            "SELECT arrayFilter(x -> (NOT has(arrayDistinct(arrayConcat(B)), x)), arrayDistinct(A))"
        },
        {
            "print set_difference(A, B, C)",
            "SELECT arrayFilter(x -> (NOT has(arrayDistinct(arrayConcat(B, C)), x)), arrayDistinct(A))"
        },
        {
            "print set_has_element(A, B)",
            "SELECT has(A, B)"
        },
        {
            "print set_intersect(A, B)",
            "SELECT arrayIntersect(A, B)"
        },
        {
            "print set_intersect(A, B, C)",
            "SELECT arrayIntersect(A, B, C)"
        },
        {
            "print set_union(A, B)",
            "SELECT arrayDistinct(arrayConcat(A, B))"
        },
        {
            "print set_union(A, B, C)",
            "SELECT arrayDistinct(arrayConcat(A, B, C))"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_DynamicRegex, ParserRegexTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print array_shift_left(A, B)",
            R"(SELECT arrayResize\(if\(B > 0, arraySlice\(A, B \+ 1\), arrayConcat\(arrayWithConstant\(abs\(B\), fill_value_\d+\), A\)\), length\(A\), if\(\(NULL IS NULL\) AND \(\(extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)*'\) AS element_type_\d+\) = 'String'\), defaultValueOfTypeName\(if\(element_type_\d+ = 'Nothing', 'Nullable\(Nothing\)', element_type_\d+\)\), NULL\) AS fill_value_\d+\))"
        },
        {
            "print array_shift_left(A, B, C)",
            R"(SELECT arrayResize\(if\(B > 0, arraySlice\(A, B \+ 1\), arrayConcat\(arrayWithConstant\(abs\(B\), fill_value_\d+\), A\)\), length\(A\), if\(\(C IS NULL\) AND \(\(extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)'\) AS element_type_\d+\) = 'String'\), defaultValueOfTypeName\(if\(element_type_\d+ = 'Nothing', 'Nullable\(Nothing\)', element_type_\d+\)\), C\) AS fill_value_\d+\))"
        },
        {
            "print array_shift_right(A, B)",
            R"(SELECT arrayResize\(if\(\(-1 \* B\) > 0, arraySlice\(A, \(-1 \* B\) \+ 1\), arrayConcat\(arrayWithConstant\(abs\(-1 \* B\), fill_value_\d+\), A\)\), length\(A\), if\(\(NULL IS NULL\) AND \(\(extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)'\) AS element_type_\d+\) = 'String'\), defaultValueOfTypeName\(if\(element_type_\d+ = 'Nothing', 'Nullable\(Nothing\)', element_type_\d+\)\), NULL\) AS fill_value_\d+\))"
        },
        {
            "print array_shift_right(A, B, C)",
            R"(SELECT arrayResize\(if\(\(-1 \* B\) > 0, arraySlice\(A, \(-1 \* B\) \+ 1\), arrayConcat\(arrayWithConstant\(abs\(-1 \* B\), fill_value_\d+\), A\)\), length\(A\), if\(\(C IS NULL\) AND \(\(extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)'\) AS element_type_\d+\) = 'String'\), defaultValueOfTypeName\(if\(element_type_\d+ = 'Nothing', 'Nullable\(Nothing\)', element_type_\d+\)\), C\) AS fill_value_\d+\))"
        },
        {
            "print array_slice(A, B, C)",
            R"(SELECT arraySlice\(A, 1 \+ if\(B >= 0, B, arrayMax\(\[-length\(A\), B\]\) \+ length\(A\)\) AS offset_\d+, \(\(1 \+ if\(C >= 0, C, arrayMax\(\[-length\(A\), C\]\) \+ length\(A\)\)\) - offset_\d+\) \+ 1\))"
        },
        {
            "print array_split(A, B)",
            R"(SELECT if\(empty\(arrayMap\(x -> if\(x >= 0, x, arrayMax\(\[0, x \+ CAST\(length\(A\), 'Int\d+'\)\]\)\), flatten\(\[B\]\)\) AS indices_\d+\), \[A\], arrayConcat\(\[arraySlice\(A, 1, indices_\d+\[1\]\)\], arrayMap\(i -> arraySlice\(A, \(indices_\d+\[i\]\) \+ 1, if\(i = length\(indices_\d+\), CAST\(length\(A\), 'Int\d+'\), CAST\(indices_\d+\[i \+ 1\], 'Int\d+'\)\) - \(indices_\d+\[i\]\)\), range\(1, length\(indices_\d+\) \+ 1\)\)\)\))"
        },
        {
            "print zip(A, B)",
            R"(SELECT arrayMap\(t -> \[untuple\(t\)\], arrayZip\(arrayResize\(arg0_\d+, arrayMax\(\[length\(if\(match\(toTypeName\(A\), 'Array\\\\\(Nullable\\\\\(\.\*\\\\\)\\\\\)'\), A, CAST\(A, concat\('Array\(Nullable\(', extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)'\), '\)\)'\)\)\) AS arg0_\d+\), length\(if\(match\(toTypeName\(B\), 'Array\\\\\(Nullable\\\\\(\.\*\\\\\)\\\\\)'\), B, CAST\(B, concat\('Array\(Nullable\(', extract\(toTypeName\(B\), 'Array\\\\\(\(\.\*\)\\\\\)'\), '\)\)'\)\)\) AS arg1_\d+\)\]\) AS max_length_\d+, NULL\), arrayResize\(arg1_\d+, max_length_\d+, NULL\)\)\))"
        },
        {
            "print zip(A, B, C)",
            R"(SELECT arrayMap\(t -> \[untuple\(t\)\], arrayZip\(arrayResize\(arg0_\d+, arrayMax\(\[length\(if\(match\(toTypeName\(A\), 'Array\\\\\(Nullable\\\\\(\.\*\\\\\)\\\\\)'\), A, CAST\(A, concat\('Array\(Nullable\(', extract\(toTypeName\(A\), 'Array\\\\\(\(\.\*\)\\\\\)'\), '\)\)'\)\)\) AS arg0_\d+\), length\(if\(match\(toTypeName\(B\), 'Array\\\\\(Nullable\\\\\(\.\*\\\\\)\\\\\)'\), B, CAST\(B, concat\('Array\(Nullable\(', extract\(toTypeName\(B\), 'Array\\\\\(\(\.\*\)\\\\\)'\), '\)\)'\)\)\) AS arg1_\d+\), length\(if\(match\(toTypeName\(C\), 'Array\\\\\(Nullable\\\\\(\.\*\\\\\)\\\\\)'\), C, CAST\(C, concat\('Array\(Nullable\(', extract\(toTypeName\(C\), 'Array\\\\\(\(\.\*\)\\\\\)'\), '\)\)'\)\)\) AS arg2_\d+\)\]\) AS max_length_\d+, NULL\), arrayResize\(arg1_\d+, max_length_\d+, NULL\), arrayResize\(arg2_\d+, max_length_\d+, NULL\)\)\))"
        }
})));
