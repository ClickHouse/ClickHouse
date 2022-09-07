#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserTest,
     ::testing::Combine(
         ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
         ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
         {
             "print t = array_sort_asc(dynamic([null, 'd', 'a', 'c', 'c']))",
             "SELECT [arraySort([NULL, 'd', 'a', 'c', 'c'])] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([4, 1, 3, 2]))",
             "SELECT [arraySort([4, 1, 3, 2])] AS t"
         },
         {
             "print array_sort_asc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
             "SELECT [arraySort(['b', 'a', 'c']), arraySort((x, y) -> y, ['q', 'p', 'r'], ['b', 'a', 'c'])]"
         },
         {
             "print array_sort_asc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))",
             "SELECT [arraySort(['q', 'p', 'r']), arraySort((x, y) -> y, ['clickhouse', 'hello', 'world'], ['q', 'p', 'r'])]"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
             "SELECT [if(false, arraySort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arraySort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)",
             "SELECT [if(false, arraySort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]), concat(arraySlice(arraySort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , true)",
             "SELECT [if(true, arraySort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]), concat(arraySlice(arraySort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]))",
             "SELECT [arraySort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL])] AS t"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 < 2)",
             "SELECT [if(1 < 2, arraySort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arraySort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)",
             "SELECT [if(1 > 2, arraySort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arraySort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), false)",
             "SELECT [if(false, arraySort([2, 1, NULL, 3, NULL]), concat(arraySlice(arraySort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(false, arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print array_sort_asc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), 1 > 2)",
             "SELECT [if(1 > 2, arraySort([2, 1, NULL, 3, NULL]), concat(arraySlice(arraySort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 > 2, arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))]"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), true)",
             "SELECT [if(true, arraySort([2, 1, NULL, 3, NULL]), concat(arraySlice(arraySort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), 1 < 2)",
             "SELECT [if(1 < 2, arraySort([2, 1, NULL, 3, NULL]), concat(arraySlice(arraySort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 < 2, arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]))",
             "SELECT [if(true, arraySort([2, 1, NULL, 3, NULL]), concat(arraySlice(arraySort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([null, 'd', 'a', 'c', 'c']))",
             "SELECT [arrayReverseSort([NULL, 'd', 'a', 'c', 'c'])] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([4, 1, 3, 2]))",
             "SELECT [arrayReverseSort([4, 1, 3, 2])] AS t"
         },
         {
             "print t = array_sort_desc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
             "SELECT [arrayReverseSort(['b', 'a', 'c']), arrayReverseSort((x, y) -> y, ['q', 'p', 'r'], ['b', 'a', 'c'])] AS t"
         },
         {
             "print t = array_sort_desc(dynamic(['2', '1', '3']), dynamic(['clickhouse','hello', 'world']))",
             "SELECT [arrayReverseSort(['2', '1', '3']), arrayReverseSort((x, y) -> y, ['clickhouse', 'hello', 'world'], ['2', '1', '3'])] AS t"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
             "SELECT [if(false, arrayReverseSort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arrayReverseSort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)",
             "SELECT [if(false, arrayReverseSort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]), concat(arraySlice(arrayReverseSort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , true)",
             "SELECT [if(true, arrayReverseSort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]), concat(arraySlice(arrayReverseSort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]))",
             "SELECT [arrayReverseSort([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL])] AS t"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 < 2)",
             "SELECT [if(1 < 2, arrayReverseSort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arrayReverseSort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)",
             "SELECT [if(1 > 2, arrayReverseSort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arrayReverseSort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), false)",
             "SELECT [if(false, arrayReverseSort([2, 1, NULL, 3, NULL]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(false, arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), 1 > 2)",
             "SELECT [if(1 > 2, arrayReverseSort([2, 1, NULL, 3, NULL]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 > 2, arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), true)",
             "SELECT [if(true, arrayReverseSort([2, 1, NULL, 3, NULL]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]), 1 < 2)",
             "SELECT [if(1 < 2, arrayReverseSort([2, 1, NULL, 3, NULL]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 < 2, arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3, null]), dynamic([20, 10, 40, 30, 50]))",
             "SELECT [if(true, arrayReverseSort([2, 1, NULL, 3, NULL]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3, NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), (length([2, 1, NULL, 3, NULL]) - 2) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30, 50], [2, 1, NULL, 3, NULL]), 1, length([2, 1, NULL, 3, NULL]) - 2)))] AS t"
         },
         {
            "print t = array_sort_desc(dynamic(['b', 'a', null]), dynamic(['p', 'q', 'r', 's']), 1 < 2)",
            "SELECT [if(1 < 2, arrayReverseSort(['b', 'a', NULL]), concat(arraySlice(arrayReverseSort(['b', 'a', NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), [NULL]] AS t"
         }
 })));
