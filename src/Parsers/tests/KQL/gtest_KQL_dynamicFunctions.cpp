#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Dynamic, ParserTest,
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
             "print t = array_sort_asc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
             "SELECT [arraySort(['b', 'a', 'c']), arraySort((x, y) -> y, ['q', 'p', 'r'], ['b', 'a', 'c'])] AS t"
         },
         {
             "print t = array_sort_asc(dynamic(['q', 'p', 'r']), dynamic(['clickhouse','hello', 'world']))",
             "SELECT [arraySort(['q', 'p', 'r']), arraySort((x, y) -> y, ['clickhouse', 'hello', 'world'], ['q', 'p', 'r'])] AS t"
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
             "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), false)",
             "SELECT [if(false, arraySort([2, 1, NULL, 3]), concat(arraySlice(arraySort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(false, arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 > 2)",
             "SELECT [if(1 > 2, arraySort([2, 1, NULL, 3]), concat(arraySlice(arraySort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 > 2, arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), true)",
             "SELECT [if(true, arraySort([2, 1, NULL, 3]), concat(arraySlice(arraySort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 < 2)",
             "SELECT [if(1 < 2, arraySort([2, 1, NULL, 3]), concat(arraySlice(arraySort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 < 2, arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]))",
             "SELECT [if(true, arraySort([2, 1, NULL, 3]), concat(arraySlice(arraySort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arraySort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
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
             "print t = array_sort_desc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), false)",
             "SELECT [if(false, arrayReverseSort([2, 1, NULL, 3]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(false, arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 > 2)",
             "SELECT [if(1 > 2, arrayReverseSort([2, 1, NULL, 3]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 > 2, arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), true)",
             "SELECT [if(true, arrayReverseSort([2, 1, NULL, 3]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 < 2)",
             "SELECT [if(1 < 2, arrayReverseSort([2, 1, NULL, 3]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(1 < 2, arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
             "print t = array_sort_desc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]))",
             "SELECT [if(true, arrayReverseSort([2, 1, NULL, 3]), concat(arraySlice(arrayReverseSort([2, 1, NULL, 3]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), If(true, arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), arrayConcat(arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), (length([2, 1, NULL, 3]) - 1) + 1), arraySlice(arrayReverseSort((x, y) -> y, [20, 10, 40, 30], [2, 1, NULL, 3]), 1, length([2, 1, NULL, 3]) - 1)))] AS t"
         },
         {
            "print t = array_sort_desc(dynamic(['b', 'a', null]), dynamic(['p', 'q', 'r', 's']), 1 < 2)",
            "SELECT [if(1 < 2, arrayReverseSort(['b', 'a', NULL]), concat(arraySlice(arrayReverseSort(['b', 'a', NULL]) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))), [NULL]] AS t"
         },
         {
             "print A[0]",
             "SELECT A[if(0 >= 0, 0 + 1, 0)]"
         },
         {
             "print A[0][1]",
             "SELECT (A[if(0 >= 0, 0 + 1, 0)])[if(1 >= 0, 1 + 1, 1)]"
         },
         {
             "print dynamic([[1,2,3,4,5],[20,30]])[0]",
             "SELECT [[1, 2, 3, 4, 5], [20, 30]][if(0 >= 0, 0 + 1, 0)]"
         },
         {
             "print dynamic([[1,2,3,4,5],[20,30]])[1][1]",
             "SELECT ([[1, 2, 3, 4, 5], [20, 30]][if(1 >= 0, 1 + 1, 1)])[if(1 >= 0, 1 + 1, 1)]"
         },
         {
             "print A[B[1]]",
             "SELECT A[if((B[if(1 >= 0, 1 + 1, 1)]) >= 0, (B[if(1 >= 0, 1 + 1, 1)]) + 1, B[if(1 >= 0, 1 + 1, 1)])]"
         },
         {
             "print A[strlen('a')-1]",
             "SELECT A[if((lengthUTF8('a') - 1) >= 0, (lengthUTF8('a') - 1) + 1, lengthUTF8('a') - 1)]"
         },
         {
             "print strlen(A[0])",
             "SELECT lengthUTF8(A[if(0 >= 0, 0 + 1, 0)])"
         }
 })));
