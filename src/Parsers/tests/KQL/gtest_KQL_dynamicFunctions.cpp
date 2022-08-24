#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserTest,
     ::testing::Combine(
         ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
         ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
         {
             "print t = array_sort_asc(dynamic([null, 'd', 'a', 'c', 'c']))",
             "SELECT arraySort([NULL, 'd', 'a', 'c', 'c']) AS t"
         },
         {
             "print t = array_sort_asc(dynamic([4, 1, 3, 2]))",
             "SELECT arraySort([4, 1, 3, 2]) AS t"
         },
         {
             "print array_sort_asc(dynamic(['b', 'a', 'c']), dynamic([20, 10, 30]))",
             "SELECT\n    arraySort(['b', 'a', 'c']) AS array0_sorted,\n    arraySort((x, y) -> y, [20, 10, 30], ['b', 'a', 'c']) AS array1_sorted"
         },
         {
             "print array_sort_asc(dynamic([2, 1, 3]), dynamic(['clickhouse','hello', 'world']))",
             "SELECT\n    arraySort([2, 1, 3]) AS array0_sorted,\n    arraySort((x, y) -> y, ['clickhouse', 'hello', 'world'], [2, 1, 3]) AS array1_sorted"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
             "SELECT arrayConcat([NULL], arraySort(['d', 'a', 'c', 'c'])) AS t"
         },
         {
             "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)",
             "SELECT arrayConcat([NULL, NULL, NULL, NULL, NULL, NULL], arraySort(['d', 'a', 'c', 'c'])) AS t"
         },
         {
             "print t = array_sort_asc( dynamic([null, null, null]) , false)",
             "SELECT arrayConcat([NULL, NULL, NULL], arraySort([])) AS t"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 < 2)",
             "SELECT if(1 < 2, arraySort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arraySort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))) AS t"
         },
         {
             "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)",
             "SELECT if(1 > 2, arraySort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arraySort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))) AS t"
         },
         {
             "print t = array_sort_desc(dynamic([null, 'd', 'a', 'c', 'c']))",
             "SELECT arrayReverseSort([NULL, 'd', 'a', 'c', 'c']) AS t"
         },
         {
             "print t = array_sort_desc(dynamic([4, 1, 3, 2]))",
             "SELECT arrayReverseSort([4, 1, 3, 2]) AS t"
         },
         {
             "print array_sort_desc(dynamic(['b', 'a', 'c']), dynamic([20, 10, 30]))",
             "SELECT\n    arrayReverseSort(['b', 'a', 'c']) AS array0_sorted,\n    arrayReverseSort((x, y) -> y, [20, 10, 30], ['b', 'a', 'c']) AS array1_sorted"
         },
         {
             "print array_sort_desc(dynamic([2, 1, 3]), dynamic(['clickhouse','hello', 'world']))",
             "SELECT\n    arrayReverseSort([2, 1, 3]) AS array0_sorted,\n    arrayReverseSort((x, y) -> y, ['clickhouse', 'hello', 'world'], [2, 1, 3]) AS array1_sorted"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
             "SELECT arrayConcat([NULL], arrayReverseSort(['d', 'a', 'c', 'c'])) AS t"
         },
         {
             "print t = array_sort_desc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)",
             "SELECT arrayConcat([NULL, NULL, NULL, NULL, NULL, NULL], arrayReverseSort(['d', 'a', 'c', 'c'])) AS t"
         },
         {
             "print t = array_sort_desc( dynamic([null, null, null]) , false)",
             "SELECT arrayConcat([NULL, NULL, NULL], arrayReverseSort([])) AS t"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 < 2)",
             "SELECT if(1 < 2, arrayReverseSort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arrayReverseSort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))) AS t"
         },
         {
             "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)",
             "SELECT if(1 > 2, arrayReverseSort(['d', NULL, 'a', 'c', 'c']), concat(arraySlice(arrayReverseSort(['d', NULL, 'a', 'c', 'c']) AS as1, indexOf(as1, NULL) AS len1), arraySlice(as1, 1, len1 - 1))) AS t"
         }
 })));
