#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Dynamic, ParserKQLTest,
     ::testing::Combine(
         ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
         ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
         {
             "print t = array_sort_asc(dynamic([null, 'd', 'a', 'c', 'c']))",
            "SELECT kql_array_sort_asc([NULL, 'd', 'a', 'c', 'c']).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic([4, 1, 3, 2]))",
            "SELECT kql_array_sort_asc([4, 1, 3, 2]).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
            "SELECT kql_array_sort_asc(['b', 'a', 'c'], ['q', 'p', 'r']).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
            "SELECT kql_array_sort_asc(['d', NULL, 'a', 'c', 'c'], false).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , false)",
            "SELECT kql_array_sort_asc([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL], false).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]) , true)",
            "SELECT kql_array_sort_asc([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL], true).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic([null, 'd', null, null, 'a', 'c', 'c', null, null, null]))",
            "SELECT kql_array_sort_asc([NULL, 'd', NULL, NULL, 'a', 'c', 'c', NULL, NULL, NULL]).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']), 1 < 2)",
            "SELECT kql_array_sort_asc(['d', NULL, 'a', 'c', 'c'], 1 < 2).1 AS t"
        },
        {
            "print t = array_sort_asc( dynamic(['d', null, 'a', 'c', 'c']) , 1 > 2)",
            "SELECT kql_array_sort_asc(['d', NULL, 'a', 'c', 'c'], 1 > 2).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), false)",
            "SELECT kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30], false).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 > 2)",
            "SELECT kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30], 1 > 2).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), true)",
            "SELECT kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30], true).1 AS t"
        },
        {
            "print t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]), 1 < 2)",
            "SELECT kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30], 1 < 2).1 AS t"
        },
        {
            "print t = array_sort_desc(dynamic([null, 'd', 'a', 'c', 'c']))",
            "SELECT kql_array_sort_desc([NULL, 'd', 'a', 'c', 'c']).1 AS t"
        },
        {
            "print t = array_sort_desc(dynamic([4, 1, 3, 2]))",
            "SELECT kql_array_sort_desc([4, 1, 3, 2]).1 AS t"
        },
        {
            "print t = array_sort_desc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
             "SELECT kql_array_sort_desc(['b', 'a', 'c'], ['q', 'p', 'r']).1 AS t"
         },
         {
            "print array_sort_desc(dynamic(['b', 'a', 'c']), dynamic(['q', 'p', 'r']))",
            "SELECT kql_array_sort_desc(['b', 'a', 'c'], ['q', 'p', 'r'])"
        },
        {
            "print t = array_sort_desc( dynamic(['d', null, 'a', 'c', 'c']) , false)",
            "SELECT kql_array_sort_desc(['d', NULL, 'a', 'c', 'c'], false).1 AS t"
        },
        {
            "print array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]))[0]",
            "SELECT tupleElement(kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]), if(0 >= 0, 0 + 1, 0))"
        },
        {
            "print  (t) = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]))",
            "SELECT kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).1 AS t"
        },
        {
            "print  (t,w) = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30]))",
            "SELECT\n    kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).1 AS t,\n    kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).2 AS w"
        },
        {
            "print  t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30])),y=5",
            "SELECT\n    kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).1 AS t,\n    5 AS y"
        },
        {
            "print 5, (t) = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30])),y=5",
            "SELECT\n    5,\n    kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).1 AS t,\n    5 AS y"
        },
        {
            "print  t = array_sort_asc(dynamic([2, 1, null,3]), dynamic([20, 10, 40, 30])),w = array_sort_asc(dynamic([2, 1, 3]))",
            "SELECT\n    kql_array_sort_asc([2, 1, NULL, 3], [20, 10, 40, 30]).1 AS t,\n    kql_array_sort_asc([2, 1, 3]).1 AS w"
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
         },
         {
            "print repeat(1, 3)",
            "SELECT if(3 < 0, [NULL], arrayWithConstant(abs(3), 1))"
         },
         {
            "print repeat(1, -3)",
            "SELECT if(-3 < 0, [NULL], arrayWithConstant(abs(-3), 1))"
         }
 })));
