#include <Parsers/tests/gtest_common.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/applyTableOverride.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserAttachAccessEntity.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <string_view>
#include <regex>
#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}

class ParserDynamicFuncTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
 {};

 TEST_P(ParserDynamicFuncTest, ParseQuery)
 {  const auto & parser = std::get<0>(GetParam());
     const auto & [input_text, expected_ast] = std::get<1>(GetParam());
     ASSERT_NE(nullptr, parser);
     if (expected_ast)
     {
         if (std::string(expected_ast).starts_with("throws"))
         {
             EXPECT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
         }
         else
         {
             ASTPtr ast;
             ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
             if (std::string("CREATE USER or ALTER USER query") != parser->getName()
                     && std::string("ATTACH access entity query") != parser->getName())
             {
                 EXPECT_EQ(expected_ast, serializeAST(*ast->clone(), false));
             }
             else
             {
                 if (input_text.starts_with("ATTACH"))
                 {
                     auto salt = (dynamic_cast<const ASTCreateUserQuery *>(ast.get())->auth_data)->getSalt();
                     EXPECT_TRUE(std::regex_match(salt, std::regex(expected_ast)));
                 }
                 else
                 {
                     EXPECT_TRUE(std::regex_match(serializeAST(*ast->clone(), false), std::regex(expected_ast)));
                 }
             }
         }
     }
     else
     {
         ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
     }
 }

 INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserDynamicFuncTest,
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
         }

 })));
