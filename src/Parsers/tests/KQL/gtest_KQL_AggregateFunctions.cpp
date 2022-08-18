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
class ParserAggregateFuncTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};

TEST_P(ParserAggregateFuncTest, ParseQuery)
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

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserAggregateFuncTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | summarize t = stdev(Age) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSamp(Age)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = stdevif(Age, Age < 10) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSampIf(Age, Age < 10)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_and(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitAnd(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_or(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitOr(Age) AS t\nFROM Customers\nGROUP BY FirstName"

        },
        {
            "Customers | summarize t = binary_all_xor(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitXor(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize percentiles(Age, 30, 40, 50, 60, 70) by FirstName",
            "SELECT\n    FirstName,\n    quantile(30 / 100)(Age) AS percentile_Age_30,\n    quantile(40 / 100)(Age) AS percentile_Age_40,\n    quantile(50 / 100)(Age) AS percentile_Age_50,\n    quantile(60 / 100)(Age) AS percentile_Age_60,\n    quantile(70 / 100)(Age) AS percentile_Age_70\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, 10, 20, 30, 50) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, dynamic([10, 20, 30, 50])) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "DataTable | summarize t = percentilesw(Bucket, Frequency, 50, 75, 99.9)",
            "SELECT\n    quantileExactWeighted(50 / 100)(Bucket, Frequency) AS percentile_Bucket_50,\n    quantileExactWeighted(75 / 100)(Bucket, Frequency) AS percentile_Bucket_75,\n    quantileExactWeighted(99.9 / 100)(Bucket, Frequency) AS percentile_Bucket_99_9\nFROM DataTable"
        },
        {
            "DataTable| summarize t = percentilesw_array(Bucket, Frequency, dynamic([10, 50, 30]))",
            "SELECT quantilesExactWeighted(10 / 100, 50 / 100, 30 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        }
})));
