#include "gtest_common.h"

#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/parseKQLQuery.h>

#include <Common/re2.h>

#include <gmock/gmock.h>



namespace
{
using namespace DB;
using namespace std::literals;
}


TEST_P(ParserRegexTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_TRUE(parser);
    ASSERT_TRUE(expected_ast);

    DB::ASTPtr ast;
    ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0));
    DB::WriteBufferFromOwnString buf;
    formatAST(*ast->clone(), buf, false, false);
    EXPECT_THAT(buf.str(), ::testing::MatchesRegex(expected_ast));
}

TEST_P(ParserKQLTest, parseKQLQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_NE(nullptr, parser);

    if (expected_ast)
    {
        if (std::string(expected_ast).starts_with("throws"))
        {
            EXPECT_THROW(parseKQLQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0), DB::Exception);
        }
        else
        {
            DB::ASTPtr ast;
            ASSERT_NO_THROW(ast = parseKQLQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0));
            if (std::string("CREATE USER or ALTER USER query") != parser->getName()
                    && std::string("ATTACH access entity query") != parser->getName())
            {
                DB::WriteBufferFromOwnString buf;
                formatAST(*ast->clone(), buf, false, false);
                String formatted_ast = buf.str();
                EXPECT_EQ(expected_ast, formatted_ast);
            }
            else
            {
                if (input_text.starts_with("ATTACH"))
                {
                    auto salt = (dynamic_cast<const ASTCreateUserQuery *>(ast.get())->authentication_methods.back())->getSalt().value_or("");
                    EXPECT_TRUE(re2::RE2::FullMatch(salt, expected_ast));
                }
                else
                {
                    DB::WriteBufferFromOwnString buf;
                    formatAST(*ast->clone(), buf, false, false);
                    String formatted_ast = buf.str();
                    EXPECT_TRUE(re2::RE2::FullMatch(formatted_ast, expected_ast));
                }
            }
        }
    }
    else
    {
        ASSERT_THROW(parseKQLQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0), DB::Exception);
    }
}
