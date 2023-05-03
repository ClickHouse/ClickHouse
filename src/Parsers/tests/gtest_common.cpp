#include "gtest_common.h"

#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <gmock/gmock.h>

#include <regex>

TEST_P(ParserRegexTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_TRUE(parser);
    ASSERT_TRUE(expected_ast);

    DB::ASTPtr ast;
    ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
    EXPECT_THAT(serializeAST(*ast->clone(), false), ::testing::MatchesRegex(expected_ast));
}
