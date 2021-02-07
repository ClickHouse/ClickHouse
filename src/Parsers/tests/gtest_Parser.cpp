#include <Parsers/ParserOptimizeQuery.h>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromOStream.h>

#include <string_view>

#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}

struct ParserTestCase
{
    std::shared_ptr<IParser> parser;
    const std::string_view input_text;
    const char * expected_ast = nullptr;
};

std::ostream & operator<<(std::ostream & ostr, const ParserTestCase & test_case)
{
    return ostr << "parser: " << test_case.parser->getName() << ", input: " << test_case.input_text;
}

class ParserTest : public ::testing::TestWithParam<ParserTestCase>
{};

TEST_P(ParserTest, parseQuery)
{
    const auto & [parser, input_text, expected_ast] = GetParam();

    ASSERT_NE(nullptr, parser);

    if (expected_ast)
    {
        ASTPtr ast;
        ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
        EXPECT_EQ(expected_ast, serializeAST(*ast->clone(), false));
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
    }
}


INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery, ParserTest, ::testing::Values(
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY *"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a"
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)"
    }
));

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery_FAIL, ParserTest, ::testing::Values(
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY",
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * APPLY(x)",
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY * REPLACE(y)",
    },
    ParserTestCase
    {
        std::make_shared<ParserOptimizeQuery>(),
        "OPTIMIZE TABLE table_name DEDUPLICATE BY db.a, db.b, db.c",
    }
));
