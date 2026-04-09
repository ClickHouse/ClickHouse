#include <Parsers/Lexer.h>
#include <gtest/gtest.h>


using namespace DB;

TEST(Lexer, NullBuffer)
{
    /// Lexer with null begin/end and non-zero max_query_size should not trigger UB
    /// (adding a non-zero offset to a null pointer is undefined behavior).
    Lexer lexer(nullptr, nullptr, 262144);
    Token token = lexer.nextToken();
    ASSERT_EQ(token.type, TokenType::EndOfStream);
}
