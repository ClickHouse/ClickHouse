#include <gtest/gtest.h>

#include <Parsers/Lexer.h>


/// Regression tests for the `DB::Lexer` edge cases exercised by the
/// function property fuzzer and libFuzzer-driven parser fuzzing.

/// Previously `Lexer::nextToken` computed `begin + max_query_size` unconditionally.
/// When the fuzzer constructed a lexer with `begin == nullptr` and a non-zero
/// `max_query_size`, this produced undefined behaviour:
///     applying non-zero offset 262144 to null pointer
/// Reported at `src/Parsers/Lexer.cpp:119` by the `function_prop_fuzzer` UBSan run.
TEST(LexerTest, NextTokenWithNullBeginAndMaxQuerySize)
{
    DB::Lexer lexer(nullptr, nullptr, 262144);
    DB::Token token = lexer.nextToken();
    EXPECT_EQ(token.type, DB::TokenType::EndOfStream);
}

/// Construct a lexer over an empty but non-null buffer and confirm that we
/// return `EndOfStream` with `max_query_size` still honoured.
TEST(LexerTest, NextTokenWithEmptyButNonNullBuffer)
{
    const char data[1] = {0};
    DB::Lexer lexer(data, data, 256);
    DB::Token token = lexer.nextToken();
    EXPECT_EQ(token.type, DB::TokenType::EndOfStream);
}

/// Sanity: a real query still produces a normal token stream after the null check
/// on `begin` was added.
TEST(LexerTest, NextTokenWithSimpleQuery)
{
    std::string_view sql = "SELECT 1";
    DB::Lexer lexer(sql.data(), sql.data() + sql.size());
    DB::Token token = lexer.nextToken();
    EXPECT_EQ(token.type, DB::TokenType::BareWord);
    EXPECT_EQ(std::string_view(token.begin, token.size()), "SELECT");
}
