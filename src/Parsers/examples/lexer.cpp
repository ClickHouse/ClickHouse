#include <map>
#include <Parsers/Lexer.h>
#include <base/types.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


/// How to test:
/// for i in ~/work/ClickHouse/tests/queries/0_stateless/*.sql; do echo $i; grep -q 'FORMAT' $i || ./lexer < $i || break; done
///


using namespace DB;

std::map<TokenType, const char *> hilite =
{
    {TokenType::Whitespace, "\033[0;44m"},
    {TokenType::Comment, "\033[1;46m"},
    {TokenType::BareWord, "\033[1m"},
    {TokenType::Number, "\033[1;36m"},
    {TokenType::StringLiteral, "\033[1;32m"},
    {TokenType::QuotedIdentifier, "\033[1;35m"},

    {TokenType::OpeningRoundBracket, "\033[1;33m"},
    {TokenType::ClosingRoundBracket, "\033[1;33m"},
    {TokenType::OpeningSquareBracket, "\033[1;33m"},
    {TokenType::ClosingSquareBracket, "\033[1;33m"},
    {TokenType::OpeningCurlyBrace, "\033[1;33m"},
    {TokenType::ClosingCurlyBrace, "\033[1;33m"},

    {TokenType::Comma, "\033[1;33m"},
    {TokenType::Semicolon, "\033[1;33m"},
    {TokenType::Dot, "\033[1;33m"},
    {TokenType::Asterisk, "\033[1;33m"},
    {TokenType::Plus, "\033[1;33m"},
    {TokenType::Minus, "\033[1;33m"},
    {TokenType::Slash, "\033[1;33m"},
    {TokenType::Percent, "\033[1;33m"},
    {TokenType::Arrow, "\033[1;33m"},
    {TokenType::QuestionMark, "\033[1;33m"},
    {TokenType::Colon, "\033[1;33m"},
    {TokenType::Equals, "\033[1;33m"},
    {TokenType::NotEquals, "\033[1;33m"},
    {TokenType::Less, "\033[1;33m"},
    {TokenType::Greater, "\033[1;33m"},
    {TokenType::LessOrEquals, "\033[1;33m"},
    {TokenType::GreaterOrEquals, "\033[1;33m"},
    {TokenType::Concatenation, "\033[1;33m"},

    {TokenType::EndOfStream, ""},

    {TokenType::Error, "\033[0;41m"},
    {TokenType::ErrorMultilineCommentIsNotClosed, "\033[0;41m"},
    {TokenType::ErrorSingleQuoteIsNotClosed, "\033[0;41m"},
    {TokenType::ErrorDoubleQuoteIsNotClosed, "\033[0;41m"},
    {TokenType::ErrorBackQuoteIsNotClosed, "\033[0;41m"},
    {TokenType::ErrorSingleExclamationMark, "\033[0;41m"},
    {TokenType::ErrorWrongNumber, "\033[0;41m"},
    {TokenType::ErrorMaxQuerySizeExceeded, "\033[0;41m"},
};


int main(int, char **)
{
    String query;
    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    readStringUntilEOF(query, in);

    Lexer lexer(query.data(), query.data() + query.size());

    while (true)
    {
        Token token = lexer.nextToken();

        if (token.isEnd())
            break;

        writeChar(' ', out);

        auto it = hilite.find(token.type);
        if (it != hilite.end())
            writeCString(it->second, out);

        writeString(token.begin, token.size(), out);

        if (it != hilite.end())
            writeCString("\033[0m", out);

        writeChar(' ', out);

        if (token.isError())
            return 1;
    }

    writeChar('\n', out);
/*
    Tokens tokens(query.data(), query.data() + query.size());
    TokenIterator token(tokens);

    while (token->type.isEnd())
    {
        auto it = hilite.find(token->type);
        if (it != hilite.end())
            writeCString(it->second, out);

        writeString(token->begin, token->size(), out);

        if (it != hilite.end())
            writeCString("\033[0m", out);

        writeChar('\n', out);
        ++token;
    }*/

    return 0;
}
