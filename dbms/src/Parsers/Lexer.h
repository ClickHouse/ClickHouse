#pragma once

#include <stddef.h>


namespace DB
{

enum class TokenType
{
    Whitespace,
    Comment,

    BareWord,               /// Either keyword (SELECT) or identifier (column)

    Number,                 /// Always non-negative. No leading plus. 123 or something like 123.456e12, 0x123p12
    StringLiteral,          /// 'hello word', 'hello''word', 'hello\'word\\'

    QuotedIdentifier,       /// "x", `x`

    OpeningRoundBracket,
    ClosingRoundBracket,

    OpeningSquareBracket,
    ClosingSquareBracket,

    Comma,
    Semicolon,
    Dot,                    /// Compound identifiers, like a.b or tuple access operator a.1, (x, y).2.
                            /// Need to be distinguished from floating point number with omitted integer part: .1

    Asterisk,               /// Could be used as multiplication operator or on it's own: SELECT *

    Plus,
    Minus,
    Slash,
    Percent,
    Arrow,                  /// ->. Should be distinguished from minus operator.
    QuestionMark,
    Colon,
    Equals,
    NotEquals,
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
    Concatenation,          /// String concatenation operator: ||

    /// Order is important. EndOfStream goes after all usual tokens, and special error tokens goes after EndOfStream.

    EndOfStream,

    /// Something unrecognized.
    Error,
    /// Something is wrong and we have more information.
    ErrorMultilineCommentIsNotClosed,
    ErrorSingleQuoteIsNotClosed,
    ErrorDoubleQuoteIsNotClosed,
    ErrorBackQuoteIsNotClosed,
    ErrorSingleExclamationMark,
    ErrorSinglePipeMark,
    ErrorWrongNumber,
};


struct Token
{
    TokenType type;
    const char * begin;
    const char * end;

    size_t size() const { return end - begin; }

    Token() = default;
    Token(TokenType type, const char * begin, const char * end) : type(type), begin(begin), end(end) {}

    bool isSignificant() const { return type != TokenType::Whitespace && type != TokenType::Comment; }
    bool isError() const { return type > TokenType::EndOfStream; }
    bool isEnd() const { return type == TokenType::EndOfStream; }
};


class Lexer
{
public:
    Lexer(const char * begin, const char * end) : begin(begin), pos(begin), end(end) {}
    Token nextToken();

private:
    const char * const begin;
    const char * pos;
    const char * const end;

    Token nextTokenImpl();

    /// This is needed to disambiguate tuple access operator from floating point number (.1).
    TokenType prev_significant_token_type = TokenType::Whitespace;   /// No previous token.
};

}
