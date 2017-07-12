#pragma once


namespace DB
{

enum class TokenType
{
    Whitespace,
    Comment,

    BareWord,               /// Either keyword (SELECT) or identifier (column)

    Number,                 /// Always non-negative. 123 or something like 123.456e12, 0x123p12
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
    Concatenation,          /// ||

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

    auto size() const { return end - begin; }

    Token() = default;
    Token(TokenType type, const char * begin, const char * end) : type(type), begin(begin), end(end) {}
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

    void skipWhitespacesAndComments();
};

}
