#pragma once

#include <stddef.h>

namespace DB
{

#define APPLY_FOR_KQLTOKENS(M) \
    M(Whitespace) \
    M(Comment) \
    \
    M(BareWord)               /** Either keyword (SELECT) or identifier (column) */ \
    \
    M(Number)                 /** Always non-negative. No leading plus. 123 or something like 123.456e12, 0x123p12 */ \
    M(StringLiteral)          /** 'hello word', 'hello''word', 'hello\'word\\' */ \
    \
    M(QuotedIdentifier)       /** "x", `x` */ \
    \
    M(OpeningRoundBracket) \
    M(ClosingRoundBracket) \
    \
    M(OpeningSquareBracket) \
    M(ClosingSquareBracket) \
    \
    M(OpeningCurlyBrace) \
    M(ClosingCurlyBrace) \
    \
    M(Comma) \
    M(Semicolon) \
    M(Minus) \
    M(Equals) \
    M(PipeMark) \
    M(Tilde) \
    M(ExclamationMark) \
    \
    /** Order is important. EndOfStream goes after all usual tokens, and special error tokens goes after EndOfStream. */ \
    \
    M(EndOfStream) \
    \
    /** Something unrecognized. */ \
    M(Error) \
    /** Something is wrong and we have more information. */ \
    M(ErrorMultilineCommentIsNotClosed) \
    M(ErrorSingleQuoteIsNotClosed) \
    M(ErrorDoubleQuoteIsNotClosed) \
    M(ErrorBackQuoteIsNotClosed) \
    M(ErrorMaxQuerySizeExceeded) \


enum class KQLTokenType
{
#define M(TOKEN) TOKEN,
APPLY_FOR_KQLTOKENS(M)
#undef M
};

const char * getTokenName(KQLTokenType type);
const char * getErrorTokenDescription(KQLTokenType type);


struct KQLToken
{
    KQLTokenType type;
    const char * begin;
    const char * end;

    size_t size() const { return end - begin; }

    KQLToken() = default;
    KQLToken(KQLTokenType type_, const char * begin_, const char * end_) : type(type_), begin(begin_), end(end_) {}

    bool isSignificant() const { return type != KQLTokenType::Whitespace && type != KQLTokenType::Comment; }
    bool isError() const { return type > KQLTokenType::EndOfStream; }
    bool isEnd() const { return type == KQLTokenType::EndOfStream; }
};


class KQLLexer
{
public:
    KQLLexer(const char * begin_, const char * end_, size_t max_query_size_ = 0)
            : begin(begin_), pos(begin_), end(end_), max_query_size(max_query_size_) {}
    KQLToken nextToken();

private:
    const char * const begin;
    const char * pos;
    const char * const end;

    const size_t max_query_size;

    KQLToken nextTokenImpl();

    /// This is needed to disambiguate tuple access operator from floating point number (.1).
    KQLTokenType prev_significant_token_type = KQLTokenType::Whitespace;   /// No previous token.
};

}
