#pragma once

#include <base/types.h>

#include <optional>
#include <vector>


namespace DB
{

/** Token types of the Kusto Query Language.
  *
  * KQL is lexically distinct from ClickHouse SQL, so it gets its own lexer rather than
  * reusing `DB::Lexer`. The previous implementation shared the SQL lexer and recovered
  * KQL-only spellings by reinterpreting *error* tokens: `!in` came back as
  * `ErrorSingleExclamationMark`, the timespan `2.6h` as `ErrorWrongNumber`. Every consumer
  * then had to ask `isValidKQLPos()` instead of `isValid()`, which is how malformed input
  * kept reaching code that assumed a well-formed token stream.
  */
enum class KQLTokenType : uint8_t
{
    BareWord, /// Identifier or keyword. Also the negated word operators (`!in`, `!contains`, ...).
    Number,
    StringLiteral,
    Timespan, /// `1d`, `2.5h`, `500ms` - value carried in `timespan_ticks`.
    DateTimeLiteral, /// `datetime(2020-01-01 10:00)` - inner text carried in `inner`.
    GuidLiteral, /// `guid(74be27de-1e4e-49d9-b579-fe0b331d3642)`.

    Pipe,
    Comma,
    Semicolon,
    Dot,
    DotDot,
    Colon,

    OpeningRoundBracket,
    ClosingRoundBracket,
    OpeningSquareBracket,
    ClosingSquareBracket,
    OpeningCurlyBrace,
    ClosingCurlyBrace,

    Plus,
    Minus,
    Asterisk,
    Slash,
    Percent,

    Equals, /// `=`
    DoubleEquals, /// `==`
    NotEquals, /// `!=` or `<>`
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
    TildeEquals, /// `=~`
    NotTildeEquals, /// `!~`

    EndOfStream,

    /// Anything the lexer could not make sense of. Carries a human-readable reason so the
    /// parser can report it verbatim; there is no "error token that actually means something"
    /// path in this lexer.
    Error,
};

const char * getKQLTokenName(KQLTokenType type);

struct KQLToken
{
    KQLTokenType type = KQLTokenType::EndOfStream;

    /// Extent in the original query text. Always valid, so error messages can point at it.
    const char * begin = nullptr;
    const char * end = nullptr;

    /// Decoded payload for tokens whose text is not their value.
    ///  - StringLiteral: the unescaped contents, without quotes.
    ///  - DateTimeLiteral / GuidLiteral: the text between the parentheses.
    ///  - Error: the reason.
    String inner;

    /// Timespan: the value in 100-nanosecond ticks, the unit KQL itself uses.
    Int64 timespan_ticks = 0;

    std::string_view text() const { return {begin, static_cast<size_t>(end - begin)}; }
    bool isEnd() const { return type == KQLTokenType::EndOfStream; }
    bool isError() const { return type == KQLTokenType::Error; }
};

/** Converts KQL source text into a token vector in one pass.
  *
  * The lexer never throws: a malformed literal produces a single `Error` token carrying the
  * reason, and lexing stops there. Deciding what to do about it is the parser's job.
  */
class KQLLexer
{
public:
    KQLLexer(const char * begin_, const char * end_) : pos(begin_), begin(begin_), end(end_) { }

    std::vector<KQLToken> tokenize();

private:
    KQLToken nextToken();
    KQLToken makeToken(KQLTokenType type, const char * token_begin) const;
    KQLToken makeError(const char * token_begin, String reason) const;

    /// `datetime(...)` / `guid(...)`: the parentheses hold text the ordinary rules would
    /// mangle (`2020-01-01` is not three numbers and two minus signs).
    KQLToken lexParenthesizedLiteral(const char * token_begin, KQLTokenType type);
    KQLToken lexNumberOrTimespan(const char * token_begin);
    KQLToken lexString(const char * token_begin, char quote, bool verbatim);
    KQLToken lexBareWord(const char * token_begin);

    void skipWhitespaceAndComments();
    bool atLineStart() const;

    const char * pos;
    const char * const begin;
    const char * const end;
};

/// Ticks (100 ns) per KQL timespan unit, or 0 if `unit` is not one. Shared with the parser,
/// which accepts the same spellings in `totimespan()` and friends.
Int64 kqlTimespanUnitInTicks(std::string_view unit);

/// Reads `[-][d.]hh:mm:ss[.fffffff]` - the way Kusto writes a timespan as a string - and
/// returns the value in ticks. Shared between the parser (`timespan('...')`) and the runtime
/// `kqlToTimespan` function, which applies the same reading per row.
std::optional<Int64> kqlParseTimespanText(std::string_view text);

}
