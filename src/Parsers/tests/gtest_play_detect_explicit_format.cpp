#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <vector>

/** Regression coverage for the `detectExplicitFormat` / `detectExplicitFormatClause` logic in
  * `programs/server/play.html`.
  *
  * The Web UI decides whether a query has a real `FORMAT` clause - to know whether the page's
  * default format applies (and hence whether to request extremes) and whether to opt out of framing
  * for `JSONCompactColumns`. The download handler additionally strips only that real clause span, so
  * the download's `default_format` applies while the rest of the query stays byte-for-byte intact
  * (a raw regex would rewrite a `FORMAT ...` that is only text or ordinary SQL and download the
  * result of a different query).
  *
  * The detection tokenizes the query with the ClickHouse `Lexer` (compiled to WebAssembly from
  * `src/Parsers/Lexer.cpp` - the very same source exercised here) and counts `FORMAT` only as a real
  * trailing clause: a `BareWord` `format` at bracket depth 0, immediately followed by the format name
  * (another `BareWord`), after which the query has nothing more except an optional `;` or a trailing
  * `SETTINGS` clause (the `FORMAT` and `SETTINGS` clauses may appear in either order). A plain text
  * match is fooled by a `FORMAT` mention inside a string literal or a comment, e.g.
  * `SELECT 'FORMAT JSONCompactColumns'`, and - crucially - by a column named `format` in the query
  * body, e.g. `SELECT format JSONCompactColumns FROM values('format UInt8', (1))` (a column aliased
  * as `JSONCompactColumns`). Either would be taken as a real clause and silently drop the page's own
  * `EventStream` request. Walking the lexer tokens with the trailing-clause constraint ignores such
  * occurrences.
  *
  * There is no JavaScript/WebAssembly runtime in CI, so we cannot run the browser code directly.
  * Instead we reproduce the token-walking algorithm here on top of the real `DB::Lexer`. The lexer
  * (the part most likely to evolve) is shared; only the small detection below is a port. Keep this
  * in sync with `detectExplicitFormatClause` / `detectExplicitFormat` in `programs/server/play.html`.
  */

namespace
{

std::string toLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

/// Mirror of `tokenize` in play.html, keeping only significant tokens (the browser filters
/// `.filter(t => t.significant)`): for each we record the token type, its text, and the character
/// span `[start, end)` in the query. The browser derives the span by summing the length of every
/// token (significant or not); here the lexer gives us the byte offsets directly, which coincide
/// with the JS UTF-16 offsets for the ASCII queries covered below.
struct Tok
{
    DB::TokenType type;
    std::string text;
    size_t start;
    size_t end;
};

std::vector<Tok> tokenizeSignificant(const std::string & query)
{
    DB::Lexer lexer(query.data(), query.data() + query.size(), 65536);
    std::vector<Tok> tokens;
    const char * base = query.data();
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        if (token.isSignificant())
            tokens.push_back({token.type, std::string(token.begin, token.end),
                static_cast<size_t>(token.begin - base), static_cast<size_t>(token.end - base)});
    }
    return tokens;
}

/// The format name and the character span of a real `FORMAT <name>` clause.
struct FormatClause
{
    std::string name;
    size_t start;
    size_t end;
};

/// Mirror of `OPENING_BRACKETS` / `CLOSING_BRACKETS` in play.html.
bool isOpeningBracket(DB::TokenType type)
{
    return type == DB::TokenType::OpeningRoundBracket
        || type == DB::TokenType::OpeningSquareBracket
        || type == DB::TokenType::OpeningCurlyBrace;
}

bool isClosingBracket(DB::TokenType type)
{
    return type == DB::TokenType::ClosingRoundBracket
        || type == DB::TokenType::ClosingSquareBracket
        || type == DB::TokenType::ClosingCurlyBrace;
}

/// Faithful port of `detectExplicitFormatClause` from play.html. Returns the format name and the
/// span of the whole `FORMAT <name>` clause, or `nullopt` when the query has no real `FORMAT` clause.
std::optional<FormatClause> detectExplicitFormatClause(const std::string & query)
{
    const std::vector<Tok> tokens = tokenizeSignificant(query);
    int depth = 0;
    for (size_t i = 0; i + 1 < tokens.size(); ++i)
    {
        const Tok & t = tokens[i];
        if (isOpeningBracket(t.type))
        {
            ++depth;
        }
        else if (isClosingBracket(t.type))
        {
            if (depth > 0)
                --depth;
        }
        else if (depth == 0
            && t.type == DB::TokenType::BareWord
            && toLower(t.text) == "format"
            && tokens[i + 1].type == DB::TokenType::BareWord)
        {
            /// A real `FORMAT` clause is the last clause of the statement: only `;` or a trailing
            /// `SETTINGS` list may follow the format name.
            const bool has_after = i + 2 < tokens.size();
            if (!has_after
                || tokens[i + 2].type == DB::TokenType::Semicolon
                || (tokens[i + 2].type == DB::TokenType::BareWord && toLower(tokens[i + 2].text) == "settings"))
            {
                return FormatClause{tokens[i + 1].text, t.start, tokens[i + 1].end};
            }
        }
    }
    return std::nullopt;
}

/// Thin wrapper mirroring `detectExplicitFormat` in play.html (name only).
std::optional<std::string> detectExplicitFormat(const std::string & query)
{
    const std::optional<FormatClause> clause = detectExplicitFormatClause(query);
    if (clause)
        return clause->name;
    return std::nullopt;
}

/// Mirror of the download handler's strip: remove only the real trailing `FORMAT` clause span, so
/// the rest of the query is left byte-for-byte intact (a plain regex would rewrite ordinary SQL).
std::string stripExplicitFormat(const std::string & query)
{
    const std::optional<FormatClause> clause = detectExplicitFormatClause(query);
    if (!clause)
        return query;
    return query.substr(0, clause->start) + query.substr(clause->end);
}

void expectFormat(const std::string & query, const std::optional<std::string> & expected)
{
    const std::optional<std::string> result = detectExplicitFormat(query);
    EXPECT_EQ(result, expected) << "query: " << query;
}

void expectStrip(const std::string & query, const std::string & expected)
{
    EXPECT_EQ(stripExplicitFormat(query), expected) << "query: " << query;
}

}

TEST(PlayDetectExplicitFormat, NoFormatClause)
{
    expectFormat("SELECT 1", std::nullopt);
    expectFormat("SELECT 1 SETTINGS max_threads = 4", std::nullopt);
    expectFormat("", std::nullopt);
    /// `formatDateTime` is a single identifier, not the `FORMAT` keyword.
    expectFormat("SELECT formatDateTime(now(), '%Y')", std::nullopt);
}

TEST(PlayDetectExplicitFormat, RealFormatClause)
{
    expectFormat("SELECT 1 FORMAT JSON", "JSON");
    expectFormat("SELECT * FROM system.numbers LIMIT 1 FORMAT JSONCompactColumns", "JSONCompactColumns");
    /// Case-insensitive keyword.
    expectFormat("select 1 format PrettyCompact", "PrettyCompact");
    /// An `INSERT ... FORMAT` clause is a real clause too.
    expectFormat("INSERT INTO t FORMAT CSV", "CSV");
    /// A format name that is also a SQL keyword (`Values`) is still returned.
    expectFormat("INSERT INTO t FORMAT Values", "Values");
    /// A trailing `;` still ends the clause.
    expectFormat("SELECT 1 FORMAT JSON;", "JSON");
    /// The `SETTINGS` clause may follow the `FORMAT` clause.
    expectFormat("SELECT 1 FORMAT TSV SETTINGS max_threads = 1", "TSV");
}

TEST(PlayDetectExplicitFormat, StringLiteralIsNotAFormatClause)
{
    /// The reported bug: a `FORMAT` mention inside a string literal must not be treated as a real
    /// clause, otherwise the page opts out of its own framing.
    expectFormat("SELECT 'FORMAT JSONCompactColumns'", std::nullopt);
    expectFormat("SELECT 'FORMAT JSON' AS x", std::nullopt);
}

TEST(PlayDetectExplicitFormat, IdentifierInBodyIsNotAFormatClause)
{
    /// The reported bug: `format <name>` in the query body (a column named `format` with an alias)
    /// is not an output `FORMAT` clause - more of the query follows the candidate name.
    expectFormat("SELECT format JSONCompactColumns FROM values('format UInt8', (1))", std::nullopt);
    expectFormat("SELECT format AS x FROM t", std::nullopt);
}

TEST(PlayDetectExplicitFormat, CommentIsNotAFormatClause)
{
    expectFormat("SELECT 1 -- FORMAT JSON\n", std::nullopt);
    expectFormat("SELECT 1 /* FORMAT JSONCompactColumns */", std::nullopt);
}

TEST(PlayDetectExplicitFormat, RealClauseWinsOverStringMention)
{
    /// A real clause alongside a string-literal mention is still detected.
    expectFormat("SELECT 'FORMAT JSON' FORMAT JSONCompactColumns", "JSONCompactColumns");
}

TEST(PlayDetectExplicitFormat, StripRemovesOnlyTheRealClause)
{
    /// The download handler strips only the real trailing `FORMAT` clause span so the download's
    /// `default_format` applies; the rest of the query stays byte-for-byte intact.
    expectStrip("SELECT 1 FORMAT JSON", "SELECT 1 ");
    expectStrip("SELECT 1 FORMAT JSON;", "SELECT 1 ;");
    /// A trailing `SETTINGS` clause is preserved; only `FORMAT <name>` is removed.
    expectStrip("SELECT 1 FORMAT TSV SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1");
    /// A real clause alongside a string mention removes only the real clause.
    expectStrip("SELECT 'FORMAT JSON' FORMAT JSONCompactColumns", "SELECT 'FORMAT JSON' ");
}

TEST(PlayDetectExplicitFormat, StripLeavesOrdinarySqlUnchanged)
{
    /// The reported bug: a raw `replaceAll(/\bFORMAT\s+\w+/)` would rewrite a `FORMAT ...` that is
    /// only text or ordinary SQL, downloading a different query than the one that ran. The span-based
    /// strip leaves such queries untouched (there is no real `FORMAT` clause to remove).
    expectStrip("SELECT 'FORMAT TSV' AS s", "SELECT 'FORMAT TSV' AS s");
    expectStrip("SELECT format JSONCompactColumns FROM values('format UInt8', (1))",
        "SELECT format JSONCompactColumns FROM values('format UInt8', (1))");
    expectStrip("SELECT 1", "SELECT 1");
}
