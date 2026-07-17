#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <vector>

/** Regression coverage for the `detectExplicitFormat` logic in `programs/server/play.html`.
  *
  * The Web UI decides whether a query has a real `FORMAT` clause - to know whether the page's
  * default format applies (and hence whether to request extremes) and whether to opt out of framing
  * for `JSONCompactColumns`.
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
  * in sync with `detectExplicitFormat` in `programs/server/play.html`.
  */

namespace
{

std::string toLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

/// Mirror of `tokenize` in play.html, keeping only significant tokens (the browser filters
/// `.filter(t => t.significant)`): for each we record the token type and its text.
struct Tok
{
    DB::TokenType type;
    std::string text;
};

std::vector<Tok> tokenizeSignificant(const std::string & query)
{
    DB::Lexer lexer(query.data(), query.data() + query.size(), 65536);
    std::vector<Tok> tokens;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        if (token.isSignificant())
            tokens.push_back({token.type, std::string(token.begin, token.end)});
    }
    return tokens;
}

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

/// Faithful port of `detectExplicitFormat` from play.html. Returns the format name, or `nullopt`
/// when the query has no real `FORMAT` clause.
std::optional<std::string> detectExplicitFormat(const std::string & query)
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
                return tokens[i + 1].text;
            }
        }
    }
    return std::nullopt;
}

void expectFormat(const std::string & query, const std::optional<std::string> & expected)
{
    const std::optional<std::string> result = detectExplicitFormat(query);
    EXPECT_EQ(result, expected) << "query: " << query;
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
