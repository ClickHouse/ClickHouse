#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <Common/re2.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <string_view>
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
  * trailing clause: a `BareWord` `format` at bracket depth 0, preceded by a token that ends an
  * expression (a literal, an identifier, `*`, or a closing bracket - the clause is tried exactly
  * where an alias could appear, mirroring the server parser), immediately followed by the format
  * name (another `BareWord`), after which the query has nothing more except an optional `;` or a
  * trailing `SETTINGS` clause (the `FORMAT` and `SETTINGS` clauses may appear in either order). A plain text
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
    /// `max_query_size = 0` means no limit, exactly like the browser's `tokenize`: the page lexes
    /// whatever the editor holds (the server applies its own limits), and a cap here would flag every
    /// token crossing it as an error and silently truncate the token stream of a big query.
    DB::Lexer lexer(query.data(), query.data() + query.size(), 0);
    std::vector<Tok> tokens;
    const char * base = query.data();
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError())
        {
            /// The browser's `tokenize` also stops at an error token, but a port that stopped
            /// silently would analyze a prefix of the query and report the result as if it were
            /// complete - so a truncated analysis fails the test loudly instead.
            ADD_FAILURE() << "the SQL lexer reported an error token: " << DB::getErrorTokenDescription(token.type);
            break;
        }
        if (token.isEnd())
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

/// Mirror of `OPERAND_EXPECTING_KEYWORDS` in play.html: bare words after which an operand (an
/// expression, or a table/column name) is expected, so a following `format` word is that operand,
/// not the `FORMAT` clause keyword.
bool isOperandExpectingKeyword(const std::string & lower)
{
    static const std::vector<std::string> keywords = {
        "select", "from", "where", "prewhere", "having", "by", "and", "or", "not", "as", "on",
        "using", "in", "when", "then", "else", "case", "distinct", "all", "any", "some", "join",
        "union", "intersect", "except", "with", "settings", "limit", "offset", "top", "interval",
        "like", "ilike", "between", "is", "over", "global", "array", "to", "if", "mod", "div",
        "cross", "inner", "outer", "left", "right", "full", "asof", "semi", "anti", "paste",
        "apply", "lateral", "sample", "into", "values",
    };
    return std::find(keywords.begin(), keywords.end(), lower) != keywords.end();
}

/// Mirror of `endsExpression` in play.html: whether the token ENDS an expression, so the `FORMAT`
/// clause may begin right after it - a literal, an identifier, `*`, or a closing bracket. The
/// server tries the `FORMAT` clause exactly where an alias could appear (only after a complete
/// expression), so a `format` word elsewhere is an identifier in the query body.
bool endsExpression(const Tok * tok)
{
    if (!tok)
        return false;
    if (tok->type == DB::TokenType::Number || tok->type == DB::TokenType::StringLiteral
        || tok->type == DB::TokenType::QuotedIdentifier || tok->type == DB::TokenType::Asterisk
        || isClosingBracket(tok->type))
        return true;
    return tok->type == DB::TokenType::BareWord && !isOperandExpectingKeyword(toLower(tok->text));
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
            && (tokens[i + 1].type == DB::TokenType::BareWord || tokens[i + 1].type == DB::TokenType::QuotedIdentifier)
            && endsExpression(i > 0 ? &tokens[i - 1] : nullptr))
        {
            /// A real `FORMAT` clause is the last clause of the statement: only `;` or a trailing
            /// `SETTINGS` list may follow the format name.
            const bool has_after = i + 2 < tokens.size();
            if (!has_after
                || tokens[i + 2].type == DB::TokenType::Semicolon
                || (tokens[i + 2].type == DB::TokenType::BareWord && toLower(tokens[i + 2].text) == "settings"))
            {
                /// The server parses the format name with an identifier parser, so a backquoted
                /// spelling is a real clause too; report the unquoted name while the span keeps the
                /// quotes so the download strips the whole clause.
                std::string name = tokens[i + 1].text;
                if (tokens[i + 1].type == DB::TokenType::QuotedIdentifier && name.size() >= 2)
                    name = name.substr(1, name.size() - 2);
                return FormatClause{name, t.start, tokens[i + 1].end};
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

/// Mirror of the no-WebAssembly fallback of `detectExplicitFormatClause` in play.html: without the
/// lexer the page falls back to a best-effort text match anchored to a real trailing clause (only `;`
/// or `SETTINGS` may follow the name). The browser anchors the tail with a lookahead, which re2 does
/// not support, so this port consumes the tail inside the match instead and takes the clause span
/// from a capture group wrapped around the keyword and the name - the acceptance is the same, and
/// for a single search so is the reported span. Like the lexer branch, the name is reported unquoted
/// while the span keeps the quotes, so the download strips the whole clause.
std::optional<FormatClause> detectExplicitFormatClauseNoLexer(const std::string & query)
{
    /// A custom raw-string delimiter: the pattern itself contains `)"`.
    static const re2::RE2 re(R"RE((?i)(\bFORMAT\s+(?:`([^`]+)`|"([^"]+)"|(\w+)))\s*(?:;|\bSETTINGS\b|$))RE");
    /// [0] = whole match (including the consumed tail), [1] = the clause span, [2]-[4] = the name.
    std::string_view groups[5];
    if (!re.Match({query.data(), query.size()}, 0, query.size(), re2::RE2::UNANCHORED, groups, 5))
        return std::nullopt;
    const std::string_view name = !groups[2].empty() ? groups[2] : (!groups[3].empty() ? groups[3] : groups[4]);
    const size_t start = static_cast<size_t>(groups[1].data() - query.data());
    return FormatClause{std::string(name), start, start + groups[1].size()};
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

TEST(PlayDetectExplicitFormat, QuotedFormatNameIsARealClause)
{
    /// The reported bug: the server parses the format name with an identifier parser, so a quoted
    /// spelling of the name is a real clause. A detector that requires a bare word would miss it:
    /// the page would then add its own framing (losing e.g. the chart path of `JSONCompactColumns`)
    /// and the download would fail to strip the clause. The reported name is unquoted, while the
    /// stripped span covers the quotes.
    expectFormat("SELECT 1 FORMAT `JSON`", "JSON");
    expectFormat("SELECT * FROM system.numbers LIMIT 1 FORMAT `JSONCompactColumns`", "JSONCompactColumns");
    expectFormat("SELECT 1 FORMAT \"TSV\"", "TSV");
    expectFormat("SELECT 1 FORMAT `TSV` SETTINGS max_threads = 1", "TSV");
    expectFormat("SELECT 1 FORMAT `JSON`;", "JSON");
    expectStrip("SELECT 1 FORMAT `JSON`", "SELECT 1 ");
    expectStrip("SELECT 1 FORMAT `TSV` SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1");
    /// A quoted identifier in the query body is still not a clause: after `SELECT` an operand is
    /// expected, so a backquoted word there is a column, aliased by the next word.
    expectFormat("SELECT format `JSONCompactColumns` FROM values('format UInt8', (1))", std::nullopt);
    /// A quoted `format` word is an identifier, never the clause keyword - even in trailing
    /// position (`JSON` is then its alias).
    expectFormat("SELECT `format` JSON", std::nullopt);
}

TEST(PlayDetectExplicitFormat, NoLexerFallbackAcceptsAQuotedFormatName)
{
    /// A browser without WebAssembly has no lexer, so the page falls back to a text match. It must
    /// honor the same contract as the lexer branch for the quoted spellings the server accepts:
    /// otherwise `FORMAT `JSONCompactColumns`` still looks like "no explicit format" there, the page
    /// adds its own `EventStream` framing, and the download does not strip the real clause.
    const auto name = [](const std::string & query) -> std::optional<std::string>
    {
        const std::optional<FormatClause> clause = detectExplicitFormatClauseNoLexer(query);
        if (clause)
            return clause->name;
        return std::nullopt;
    };
    const auto strip = [](const std::string & query)
    {
        const std::optional<FormatClause> clause = detectExplicitFormatClauseNoLexer(query);
        if (!clause)
            return query;
        return query.substr(0, clause->start) + query.substr(clause->end);
    };

    /// The bare-word spelling kept working all along.
    EXPECT_EQ(name("SELECT 1 FORMAT JSON"), std::optional<std::string>("JSON"));
    /// The regression: quoted names, in both spellings and with either trailing clause terminator.
    EXPECT_EQ(name("SELECT 1 FORMAT `JSON`"), std::optional<std::string>("JSON"));
    EXPECT_EQ(name("SELECT * FROM system.numbers LIMIT 1 FORMAT `JSONCompactColumns`"),
              std::optional<std::string>("JSONCompactColumns"));
    EXPECT_EQ(name("SELECT 1 FORMAT \"TSV\""), std::optional<std::string>("TSV"));
    EXPECT_EQ(name("SELECT 1 FORMAT `JSON`;"), std::optional<std::string>("JSON"));
    EXPECT_EQ(name("SELECT 1 FORMAT `TSV` SETTINGS max_threads = 1"), std::optional<std::string>("TSV"));
    /// The span covers the quotes, so the download strips the whole clause.
    EXPECT_EQ(strip("SELECT 1 FORMAT `JSON`"), "SELECT 1 ");
    EXPECT_EQ(strip("SELECT 1 FORMAT `TSV` SETTINGS max_threads = 1"), "SELECT 1  SETTINGS max_threads = 1");
    /// Still anchored to a trailing clause: a `FORMAT` mention followed by more query text is not one.
    EXPECT_EQ(name("SELECT 1 FORMAT `JSON` FROM t"), std::nullopt);
    EXPECT_EQ(name("SELECT 1"), std::nullopt);
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

TEST(PlayDetectExplicitFormat, AliasedIdentifierBeforeSettingsIsNotAFormatClause)
{
    /// The reported bug: with a leading `WITH` alias, `format JSONCompactColumns` sits in trailing
    /// position (only a `SETTINGS` clause follows), yet `format` is still just an identifier - it
    /// follows `SELECT`, where an expression is expected, so the server parses `JSONCompactColumns`
    /// as its alias, not as an output format.
    expectFormat("WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1", std::nullopt);
    expectStrip("WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1",
        "WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1");
    /// The same shape with a real trailing clause (after a complete expression) is still detected.
    expectFormat("WITH 1 AS x SELECT x FORMAT JSONCompactColumns SETTINGS max_threads = 1", "JSONCompactColumns");
}

TEST(PlayDetectExplicitFormat, ClauseAfterTrailingKeywordsIsStillDetected)
{
    /// Keywords that END a clause (unlike `SELECT`/`AS`/`BY`, after which an operand is expected)
    /// may legitimately precede the `FORMAT` clause.
    expectFormat("SELECT count() FROM t GROUP BY x WITH TOTALS FORMAT JSON", "JSON");
    expectFormat("SELECT 1 ORDER BY 1 DESC FORMAT JSON", "JSON");
    expectFormat("SELECT number FROM numbers(10) LIMIT 3 WITH TIES FORMAT JSON", "JSON");
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

TEST(PlayDetectExplicitFormat, LargeQueryIsTokenizedWithoutALimit)
{
    /// The reported bug: the browser tokenizer used to cap the lexer at `max_query_size = 65536`,
    /// which flagged every token crossing that boundary as an error and silently truncated the token
    /// stream - so a `FORMAT` clause behind the cap was invisible and the page applied its own
    /// default format to a query that had chosen one. The page now lexes without a limit (the server
    /// applies its own), and so does the helper above. A padding comment keeps the query valid while
    /// pushing the clause past 64 KiB.
    const std::string padding(70000, 'x');
    expectFormat("SELECT 1 /* " + padding + " */ FORMAT JSONCompactColumns", "JSONCompactColumns");
    expectFormat("SELECT 1 /* " + padding + " */ FORMAT TSV SETTINGS max_threads = 1", "TSV");
    /// A `FORMAT` mention past the old cap that is only text is still not a clause.
    expectFormat("SELECT '" + padding + " FORMAT JSON' AS s", std::nullopt);
    /// The strip is span-based, so it removes only that far-away clause.
    expectStrip("SELECT 1 /* " + padding + " */ FORMAT JSON", "SELECT 1 /* " + padding + " */ ");
}
