#include <gtest/gtest.h>

#include <Parsers/Lexer.h>
#include <Parsers/tests/play_fallback_tokenizer.h>

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
  *
  * The browser has a second tokenizer: when WebAssembly is unavailable (or the WASM lexer failed),
  * `fallbackTokenize` produces a compatible token list in plain JS and the very same walk runs over
  * it. Every case here therefore runs through both tokenizations (see `expectFormat` /
  * `expectStrip`), pinning the agreement of the two paths - the regex heuristic this replaced took
  * `WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1` and
  * `SELECT 1 -- FORMAT JSON` for a real trailing clause.
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
            tokens.push_back(
                {token.type,
                 std::string(token.begin, token.end),
                 static_cast<size_t>(token.begin - base),
                 static_cast<size_t>(token.end - base)});
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
    return type == DB::TokenType::OpeningRoundBracket || type == DB::TokenType::OpeningSquareBracket
        || type == DB::TokenType::OpeningCurlyBrace;
}

bool isClosingBracket(DB::TokenType type)
{
    return type == DB::TokenType::ClosingRoundBracket || type == DB::TokenType::ClosingSquareBracket
        || type == DB::TokenType::ClosingCurlyBrace;
}

/// Mirror of `OPERAND_EXPECTING_KEYWORDS` in play.html: bare words after which an operand (an
/// expression, or a table/column name) is expected, so a following `format` word is that operand,
/// not the `FORMAT` clause keyword.
bool isOperandExpectingKeyword(const std::string & lower)
{
    static const std::vector<std::string> keywords = {
        "select", "from",  "where",    "prewhere", "having", "by",       "and",      "or",    "not",    "as",      "on",    "using",
        "in",     "when",  "then",     "else",     "case",   "distinct", "all",      "any",   "some",   "join",    "union", "intersect",
        "except", "with",  "settings", "limit",    "offset", "top",      "interval", "like",  "ilike",  "between", "is",    "over",
        "global", "array", "to",       "if",       "mod",    "div",      "cross",    "inner", "outer",  "left",    "right", "full",
        "asof",   "semi",  "anti",     "paste",    "apply",  "lateral",  "sample",   "into",  "values",
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
    if (tok->type == DB::TokenType::Number || tok->type == DB::TokenType::StringLiteral || tok->type == DB::TokenType::QuotedIdentifier
        || tok->type == DB::TokenType::Asterisk || isClosingBracket(tok->type))
        return true;
    return tok->type == DB::TokenType::BareWord && !isOperandExpectingKeyword(toLower(tok->text));
}

/// Mirror of the no-WebAssembly path: the same walk over `fallbackTokenize`'s tokens (see
/// `play_fallback_tokenizer.h`). The browser derives the character spans by summing the length of
/// every token, so the offsets are exact in both paths; the same accumulation happens here.
std::vector<Tok> fallbackTokenizeSignificant(const std::string & query)
{
    std::vector<Tok> tokens;
    size_t offset = 0;
    for (const auto & token : PlayFallbackTokenizer::tokenize(query))
    {
        if (token.significant)
            tokens.push_back({token.type, token.text, offset, offset + token.text.size()});
        offset += token.text.size();
    }
    return tokens;
}

/// Mirror of `tokensBeforeInlineInsertPayload` in play.html. `ParserInsertQuery` treats bytes after
/// `INSERT ... FORMAT <input format>` as rows, including `INSERT ... SELECT input(...) FORMAT ...`,
/// so SQL-looking payload must not reach this walker.
std::vector<Tok> tokensBeforeInlineInsertPayload(const std::vector<Tok> & tokens)
{
    int depth = 0;
    bool saw_insert = false;
    bool leading_with = false;
    bool leading_explain = false;
    int with_depth = -1;
    bool alias_follows = false;
    bool saw_select_source = false;
    bool select_reads_inline_data = false;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const Tok & t = tokens[i];
        if (saw_select_source && t.type == DB::TokenType::BareWord && toLower(t.text) == "input" && i + 1 < tokens.size()
            && tokens[i + 1].type == DB::TokenType::OpeningRoundBracket)
            select_reads_inline_data = true;
        if (isOpeningBracket(t.type))
        {
            alias_follows = false;
            ++depth;
            continue;
        }
        if (isClosingBracket(t.type))
        {
            alias_follows = false;
            if (depth > 0)
                --depth;
            continue;
        }
        if (depth != 0 || t.type != DB::TokenType::BareWord)
        {
            alias_follows = false;
            continue;
        }

        const std::string lower = toLower(t.text);
        if (!saw_insert)
        {
            if (leading_with && depth == with_depth)
            {
                if (alias_follows)
                {
                    alias_follows = false;
                    continue;
                }
                if (lower == "as")
                {
                    alias_follows = true;
                    continue;
                }
                if (i + 1 < tokens.size() && tokens[i + 1].type == DB::TokenType::BareWord && toLower(tokens[i + 1].text) == "as")
                    continue;
            }
            if (lower == "insert")
            {
                saw_insert = true;
                continue;
            }
            if (!leading_with && lower == "with")
            {
                leading_with = true;
                with_depth = depth;
                continue;
            }
            if (!leading_with && !leading_explain && lower == "explain")
            {
                leading_explain = true;
                continue;
            }
            /// Enumerating the statements that can follow is hopeless - `ParserExplainQuery` hands
            /// `EXPLAIN AST ...` to the full `ParserQuery` - so keep walking only while the prefix
            /// itself is unresolved. A `WITH` prefix has nothing left here: its aliases are already
            /// consumed above, so any other bare word resolves the statement.
            if (leading_explain && !leading_with)
            {
                /// The `EXPLAIN` kinds of `ParserExplainQuery`, spelled as individual words.
                static const std::vector<std::string> explain_kind_words = {
                    "ast", "syntax", "query", "tree", "pipeline", "plan", "estimate", "table", "override",
                    "current", "transaction", "analyze", "whatif"};
                if (std::find(explain_kind_words.begin(), explain_kind_words.end(), lower) != explain_kind_words.end())
                    continue;
                /// The optional settings list between the kind and the explained query: each entry
                /// is `name = value`, so a name is followed by `=` and a bare-word value precedes one.
                if (i + 1 < tokens.size() && tokens[i + 1].type == DB::TokenType::Equals)
                    continue;
                if (i > 0 && tokens[i - 1].type == DB::TokenType::Equals)
                    continue;
            }
            return tokens;
        }
        if (lower == "select" || lower == "with" || lower == "from")
        {
            saw_select_source = true;
            continue;
        }
        if (lower == "format" && i + 1 < tokens.size()
            && (tokens[i + 1].type == DB::TokenType::BareWord || tokens[i + 1].type == DB::TokenType::QuotedIdentifier))
        {
            if (saw_select_source && !select_reads_inline_data)
                return tokens;
            return {tokens.begin(), tokens.begin() + i};
        }
    }
    return tokens;
}

/// Faithful port of `detectExplicitFormatClause` from play.html, over an already-produced token list
/// (the browser runs the same walk over the WASM lexer's tokens and, when the lexer is unavailable,
/// over `fallbackTokenize`'s - see `expectFormat` / `expectStrip`, which exercise both here).
/// Returns the format name and the span of the whole `FORMAT <name>` clause, or `nullopt` when the
/// query has no real `FORMAT` clause.
std::optional<FormatClause> detectExplicitFormatClause(const std::vector<Tok> & all_tokens)
{
    const std::vector<Tok> tokens = tokensBeforeInlineInsertPayload(all_tokens);
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
        else if (
            depth == 0 && t.type == DB::TokenType::BareWord && toLower(t.text) == "format"
            && (tokens[i + 1].type == DB::TokenType::BareWord || tokens[i + 1].type == DB::TokenType::QuotedIdentifier)
            && endsExpression(i > 0 ? &tokens[i - 1] : nullptr))
        {
            /// A real `FORMAT` clause is the last clause of the statement: only `;` or a trailing
            /// `SETTINGS` list may follow the format name.
            const bool has_after = i + 2 < tokens.size();
            if (!has_after || tokens[i + 2].type == DB::TokenType::Semicolon
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
std::optional<std::string> detectExplicitFormat(const std::vector<Tok> & tokens)
{
    const std::optional<FormatClause> clause = detectExplicitFormatClause(tokens);
    if (clause)
        return clause->name;
    return std::nullopt;
}

/// Mirror of the download handler's strip: remove only the real trailing `FORMAT` clause span, so
/// the rest of the query is left byte-for-byte intact (a plain regex would rewrite ordinary SQL).
std::string stripExplicitFormat(const std::string & query, const std::vector<Tok> & tokens)
{
    const std::optional<FormatClause> clause = detectExplicitFormatClause(tokens);
    if (!clause)
        return query;
    return query.substr(0, clause->start) + query.substr(clause->end);
}

/// Every case runs through BOTH tokenizations: the real `DB::Lexer` (the WASM path) and the fallback
/// tokenizer (the no-WebAssembly path). The regex heuristic the fallback replaced reintroduced
/// exactly the context bugs the walk fixes - `WITH 1 AS format SELECT format JSONCompactColumns
/// SETTINGS max_threads = 1` and `SELECT 1 -- FORMAT JSON` both looked like a real trailing clause,
/// so the page suppressed its own framing or the download stripped bytes out of ordinary SQL.
/// Running the same walk over fallback tokens keeps the two paths in agreement by construction, and
/// these assertions pin it for the whole corpus (negatives included).
void expectFormat(const std::string & query, const std::optional<std::string> & expected)
{
    EXPECT_EQ(detectExplicitFormat(tokenizeSignificant(query)), expected) << "query: " << query;
    EXPECT_EQ(detectExplicitFormat(fallbackTokenizeSignificant(query)), expected) << "fallback tokenizer, query: " << query;
}

void expectStrip(const std::string & query, const std::string & expected)
{
    EXPECT_EQ(stripExplicitFormat(query, tokenizeSignificant(query)), expected) << "query: " << query;
    EXPECT_EQ(stripExplicitFormat(query, fallbackTokenizeSignificant(query)), expected) << "fallback tokenizer, query: " << query;
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
    /// `INSERT ... FORMAT` starts inline input data, not an output format clause.
    expectFormat("INSERT INTO t FORMAT CSV", std::nullopt);
    /// The input format may itself be a SQL keyword.
    expectFormat("INSERT INTO t FORMAT Values", std::nullopt);
    /// A trailing `;` still ends the clause.
    expectFormat("SELECT 1 FORMAT JSON;", "JSON");
    /// The `SETTINGS` clause may follow the `FORMAT` clause.
    expectFormat("SELECT 1 FORMAT TSV SETTINGS max_threads = 1", "TSV");
}

TEST(PlayDetectExplicitFormat, LeadingWithAndExplainReadOnlyStatementsDoNotBecomeInsert)
{
    /// A prefix walk must stop after it has resolved `WITH` / `EXPLAIN` to any non-`INSERT`
    /// statement. Otherwise an ordinary identifier named `insert` in a later table position makes
    /// the walker treat the real trailing clause as inline insert input.
    expectFormat("EXPLAIN AST DESCRIBE TABLE insert FORMAT TabSeparated", "TabSeparated");
    expectFormat("WITH 1 AS x SHOW CREATE TABLE insert FORMAT TabSeparated", "TabSeparated");
    expectFormat("WITH 1 AS x EXISTS TABLE insert FORMAT TabSeparated", "TabSeparated");
    /// `EXPLAIN AST` is parsed by the full `ParserQuery`, so the statement it wraps can be any
    /// kind at all. The walk must not depend on a list of statement starters.
    expectFormat("EXPLAIN AST DETACH TABLE insert FORMAT TabSeparated", "TabSeparated");
    expectFormat("EXPLAIN AST UNDROP TABLE insert FORMAT TabSeparated", "TabSeparated");
    expectFormat("EXPLAIN AST ATTACH TABLE insert FORMAT TabSeparated", "TabSeparated");
    /// The true wrapped `INSERT` case is still an input format, never an output clause.
    expectFormat("EXPLAIN AST INSERT INTO t FORMAT TabSeparated", std::nullopt);
    expectFormat("EXPLAIN INSERT INTO t FORMAT TabSeparated", std::nullopt);
    expectFormat("WITH 1 AS x INSERT INTO t FORMAT TabSeparated", std::nullopt);
    /// The settings list `ParserExplainQuery` accepts between the kind and the explained query
    /// keeps the prefix unresolved.
    expectFormat("EXPLAIN PLAN header = 1 INSERT INTO t FORMAT TabSeparated", std::nullopt);
    expectFormat("EXPLAIN PLAN header = true INSERT INTO t FORMAT TabSeparated", std::nullopt);
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

TEST(PlayDetectExplicitFormat, NoLexerFallbackRunsTheSameWalk)
{
    /// The reported bug: a browser without WebAssembly used to fall back to a raw regex, which
    /// reintroduced exactly the context bugs the token walk fixes - an aliased identifier or a
    /// comment mention looked like a real trailing clause, so the page suppressed its own
    /// `EventStream` framing and the download stripped bytes out of ordinary SQL. The fallback now
    /// runs the SAME walk over `fallbackTokenize`'s tokens (`expectFormat` / `expectStrip` above
    /// exercise every corpus case through it); these are the reported false positives, pinned
    /// explicitly.
    const auto fallback_name = [](const std::string & query) { return detectExplicitFormat(fallbackTokenizeSignificant(query)); };
    EXPECT_EQ(fallback_name("WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1"), std::nullopt);
    EXPECT_EQ(fallback_name("SELECT 1 -- FORMAT JSON"), std::nullopt);
    EXPECT_EQ(fallback_name("SELECT 1 /* FORMAT JSONCompactColumns */"), std::nullopt);
    EXPECT_EQ(fallback_name("SELECT 'FORMAT JSONCompactColumns'"), std::nullopt);
    /// The real spellings keep working in the fallback, quoted names included.
    EXPECT_EQ(fallback_name("SELECT 1 FORMAT JSON"), std::optional<std::string>("JSON"));
    EXPECT_EQ(fallback_name("SELECT 1 FORMAT `JSON`"), std::optional<std::string>("JSON"));
    EXPECT_EQ(fallback_name("SELECT 1 FORMAT \"TSV\""), std::optional<std::string>("TSV"));
    EXPECT_EQ(fallback_name("SELECT 1 FORMAT `TSV` SETTINGS max_threads = 1"), std::optional<std::string>("TSV"));
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
    expectStrip(
        "WITH 1 AS format SELECT format JSONCompactColumns SETTINGS max_threads = 1",
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

TEST(PlayDetectExplicitFormat, InlineInsertPayloadIsNotSql)
{
    /// A `FORMAT`-looking row after the input format is payload, not an output clause.
    expectFormat("INSERT INTO FUNCTION null('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns", std::nullopt);
    expectStrip(
        "INSERT INTO FUNCTION null('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns",
        "INSERT INTO FUNCTION null('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns");
    expectFormat(
        "INSERT INTO FUNCTION null('line String') SELECT * FROM input('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns",
        std::nullopt);
    expectStrip(
        "INSERT INTO FUNCTION null('line String') SELECT * FROM input('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns",
        "INSERT INTO FUNCTION null('line String') SELECT * FROM input('line String') FORMAT LineAsString\nFORMAT JSONCompactColumns");
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
    expectStrip(
        "SELECT format JSONCompactColumns FROM values('format UInt8', (1))",
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
