#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <set>
#include <string>
#include <vector>

/** Regression coverage for the `statementIsReadOnly` logic in `programs/server/play.html` and its
  * two consumers, `queryIsReadOnly` and `splitAllQueries`.
  *
  * The Web UI retries a query without framing when the server rejects the page's `EventStream`
  * request, but a side-effecting statement (`INSERT`, DDL) may already have run before returning a
  * plain-HTTP error, so it must not be resubmitted. `queryIsReadOnly` decides whether a query is
  * safe to re-run. `Run all` (`splitAllQueries` + `postMulti`) reuses the same classification to
  * decide which statements may run in parallel: read-only statements are batched concurrently while
  * every other statement is a barrier, so a write must never be misclassified as read-only or a
  * dependent read could execute before the write commits.
  *
  * The leading keyword is not enough: ClickHouse allows a query to begin with a `WITH` (CTE) clause,
  * so `WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y` starts with the
  * read-only-looking `WITH` yet is a write. The detection tokenizes the query with the ClickHouse
  * `Lexer` (compiled to WebAssembly from `src/Parsers/Lexer.cpp` in the browser - the very same
  * source exercised here) and resolves the real statement kind: the first statement keyword after
  * the CTE list. A write/DDL keyword there means the whole statement is a write; a read-only keyword
  * means it is safe.
  *
  * There is no JavaScript/WebAssembly runtime in CI, so we cannot run the browser code directly.
  * Instead we reproduce the token-walking algorithm here on top of the real `DB::Lexer`. The lexer
  * (the part most likely to evolve) is shared; only the small detection below is a port. Keep this
  * in sync with `statementIsReadOnly` / `queryIsReadOnly` / `splitAllQueries` in
  * `programs/server/play.html`.
  */

namespace
{

std::string toUpper(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

/// Mirror of `PARALLELIZABLE_KEYWORDS` in play.html.
const std::set<std::string> parallelizable_keywords = {
    "SELECT", "SHOW", "DESCRIBE", "DESC", "EXISTS", "EXPLAIN", "WITH"};

/// Mirror of `WRITE_STATEMENT_KEYWORDS` in play.html.
const std::set<std::string> write_statement_keywords = {
    "INSERT", "CREATE", "ALTER", "DROP", "DELETE", "TRUNCATE", "OPTIMIZE", "SYSTEM", "RENAME",
    "ATTACH", "DETACH", "GRANT", "REVOKE", "KILL", "BACKUP", "RESTORE"};

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

/// Faithful port of `statementIsReadOnly` from play.html: classifies one statement given as its
/// significant tokens. Shared - there as here - by the `queryIsReadOnly` retry gate and the
/// `splitAllQueries` per-statement `is_select` classification.
bool statementIsReadOnly(const std::vector<Tok> & tokens)
{
    int depth = 0;
    /// Bracket depth at which the leading `WITH` sits; -1 until the leading keyword is seen.
    int with_depth = -1;
    /// Whether the previous same-level token was `AS`, making the current bare word a CTE alias.
    /// Cleared by any non-bare-word token: after `AS (subquery)` there is no bare-word alias.
    bool alias_follows = false;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const Tok & t = tokens[i];
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
        if (t.type != DB::TokenType::BareWord)
        {
            alias_follows = false;
            continue;
        }
        const std::string keyword = toUpper(t.text);
        if (with_depth < 0)
        {
            /// The first significant keyword (possibly wrapped in leading brackets). If it is not a
            /// leading `WITH`, it is the statement kind.
            if (keyword != "WITH")
                return parallelizable_keywords.contains(keyword);
            with_depth = depth;
            continue;
        }
        /// After the leading `WITH`: the real statement keyword sits at the CTE-list level; CTE
        /// subqueries are nested deeper and are skipped.
        if (depth != with_depth)
            continue;
        /// A bare word right after `AS` is the CTE alias of the `expr AS alias` form - skip it even
        /// when it is keyword-shaped (with an explicit `AS`, the parser accepts any bare word as
        /// the alias; only alias-without-`AS` rejects keywords).
        if (alias_follows)
        {
            alias_follows = false;
            continue;
        }
        if (keyword == "AS")
        {
            alias_follows = true;
            continue;
        }
        /// A bare word immediately followed by `AS` is the CTE name of the `name AS (subquery)`
        /// form - skip it too; a real statement keyword is never followed by `AS`.
        if (i + 1 < tokens.size() && tokens[i + 1].type == DB::TokenType::BareWord && toUpper(tokens[i + 1].text) == "AS")
            continue;
        if (write_statement_keywords.contains(keyword))
            return false;
        if (parallelizable_keywords.contains(keyword))
            return true;
    }
    return false;
}

/// Faithful port of `queryIsReadOnly` from play.html (the WebAssembly-available branch).
bool queryIsReadOnly(const std::string & query)
{
    return statementIsReadOnly(tokenizeSignificant(query));
}

/// Faithful port of the `splitAllQueries` splitting-and-classification loop from play.html
/// (the WebAssembly-available branch), reduced to what `postMulti` consumes: the per-statement
/// `is_select` flags. The browser splits the editor content on top-level `;` tokens, collects each
/// statement's significant tokens, and classifies them with the shared `statementIsReadOnly`.
std::vector<bool> splitAllQueriesIsSelect(const std::string & text)
{
    DB::Lexer lexer(text.data(), text.data() + text.size(), 65536);
    std::vector<bool> is_select;
    std::vector<Tok> statement_tokens;
    bool has_significant = false;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        if (token.type == DB::TokenType::Semicolon)
        {
            if (has_significant)
                is_select.push_back(statementIsReadOnly(statement_tokens));
            statement_tokens.clear();
            has_significant = false;
        }
        else if (token.isSignificant())
        {
            has_significant = true;
            statement_tokens.push_back({token.type, std::string(token.begin, token.end)});
        }
    }
    /// Last query (no trailing semicolon).
    if (has_significant)
        is_select.push_back(statementIsReadOnly(statement_tokens));
    return is_select;
}

}

TEST(PlayQueryIsReadOnly, PlainReadOnly)
{
    EXPECT_TRUE(queryIsReadOnly("SELECT 1"));
    EXPECT_TRUE(queryIsReadOnly("select * from numbers(10)"));
    EXPECT_TRUE(queryIsReadOnly("SHOW TABLES"));
    EXPECT_TRUE(queryIsReadOnly("DESCRIBE TABLE t"));
    EXPECT_TRUE(queryIsReadOnly("EXISTS TABLE t"));
    EXPECT_TRUE(queryIsReadOnly("EXPLAIN SELECT 1"));
    /// Leading comments and a wrapping bracket do not change the statement kind.
    EXPECT_TRUE(queryIsReadOnly("-- a comment\nSELECT 1"));
    EXPECT_TRUE(queryIsReadOnly("/* c */ SELECT 1"));
    EXPECT_TRUE(queryIsReadOnly("(SELECT 1)"));
    EXPECT_TRUE(queryIsReadOnly("(SELECT 1 UNION ALL SELECT 2)"));
}

TEST(PlayQueryIsReadOnly, PlainWrite)
{
    EXPECT_FALSE(queryIsReadOnly("INSERT INTO t VALUES (1)"));
    EXPECT_FALSE(queryIsReadOnly("insert into t select * from s"));
    EXPECT_FALSE(queryIsReadOnly("CREATE TABLE t (x UInt8) ENGINE = Memory"));
    EXPECT_FALSE(queryIsReadOnly("ALTER TABLE t DELETE WHERE x = 1"));
    EXPECT_FALSE(queryIsReadOnly("DROP TABLE t"));
    EXPECT_FALSE(queryIsReadOnly("TRUNCATE TABLE t"));
    EXPECT_FALSE(queryIsReadOnly("OPTIMIZE TABLE t"));
    EXPECT_FALSE(queryIsReadOnly("SYSTEM FLUSH LOGS"));
    EXPECT_FALSE(queryIsReadOnly(""));
}

TEST(PlayQueryIsReadOnly, LeadingWithSelectIsReadOnly)
{
    EXPECT_TRUE(queryIsReadOnly("WITH 1 AS a SELECT a"));
    EXPECT_TRUE(queryIsReadOnly("WITH x AS (SELECT * FROM numbers(10)) SELECT * FROM x"));
    /// Several CTEs, including scalar ones with expressions and keyword-like operators.
    EXPECT_TRUE(queryIsReadOnly("WITH 1 AS a, 2 AS b SELECT a + b"));
    EXPECT_TRUE(queryIsReadOnly("WITH toInt32(number) AS a SELECT a FROM numbers(3)"));
    EXPECT_TRUE(queryIsReadOnly("WITH 1 IN (1, 2) AS a SELECT a"));
    /// A subquery CTE that itself contains an `INSERT`-like string must not flip the decision.
    EXPECT_TRUE(queryIsReadOnly("WITH x AS (SELECT 'INSERT INTO t' AS s) SELECT * FROM x"));
    /// `WITH RECURSIVE`.
    EXPECT_TRUE(queryIsReadOnly("WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 10) SELECT * FROM t"));
    /// A wrapping bracket around a `WITH ... SELECT`.
    EXPECT_TRUE(queryIsReadOnly("(WITH 1 AS a SELECT a)"));
}

TEST(PlayQueryIsReadOnly, LeadingWithWriteIsNotReadOnly)
{
    /// The reported bug: a `WITH ... INSERT ...` starts with the read-only-looking `WITH` but is a
    /// write, so it must not be classified as read-only (and therefore must not be resubmitted).
    EXPECT_FALSE(queryIsReadOnly("WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y"));
    EXPECT_FALSE(queryIsReadOnly("with y as (select 1) insert into x select * from y"));
    /// A scalar CTE followed by an `INSERT`.
    EXPECT_FALSE(queryIsReadOnly("WITH 42 AS v INSERT INTO x SELECT v"));
    /// Whitespace and comments before the `WITH` do not matter.
    EXPECT_FALSE(queryIsReadOnly("  /* c */ WITH y AS (SELECT 1) INSERT INTO x SELECT * FROM y"));
}

TEST(PlayQueryIsReadOnly, KeywordAliasesInWithList)
{
    /// The reported bug: with an explicit `AS`, the alias may be ANY bare word, including a
    /// reserved keyword (`ParserAlias` only rejects keyword aliases when `AS` is omitted). The
    /// keyword-shaped alias must not be taken as the statement kind: the real kind here is the
    /// `INSERT` that follows, so the statement is a write and must not be resubmitted or
    /// parallelized.
    EXPECT_FALSE(queryIsReadOnly("WITH 1 AS SELECT INSERT INTO t SELECT SELECT"));
    EXPECT_FALSE(queryIsReadOnly("WITH 1 AS select INSERT INTO t SELECT select"));
    EXPECT_FALSE(queryIsReadOnly("WITH 1 AS show, 2 AS explain INSERT INTO t SELECT show + explain"));
    /// The same alias positions in a read-only statement stay read-only, including a write keyword
    /// used as an alias.
    EXPECT_TRUE(queryIsReadOnly("WITH 1 AS insert SELECT insert"));
    EXPECT_TRUE(queryIsReadOnly("WITH 1 AS drop, 2 AS b SELECT drop + b"));
    /// The `name AS (subquery)` form: the CTE name precedes `AS`, and may be keyword-shaped too.
    EXPECT_FALSE(queryIsReadOnly("WITH select AS (SELECT 1) INSERT INTO t SELECT * FROM select"));
    EXPECT_TRUE(queryIsReadOnly("WITH insert AS (SELECT 1) SELECT * FROM insert"));
}

TEST(PlayQueryIsReadOnly, SplitAllQueriesUsesStatementKind)
{
    /// The reported bug: `Run all` classified each statement by its first keyword only, so a
    /// `WITH ... INSERT ...` was tagged parallelizable and could be launched together with the
    /// `SELECT` that follows it, letting the read execute before the write commits. The splitter
    /// must classify with the same CTE-aware walk as the retry gate: the insert is a barrier
    /// (false), the dependent read is parallelizable (true).
    EXPECT_EQ(
        splitAllQueriesIsSelect("WITH y AS (SELECT 1) INSERT INTO t SELECT * FROM y; SELECT count() FROM t"),
        (std::vector<bool>{false, true}));
    /// The statement kind is per statement, not inherited from neighbors; a trailing semicolon and
    /// plain writes behave as before.
    EXPECT_EQ(
        splitAllQueriesIsSelect("SELECT 1; INSERT INTO t VALUES (1); WITH 42 AS v SELECT v;"),
        (std::vector<bool>{true, false, true}));
    /// A semicolon inside a string literal is one token and does not split the statement.
    EXPECT_EQ(
        splitAllQueriesIsSelect("SELECT 'a;b'; WITH y AS (SELECT 1) INSERT INTO t SELECT * FROM y"),
        (std::vector<bool>{true, false}));
}
