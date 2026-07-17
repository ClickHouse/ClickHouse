#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <set>
#include <string>
#include <vector>

/** Regression coverage for the `queryIsReadOnly` logic in `programs/server/play.html`.
  *
  * The Web UI retries a query without framing when the server rejects the page's `EventStream`
  * request, but a side-effecting statement (`INSERT`, DDL) may already have run before returning a
  * plain-HTTP error, so it must not be resubmitted. `queryIsReadOnly` decides whether a query is
  * safe to re-run.
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
  * in sync with `queryIsReadOnly` in `programs/server/play.html`.
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

/// Faithful port of `queryIsReadOnly` from play.html (the WebAssembly-available branch).
bool queryIsReadOnly(const std::string & query)
{
    const std::vector<Tok> tokens = tokenizeSignificant(query);
    int depth = 0;
    /// Bracket depth at which the leading `WITH` sits; -1 until the leading keyword is seen.
    int with_depth = -1;
    for (const Tok & t : tokens)
    {
        if (isOpeningBracket(t.type))
        {
            ++depth;
            continue;
        }
        if (isClosingBracket(t.type))
        {
            if (depth > 0)
                --depth;
            continue;
        }
        if (t.type != DB::TokenType::BareWord)
            continue;
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
        /// subqueries are nested deeper and are skipped, and CTE aliases / `AS` / `RECURSIVE` are
        /// neither write nor read-only keywords, so they are skipped too.
        if (depth != with_depth)
            continue;
        if (write_statement_keywords.contains(keyword))
            return false;
        if (parallelizable_keywords.contains(keyword))
            return true;
    }
    return false;
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
