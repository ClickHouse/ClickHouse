#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <string>
#include <vector>

/** Regression coverage for the `getQueryUnderCursor` logic in `programs/server/play.html`.
  *
  * `play.html` selects the query to run by tokenizing the textarea content with the
  * ClickHouse `Lexer` (compiled to WebAssembly from `src/Parsers/Lexer.cpp` — the very
  * same source exercised here) and then walking the tokens relative to the cursor.
  *
  * There is no JavaScript/WebAssembly runtime in CI, so we cannot run the browser code
  * directly. Instead we reproduce the exact token-walking algorithm here on top of the
  * real `DB::Lexer`. The lexer (the part most likely to evolve) is shared; only the small
  * selection algorithm below is a port. Keep this in sync with `getQueryUnderCursor` in
  * `programs/server/play.html`.
  *
  * The contract being locked: when the cursor is past the last `;` and the tail after it
  * has no significant content and no lexer error, run the previously completed query
  * rather than an empty string or a bare delimiter; but when the tail contains an
  * incomplete token (an unclosed string or comment, which the lexer reports as an error),
  * run that incomplete tail so the server surfaces a syntax error.
  */

namespace
{

/// Mirror of `tokenize` in play.html: collect tokens until the first error or the end of
/// stream (both stop the loop in the browser), recording for each token whether it is
/// significant (not whitespace/comment) and whether it is a `;` delimiter.
struct Tok
{
    bool significant;
    bool is_semicolon;
    size_t length;
};

std::vector<Tok> tokenize(const std::string & query)
{
    DB::Lexer lexer(query.data(), query.data() + query.size(), 65536);
    std::vector<Tok> tokens;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        tokens.push_back({token.isSignificant(), token.type == DB::TokenType::Semicolon, token.size()});
    }
    return tokens;
}

/// Faithful port of `getQueryUnderCursor` from play.html. Returns the substring that the
/// Web UI would select and send to the server for the given cursor position.
std::string getQueryUnderCursor(const std::string & all_queries, size_t cursor_position)
{
    const std::vector<Tok> tokens = tokenize(all_queries);

    size_t current_query_start = 0;
    size_t current_offset = 0;
    /// Most recently completed non-empty query (the segment ending at a `;`).
    ssize_t last_query_start = -1;
    ssize_t last_query_end = -1;

    for (const auto & elem : tokens)
    {
        if (current_query_start == current_offset && !elem.significant)
            current_query_start += elem.length;

        const size_t offset_before_token = current_offset;
        current_offset += elem.length;

        if (elem.is_semicolon)
        {
            /// A delimiter-only segment (e.g. the second `;` in `;;`) has no significant
            /// content: do not run it and do not record it as the last completed query.
            const bool has_content = current_query_start < offset_before_token;
            if (has_content && current_offset >= cursor_position)
                return all_queries.substr(current_query_start, current_offset - current_query_start);
            if (has_content)
            {
                last_query_start = static_cast<ssize_t>(current_query_start);
                last_query_end = static_cast<ssize_t>(current_offset);
            }
            current_query_start = current_offset;
        }
    }

    /// The cursor is past the last `;`. If the tail has no significant content (only
    /// whitespace/comments) and no lexer error stopped tokenization before the end, fall
    /// back to the query that ended at the last `;` rather than returning an empty string.
    if (current_query_start == current_offset && current_offset == all_queries.size() && last_query_end >= 0)
        return all_queries.substr(last_query_start, last_query_end - last_query_start);

    /// Cursor is in the last query (no trailing semicolon), or the tail is an incomplete
    /// (error) token that must be sent so the server reports a syntax error.
    return all_queries.substr(current_query_start);
}

/// Convenience: cursor placed at the very end of the text (the common "press Run" case).
std::string atEnd(const std::string & all_queries)
{
    return getQueryUnderCursor(all_queries, all_queries.size());
}

}

TEST(PlayGetQueryUnderCursor, SingleQueryCursorPastSemicolon)
{
    /// The original bug: cursor past the only trailing `;` returned an empty string.
    EXPECT_EQ(atEnd("SELECT 1;"), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1; "), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1;\n"), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1;   \n  "), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1; -- comment\n"), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1; /* comment */"), "SELECT 1;");
}

TEST(PlayGetQueryUnderCursor, DelimiterOnlySegmentsAreSkipped)
{
    /// `;;` must not select the empty segment between the two delimiters.
    EXPECT_EQ(atEnd("SELECT 1;;"), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1;; "), "SELECT 1;");
    EXPECT_EQ(atEnd("SELECT 1; ; "), "SELECT 1;");
}

TEST(PlayGetQueryUnderCursor, IncompleteTailIsSentForSyntaxError)
{
    /// An unclosed string or comment after the last `;` is a lexer error: tokenization
    /// stops before the tail, so `current_offset` is short of the full length and we send
    /// the incomplete tail instead of silently re-running the previous query.
    EXPECT_EQ(atEnd("SELECT 1; '"), "'");
    EXPECT_EQ(atEnd("SELECT 1; /*"), "/*");
    EXPECT_EQ(atEnd("TRUNCATE TABLE t; '"), "'");
}

TEST(PlayGetQueryUnderCursor, NoSemicolon)
{
    EXPECT_EQ(atEnd("SELECT 1"), "SELECT 1");
    EXPECT_EQ(atEnd("SELECT 1 "), "SELECT 1 ");
}

TEST(PlayGetQueryUnderCursor, LeadingWhitespaceIsTrimmed)
{
    EXPECT_EQ(atEnd("  SELECT 1"), "SELECT 1");
    EXPECT_EQ(atEnd("\n\t SELECT 1;"), "SELECT 1;");
}

TEST(PlayGetQueryUnderCursor, MultipleQueries)
{
    const std::string queries = "SELECT 1; SELECT 2;";

    /// Cursor inside the first query.
    EXPECT_EQ(getQueryUnderCursor(queries, 3), "SELECT 1;");
    /// Cursor inside the second query.
    EXPECT_EQ(getQueryUnderCursor(queries, 14), "SELECT 2;");
    /// Cursor at the very end (past the last `;`) selects the last query.
    EXPECT_EQ(atEnd(queries), "SELECT 2;");
}
