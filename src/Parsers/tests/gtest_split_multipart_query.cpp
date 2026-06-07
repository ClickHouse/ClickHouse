#include <gtest/gtest.h>

#include <Parsers/parseQuery.h>

#include <string>
#include <vector>

/** Regression coverage for `splitMultipartQuery`.
  *
  * `splitMultipartQuery` breaks a string of `;`-separated statements into individual
  * queries. It is used by the PostgreSQL wire-protocol handler (`PostgreSQLHandler`) and by
  * `clickhouse benchmark`. The bug being locked here: a trailing block or line comment
  * after the final `;` used to be treated as a new statement, so the parser was handed a
  * comment-only fragment and threw an `Empty query` exception. The fix
  * recognizes that the tail after the last `;` carries no significant tokens and stops,
  * folding the trailing comment into the preceding query instead of failing.
  */

namespace
{

using namespace DB;

/// Call the real `splitMultipartQuery` with the parser limits used in production callers.
std::pair<std::vector<std::string>, bool> split(const std::string & queries)
{
    std::vector<std::string> queries_list;
    auto res = splitMultipartQuery(
        queries,
        queries_list,
        /* max_query_size */ 262144,
        /* max_parser_depth */ 1000,
        /* max_parser_backtracks */ 1000000,
        /* allow_settings_after_format_in_insert */ false,
        /* implicit_select */ false);
    return {queries_list, res.second};
}

}

TEST(SplitMultipartQuery, TrailingCommentAfterSemicolon)
{
    /// The original bug: a comment after the trailing `;` was parsed as a separate,
    /// comment-only query and threw an `Empty query` exception.
    {
        const auto [queries, all_parsed] = split("SELECT 1; /* some comment */");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1; /* some comment */"}));
    }
    {
        const auto [queries, all_parsed] = split("SELECT 1; -- some comment\n");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1; -- some comment\n"}));
    }
    {
        /// Several trailing delimiters followed by a comment must not yield empty queries.
        const auto [queries, all_parsed] = split("SELECT 1;; /* c */");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1;; /* c */"}));
    }
}

TEST(SplitMultipartQuery, SingleQuery)
{
    {
        const auto [queries, all_parsed] = split("SELECT 1");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1"}));
    }
    {
        const auto [queries, all_parsed] = split("SELECT 1;");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1;"}));
    }
}

TEST(SplitMultipartQuery, MultipleQueries)
{
    const auto [queries, all_parsed] = split("SELECT 1; SELECT 2; SELECT 3");
    EXPECT_TRUE(all_parsed);
    EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1;", "SELECT 2;", "SELECT 3"}));
}

TEST(SplitMultipartQuery, MultipleQueriesWithTrailingComment)
{
    const auto [queries, all_parsed] = split("SELECT 1; SELECT 2; /* tail */");
    EXPECT_TRUE(all_parsed);
    EXPECT_EQ(queries, (std::vector<std::string>{"SELECT 1;", "SELECT 2; /* tail */"}));
}
