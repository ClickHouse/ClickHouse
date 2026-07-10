#include <gtest/gtest.h>

#include <Parsers/parseQuery.h>
#include <Common/Exception.h>

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
std::pair<std::vector<std::string>, bool> split(const std::string & queries, bool allow_pipe_syntax = false)
{
    std::vector<std::string> queries_list;
    auto res = splitMultipartQuery(
        queries,
        queries_list,
        /* max_query_size */ 262144,
        /* max_parser_depth */ 1000,
        /* max_parser_backtracks */ 1000000,
        /* allow_settings_after_format_in_insert */ false,
        /* implicit_select */ false,
        allow_pipe_syntax);
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

TEST(SplitMultipartQuery, PipeSyntaxSettingIsSnapshotForWholeBatch)
{
    /// `splitMultipartQuery` parses the whole batch up front with a single `allow_pipe_syntax`
    /// snapshot, unlike `clickhouse-client`, which parses and executes one statement at a time and
    /// re-reads the setting between statements. So a `SET allow_experimental_pipe_syntax = 1` at the
    /// start of the batch cannot enable pipe parsing for a later statement in the same batch: the flag
    /// passed to the splitter governs the whole input. This is the documented limitation of the
    /// PostgreSQL wire-protocol handler (`PostgreSQLHandler`) and `clickhouse benchmark` script mode.
    const std::string batch = "SET allow_experimental_pipe_syntax = 1; FROM numbers(3) |> WHERE number > 0;";

    /// With the setting off (session default) the `|>` statement is a syntax error even though the
    /// batch begins with `SET allow_experimental_pipe_syntax = 1` — that `SET` has not run yet.
    EXPECT_THROW(split(batch, /* allow_pipe_syntax */ false), DB::Exception);

    /// Enabling the setting at the session/connection level (so the splitter receives it) parses fine.
    const auto [queries, all_parsed] = split(batch, /* allow_pipe_syntax */ true);
    EXPECT_TRUE(all_parsed);
    EXPECT_EQ(queries.size(), 2u);
}

TEST(SplitMultipartQuery, InsertWithInlineDataAndTrailingComment)
{
    /// An INSERT with inline data carries its own one-line data boundary. A trailing comment after
    /// the final `;` must not be folded into the INSERT fragment: otherwise the bytes after the data
    /// line (`; /* tail */`) would be passed to the format reader as extra input rows instead of being
    /// dropped as a comment-only tail.
    {
        const auto [queries, all_parsed] = split("INSERT INTO t FORMAT TabSeparated\nx\n; /* tail */");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"INSERT INTO t FORMAT TabSeparated\nx"}));
    }
    {
        /// The data boundary must also be preserved when another statement follows the INSERT.
        const auto [queries, all_parsed] = split("INSERT INTO t FORMAT TabSeparated\nx\n; SELECT 2");
        EXPECT_TRUE(all_parsed);
        EXPECT_EQ(queries, (std::vector<std::string>{"INSERT INTO t FORMAT TabSeparated\nx", "SELECT 2"}));
    }
}
