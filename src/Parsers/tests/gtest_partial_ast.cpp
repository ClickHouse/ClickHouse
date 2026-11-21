#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTError.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/PartialASTCapture.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <string>

using namespace DB;

namespace
{

constexpr size_t MAX_DEPTH = 1000;
constexpr size_t MAX_ELEMENTS = 50000;

struct Parsed
{
    ASTPtr ast;
    std::string error;
};

Parsed parse(const std::string & query, ParserDiagnostics & diagnostics)
{
    ParserQuery parser(query.data() + query.size());
    const char * pos = query.data();
    Parsed result;
    result.ast = tryParseQuery(
        parser, pos, query.data() + query.size(), result.error, /*hilite=*/ false, "", /*allow_multi_statements=*/ false,
        /*max_query_size=*/ 0, MAX_DEPTH, /*max_parser_backtracks=*/ 1000000, /*skip_insignificant=*/ true, &diagnostics);
    return result;
}

PartialASTState makeState()
{
    PartialASTState state;
    state.max_depth = MAX_DEPTH;
    state.max_elements = MAX_ELEMENTS;
    return state;
}

}

TEST(PartialAST, CapturesOnlyWhenEnabled)
{
    ParserDiagnostics diagnostics;
    const auto without = parse("SELECT a FROM", diagnostics);
    EXPECT_EQ(without.ast, nullptr);
    EXPECT_FALSE(without.error.empty());

    PartialASTState state = makeState();
    diagnostics.expected.partial_ast = &state;
    const auto with = parse("SELECT a FROM", diagnostics);
    EXPECT_EQ(with.ast, nullptr);
    EXPECT_EQ(with.error, without.error);
    ASSERT_NE(state.tree, nullptr);
    EXPECT_NE(state.tree->as<ASTSelectQuery>(), nullptr);
}

/// The caller may reuse one `ParserDiagnostics` for many queries. What the second parse reports must not
/// carry anything of the first.
TEST(PartialAST, ReusedDiagnosticsStartClean)
{
    PartialASTState state = makeState();
    ParserDiagnostics diagnostics;
    diagnostics.expected.partial_ast = &state;

    parse("SELECT a FROM", diagnostics);
    ASSERT_NE(state.tree, nullptr);
    EXPECT_NE(state.tree->as<ASTSelectQuery>(), nullptr);

    /// A query that parses captures nothing.
    const auto success = parse("SELECT 1", diagnostics);
    EXPECT_NE(success.ast, nullptr);
    EXPECT_EQ(state.tree, nullptr);
    EXPECT_EQ(state.tree_pos, nullptr);
    EXPECT_EQ(state.fragment, nullptr);
    EXPECT_EQ(state.select_depth, 0);

    /// A different failing statement reports itself, at its own position.
    const std::string insert = "INSERT INTO t (a,";
    parse(insert, diagnostics);
    ASSERT_NE(state.tree, nullptr);
    EXPECT_NE(state.tree->as<ASTInsertQuery>(), nullptr);
    EXPECT_GE(state.tree_pos, insert.data());
    EXPECT_LE(state.tree_pos, insert.data() + insert.size());

    /// The limits are the caller's, and are kept.
    EXPECT_EQ(state.max_depth, MAX_DEPTH);
    EXPECT_EQ(state.max_elements, MAX_ELEMENTS);
}

/// `eval(SELECT ...)` parses its argument as a `SELECT` first and as an expression after it fails. What the
/// first attempt captured is dropped with it, so the failure is reported in the expression that went on.
TEST(PartialAST, EvalSelectAttemptIsDropped)
{
    PartialASTState state = makeState();
    ParserDiagnostics diagnostics;
    diagnostics.expected.partial_ast = &state;

    const std::string query = "SELECT * FROM eval(SELECT a FROM";
    parse(query, diagnostics);
    ASSERT_NE(state.tree, nullptr);
    const auto * select = state.tree->as<ASTSelectQuery>();
    ASSERT_NE(select, nullptr);

    /// The `SELECT` that failed is the outer one, with `*` in its list, not the abandoned inner one with `a`.
    ASSERT_NE(select->select(), nullptr);
    ASSERT_EQ(select->select()->children.size(), 1);
    EXPECT_NE(select->select()->children[0]->as<ASTAsterisk>(), nullptr);
}
