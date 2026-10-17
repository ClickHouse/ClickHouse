#include <Core/Streaming/CursorTree.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTStreamSettings.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

const ASTTableExpression * extractTableExpression(const ASTPtr & ast)
{
    const auto * select_with_union = ast->as<ASTSelectWithUnionQuery>();
    if (!select_with_union || !select_with_union->list_of_selects)
        return nullptr;

    const auto & selects = select_with_union->list_of_selects->children;
    if (selects.empty())
        return nullptr;

    const auto * select = selects.front()->as<ASTSelectQuery>();
    if (!select)
        return nullptr;

    const auto tables = select->tables();
    if (!tables || tables->children.empty())
        return nullptr;

    const auto * first_element = tables->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!first_element || !first_element->table_expression)
        return nullptr;

    return first_element->table_expression->as<ASTTableExpression>();
}

ASTPtr parse(const std::string & query)
{
    ParserQuery parser(query.data() + query.size(), false, false);
    return parseQuery(parser, query, 0, 0, 0);
}

std::string format(const ASTPtr & ast)
{
    WriteBufferFromOwnString buffer;
    IAST::FormatSettings settings(/*one_line_=*/true);
    IAST::FormatState state;
    ast->format(buffer, settings, state, IAST::FormatStateStacked());
    return buffer.str();
}

}

TEST(ParserStreamSettings, PlainStreamParses)
{
    auto ast = parse("SELECT * FROM t STREAM");

    const auto * table_expr = extractTableExpression(ast);
    ASSERT_NE(table_expr, nullptr);
    ASSERT_NE(table_expr->stream_settings, nullptr);

    const auto * stream_ast = table_expr->stream_settings->as<ASTStreamSettings>();
    ASSERT_NE(stream_ast, nullptr);
    ASSERT_FALSE(stream_ast->settings.cursor_tree.has_value());
}

TEST(ParserStreamSettings, StreamCursorParses)
{
    /// Three-level nested cursor covering two shards, multiple partitions per shard,
    /// with a two-field leaf at the deepest level — exercises recursion both on the
    /// parser side and on the tree-reconstruction side.
    auto ast = parse(
        "SELECT * FROM t STREAM CURSOR {"
            "'shard_0': {"
                "'partition_a': {'block_number': 10, 'block_offset': 20}, "
                "'partition_b': {'block_number': 7, 'block_offset': 3}"
            "}, "
            "'shard_1': {"
                "'partition_c': {'block_number': 1, 'block_offset': 99}"
            "}"
        "}");

    const auto * table_expr = extractTableExpression(ast);
    ASSERT_NE(table_expr, nullptr);
    ASSERT_NE(table_expr->stream_settings, nullptr);

    const auto * stream_ast = table_expr->stream_settings->as<ASTStreamSettings>();
    ASSERT_NE(stream_ast, nullptr);
    ASSERT_TRUE(stream_ast->settings.cursor_tree.has_value());

    /// Six distinct (path, value) leaves in the collapsed form.
    const auto & cursor = stream_ast->settings.cursor_tree.value();
    ASSERT_EQ(cursor.size(), 6u);

    /// Reconstruct the tree and walk it manually to confirm structure and values.
    auto tree = buildCursorTree(cursor);

    ASSERT_TRUE(tree->hasSubtree("shard_0"));
    ASSERT_TRUE(tree->hasSubtree("shard_1"));

    const auto & shard_0 = tree->getSubtree("shard_0");
    ASSERT_TRUE(shard_0->hasSubtree("partition_a"));
    ASSERT_TRUE(shard_0->hasSubtree("partition_b"));

    const auto & partition_a = shard_0->getSubtree("partition_a");
    ASSERT_TRUE(partition_a->hasValue("block_number"));
    ASSERT_TRUE(partition_a->hasValue("block_offset"));
    ASSERT_EQ(partition_a->getValue("block_number"), 10);
    ASSERT_EQ(partition_a->getValue("block_offset"), 20);

    const auto & partition_b = shard_0->getSubtree("partition_b");
    ASSERT_EQ(partition_b->getValue("block_number"), 7);
    ASSERT_EQ(partition_b->getValue("block_offset"), 3);

    const auto & shard_1 = tree->getSubtree("shard_1");
    ASSERT_TRUE(shard_1->hasSubtree("partition_c"));
    ASSERT_FALSE(shard_1->hasSubtree("partition_a"));

    const auto & partition_c = shard_1->getSubtree("partition_c");
    ASSERT_EQ(partition_c->getValue("block_number"), 1);
    ASSERT_EQ(partition_c->getValue("block_offset"), 99);

    /// Round-Trip
    auto _ = parse(format(ast));
}

TEST(ParserStreamSettings, FormatRoundTripPreservesStream)
{
    auto ast = parse("SELECT * FROM t STREAM");
    auto formatted = format(ast);

    /// Re-parsing the formatted query must yield an AST that also has stream_settings.
    auto ast2 = parse(formatted);
    const auto * table_expr = extractTableExpression(ast2);
    ASSERT_NE(table_expr, nullptr);
    ASSERT_NE(table_expr->stream_settings, nullptr);
}

TEST(ParserStreamSettings, FormatRoundTripPreservesCursor)
{
    auto ast = parse("SELECT * FROM t STREAM CURSOR {'all': {'block_number': 10}}");
    auto formatted = format(ast);

    auto ast2 = parse(formatted);
    const auto * table_expr = extractTableExpression(ast2);
    ASSERT_NE(table_expr, nullptr);
    ASSERT_NE(table_expr->stream_settings, nullptr);

    const auto * stream_ast = table_expr->stream_settings->as<ASTStreamSettings>();
    ASSERT_TRUE(stream_ast->settings.cursor_tree.has_value());
    ASSERT_EQ(stream_ast->settings.cursor_tree->size(), 1u);
}

TEST(ParserStreamSettings, StreamAfterSampleIsAccepted)
{
    /// STREAM must parse after the other table-expression modifiers.
    auto ast = parse("SELECT * FROM t SAMPLE 1 / 10 STREAM");
    const auto * table_expr = extractTableExpression(ast);
    ASSERT_NE(table_expr, nullptr);
    ASSERT_NE(table_expr->stream_settings, nullptr);
}

TEST(ParserStreamSettings, StreamBareKeywordIsComplete)
{
    /// STREAM followed by neither CURSOR nor another clause must still produce a valid AST.
    ASSERT_NO_THROW((void)parse("SELECT count() FROM t STREAM"));
}

TEST(ParserStreamSettings, CursorRejectsMalformedInput)
{
    /// Malformed cursor bodies must surface a parse exception instead of being silently
    /// accepted or producing a partial AST.
    const std::vector<std::string> bad_queries = {
        "SELECT * FROM t STREAM CURSOR",                            /// missing body
        "SELECT * FROM t STREAM CURSOR {",                          /// unclosed brace
        "SELECT * FROM t STREAM CURSOR {'a': 1",                    /// unclosed brace with content
        "SELECT * FROM t STREAM CURSOR {'a': }",                    /// missing value
        "SELECT * FROM t STREAM CURSOR {'a' 1}",                    /// missing colon
        "SELECT * FROM t STREAM CURSOR {'a': 1 'b': 2}",            /// missing comma between entries
        "SELECT * FROM t STREAM CURSOR {'a': 1,}",                  /// trailing comma
        "SELECT * FROM t STREAM CURSOR {'a': 'b'}",                 /// non-integer leaf
        "SELECT * FROM t STREAM CURSOR {'a': 1.5}",                 /// float leaf
        "SELECT * FROM t STREAM CURSOR {'a': -1}",                  /// signed integer
        "SELECT * FROM t STREAM CURSOR {123: 1}",                   /// non-string key
        "SELECT * FROM t STREAM CURSOR {'a': {'b' 1}}",             /// malformed nested object
    };

    for (const auto & query : bad_queries)
        EXPECT_THROW(parse(query), Exception) << "Expected parse failure for: " << query;
}
