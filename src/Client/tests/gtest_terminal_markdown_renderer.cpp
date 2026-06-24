#include <gtest/gtest.h>

#include <Client/TerminalMarkdownRenderer.h>

using namespace DB;

namespace
{

/// A renderer that produces deterministic, ANSI-free output (as when the client output is not a terminal).
TerminalMarkdownRenderer plainRenderer(size_t width = 40)
{
    TerminalMarkdownRenderer renderer;
    renderer.ansi = false;
    renderer.width = width;
    return renderer;
}

}

TEST(TerminalMarkdownRenderer, ParagraphStripsInlineFormatting)
{
    /// In plain mode the inline markers are consumed and only the text remains.
    EXPECT_EQ(plainRenderer().render("Hello **world** and `code`."), "Hello world and code.\n");
}

TEST(TerminalMarkdownRenderer, InlineCodePreservesInternalSpaces)
{
    /// A code span is an unbreakable unit; its internal spaces survive and the following text attaches to it.
    EXPECT_EQ(plainRenderer().render("Use `SELECT 1`."), "Use SELECT 1.\n");
}

TEST(TerminalMarkdownRenderer, WordWrapAtWidth)
{
    EXPECT_EQ(plainRenderer(20).render("aaaa bbbb cccc dddd eeee"), "aaaa bbbb cccc dddd\neeee\n");
}

TEST(TerminalMarkdownRenderer, AtxHeaderUnderlined)
{
    EXPECT_EQ(plainRenderer().render("# Title\n\nSome text."), "Title\n=====\n\nSome text.\n");
}

TEST(TerminalMarkdownRenderer, FencedCodeBlockIsIndentedWithoutFences)
{
    EXPECT_EQ(plainRenderer().render("```sql\nSELECT 1\n```"), "    SELECT 1\n");
}

TEST(TerminalMarkdownRenderer, SqlCodeBlockUsesHighlightCallback)
{
    TerminalMarkdownRenderer renderer = plainRenderer();
    renderer.highlight_sql = [](const String & sql) { return "<<" + sql + ">>"; };
    EXPECT_EQ(renderer.render("```sql\nSELECT 1\n```"), "    <<SELECT 1>>\n");
}

TEST(TerminalMarkdownRenderer, TableIsAligned)
{
    const String markdown = "| Name | Type |\n|------|------|\n| a | bb |\n| ccc | d |";
    const String expected = "+------+------+\n"
                            "| Name | Type |\n"
                            "+------+------+\n"
                            "| a    | bb   |\n"
                            "| ccc  | d    |\n"
                            "+------+------+\n";
    EXPECT_EQ(plainRenderer().render(markdown), expected);
}

TEST(TerminalMarkdownRenderer, UnorderedList)
{
    EXPECT_EQ(plainRenderer().render("- one\n- two\n- three"), "• one\n• two\n• three\n");
}

TEST(TerminalMarkdownRenderer, OrderedList)
{
    EXPECT_EQ(plainRenderer().render("1. first\n2. second"), "1. first\n2. second\n");
}

TEST(TerminalMarkdownRenderer, LinkShowsTextOnly)
{
    EXPECT_EQ(plainRenderer().render("See [the docs](https://example.com) now."), "See the docs now.\n");
}

TEST(TerminalMarkdownRenderer, SpacedOperatorIsLiteral)
{
    /// A lone `*`/`_` surrounded by spaces is not emphasis and must be rendered literally, otherwise the
    /// marker would be dropped and the rest of the text silently turned into emphasis.
    EXPECT_EQ(plainRenderer().render("SELECT * FROM t"), "SELECT * FROM t\n");
    EXPECT_EQ(plainRenderer().render("a * b and c _ d"), "a * b and c _ d\n");
}

TEST(TerminalMarkdownRenderer, UnbalancedEmphasisIsLiteral)
{
    /// An opening marker with no matching closer is kept literal rather than leaking emphasis to the end.
    EXPECT_EQ(plainRenderer().render("**unterminated bold"), "**unterminated bold\n");
}

TEST(TerminalMarkdownRenderer, EntryBannerAndNoAnsiInPlainMode)
{
    const String out = plainRenderer(30).renderEntry("plus", "Function", "Adds two numbers.");
    EXPECT_TRUE(out.starts_with("plus (Function)\n"));
    EXPECT_NE(out.find("Adds two numbers."), String::npos);
    EXPECT_EQ(out.find('\033'), String::npos);
}

TEST(TerminalMarkdownRenderer, AnsiModeEmitsEscapeSequences)
{
    TerminalMarkdownRenderer renderer;
    renderer.ansi = true;
    renderer.width = 40;
    const String out = renderer.render("**bold**");
    EXPECT_NE(out.find("\033[1m"), String::npos);
    EXPECT_NE(out.find("\033[0m"), String::npos);
    EXPECT_NE(out.find("bold"), String::npos);
}

TEST(TerminalMarkdownRenderer, ParagraphsSeparatedByBlankLine)
{
    EXPECT_EQ(plainRenderer().render("First.\n\nSecond."), "First.\n\nSecond.\n");
}

TEST(TerminalMarkdownRenderer, HeaderAnchorIsStripped)
{
    EXPECT_EQ(plainRenderer().render("## Projections {#projections}\n\nText."), "Projections\n-----------\n\nText.\n");
}

TEST(TerminalMarkdownRenderer, AdmonitionNote)
{
    EXPECT_EQ(plainRenderer().render(":::note\nBe careful.\n:::"), "NOTE:\nBe careful.\n");
}

TEST(TerminalMarkdownRenderer, AdmonitionWithCustomTitle)
{
    EXPECT_EQ(plainRenderer().render(":::tip My Tip\nDo it.\n:::"), "My Tip:\nDo it.\n");
}

TEST(TerminalMarkdownRenderer, AdmonitionFourColons)
{
    /// Docusaurus also uses four-colon fences; the close fence must match the open fence's length.
    EXPECT_EQ(plainRenderer().render("::::note\nBe careful.\n::::"), "NOTE:\nBe careful.\n");
}

TEST(TerminalMarkdownRenderer, StrayCloseFenceIsDropped)
{
    /// A bare colon-run with no opener is dropped, and crucially the loop keeps advancing.
    EXPECT_EQ(plainRenderer().render("Above\n:::\nBelow"), "Above\nBelow\n");
}

TEST(TerminalMarkdownRenderer, SqlCodeBlockWithInfoStringAttributes)
{
    /// The embedded docs emit fences like ```sql title=Query; the language is the first info-string token.
    TerminalMarkdownRenderer renderer = plainRenderer();
    renderer.highlight_sql = [](const String & sql) { return "<<" + sql + ">>"; };
    EXPECT_EQ(renderer.render("```sql title=Query\nSELECT 1\n```"), "    <<SELECT 1>>\n");
}

TEST(TerminalMarkdownRenderer, MdxImportIsHidden)
{
    EXPECT_EQ(plainRenderer().render("import ExperimentalBadge from '@theme/badges/ExperimentalBadge';\n\nReal text."), "Real text.\n");
}

TEST(TerminalMarkdownRenderer, BadgeComponentRendered)
{
    EXPECT_EQ(plainRenderer().render("<ExperimentalBadge/>"), "[Experimental]\n");
    EXPECT_EQ(plainRenderer().render("<CloudNotSupportedBadge/>"), "[Not supported in ClickHouse Cloud]\n");
}
