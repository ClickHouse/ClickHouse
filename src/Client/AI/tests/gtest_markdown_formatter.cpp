#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/MarkdownFormatter.h>

#include <string>

using namespace DB;

namespace
{
constexpr std::string_view RESET = "\033[0m";
constexpr std::string_view BOLD = "\033[1m";
constexpr std::string_view ITALIC = "\033[3m";
constexpr std::string_view UNDERLINE = "\033[4m";
constexpr std::string_view CODE = "\033[36m";
constexpr std::string_view HEADER = "\033[1;4m";
}

TEST(MarkdownFormatter, PlainTextIsUnchanged)
{
    EXPECT_EQ("just some text", renderMarkdownToANSI("just some text"));
}

TEST(MarkdownFormatter, Bold)
{
    EXPECT_EQ(std::string(BOLD) + "important" + std::string(RESET), renderMarkdownToANSI("**important**"));
    EXPECT_EQ(std::string(BOLD) + "important" + std::string(RESET), renderMarkdownToANSI("__important__"));
}

TEST(MarkdownFormatter, Italic)
{
    EXPECT_EQ(std::string(ITALIC) + "emph" + std::string(RESET), renderMarkdownToANSI("*emph*"));
    EXPECT_EQ(std::string(ITALIC) + "emph" + std::string(RESET), renderMarkdownToANSI("_emph_"));
}

TEST(MarkdownFormatter, InlineCode)
{
    EXPECT_EQ(std::string(CODE) + "SELECT 1" + std::string(RESET), renderMarkdownToANSI("`SELECT 1`"));
}

TEST(MarkdownFormatter, InlineCodeContentsAreVerbatim)
{
    /// Markdown markers inside inline code must not be interpreted.
    EXPECT_EQ(std::string(CODE) + "a ** b" + std::string(RESET), renderMarkdownToANSI("`a ** b`"));
}

TEST(MarkdownFormatter, Header)
{
    EXPECT_EQ(std::string(HEADER) + "Title" + std::string(RESET), renderMarkdownToANSI("# Title"));
    EXPECT_EQ(std::string(HEADER) + "Sub" + std::string(RESET), renderMarkdownToANSI("### Sub"));
}

TEST(MarkdownFormatter, HashWithoutSpaceIsNotAHeader)
{
    EXPECT_EQ("#nothashtag", renderMarkdownToANSI("#nothashtag"));
}

TEST(MarkdownFormatter, UnorderedListMarkerBecomesBullet)
{
    EXPECT_EQ("• one", renderMarkdownToANSI("- one"));
    EXPECT_EQ("• two", renderMarkdownToANSI("* two"));
    EXPECT_EQ("  • nested", renderMarkdownToANSI("  - nested"));
}

TEST(MarkdownFormatter, OrderedListIsLeftAlone)
{
    EXPECT_EQ("1. first", renderMarkdownToANSI("1. first"));
}

TEST(MarkdownFormatter, Link)
{
    EXPECT_EQ(
        std::string(UNDERLINE) + "docs" + std::string(RESET) + " (\033[2mhttps://x\033[0m)",
        renderMarkdownToANSI("[docs](https://x)"));
}

TEST(MarkdownFormatter, CodeFenceMarkersAreStrippedContentsColored)
{
    const std::string input = "```sql\nSELECT 1\n```";
    const std::string expected = std::string(CODE) + "SELECT 1" + std::string(RESET);
    EXPECT_EQ(expected, renderMarkdownToANSI(input));
}

TEST(MarkdownFormatter, CodeFenceContentsAreVerbatim)
{
    const std::string input = "```\n**not bold**\n```";
    const std::string expected = std::string(CODE) + "**not bold**" + std::string(RESET);
    EXPECT_EQ(expected, renderMarkdownToANSI(input));
}

TEST(MarkdownFormatter, MultipleLinesArePreserved)
{
    const std::string input = "line one\nline two";
    EXPECT_EQ("line one\nline two", renderMarkdownToANSI(input));
}

TEST(MarkdownFormatter, NoTrailingNewlineIsAdded)
{
    EXPECT_EQ("text", renderMarkdownToANSI("text\n"));
}

TEST(MarkdownFormatter, UnmatchedMarkersAreLiteral)
{
    /// A dangling delimiter must be emitted verbatim, never swallowed.
    EXPECT_EQ("a * b", renderMarkdownToANSI("a * b"));
    EXPECT_EQ("2 ** 8 is 256", renderMarkdownToANSI("2 ** 8 is 256"));
    EXPECT_EQ("unclosed `code", renderMarkdownToANSI("unclosed `code"));
}

TEST(MarkdownFormatter, BoldContainingInlineCode)
{
    EXPECT_EQ(
        std::string(BOLD) + "run " + std::string(CODE) + "x" + std::string(RESET) + std::string(RESET),
        renderMarkdownToANSI("**run `x`**"));
}

#endif
