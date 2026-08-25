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

TEST(TerminalMarkdownRenderer, PlanFeatureBadgeRendersItsMessage)
{
    /// A plan-gating badge builds a substantive message from its attributes on the website (which plan
    /// a feature requires and how to get it, see `badgePayload`); both the label and that message must
    /// survive on this help surface instead of the tag collapsing to the badge name.
    EXPECT_EQ(
        plainRenderer(200).render("<ScalePlanFeatureBadge feature=\"S3 Role-Based Access\" />"),
        "[Scale plan feature] S3 Role-Based Access is available in the Scale and Enterprise plans. "
        "To upgrade, visit the plans page in the cloud console.\n");
    /// `support` routes the reader to support instead of the plans page, and `linking_verb_are` picks
    /// the plural verb, as in the website components.
    EXPECT_EQ(
        plainRenderer(200).render("<EnterprisePlanFeatureBadge feature=\"HIPAA\" support=\"true\" />"),
        "[Enterprise plan feature] HIPAA is available in the Enterprise plan. Contact support to enable this feature.\n");
    EXPECT_EQ(
        plainRenderer(200).render("<ScalePlanFeatureBadge feature=\"Configurable Backups\" linking_verb_are=\"True\" />"),
        "[Scale plan feature] Configurable Backups are available in the Scale and Enterprise plans. "
        "To upgrade, visit the plans page in the cloud console.\n");
    /// Without attributes the message falls back to the components' defaults.
    EXPECT_EQ(
        plainRenderer(200).render("<EnterprisePlanFeatureBadge/>"),
        "[Enterprise plan feature] This feature is available in the Enterprise plan. "
        "To upgrade, visit the plans page in the cloud console.\n");
}

TEST(TerminalMarkdownRenderer, MintlifyAdmonitionComponent)
{
    /// Embedded pages converted from the website's Mintlify sources carry `<Note>` / `<Warning>` / ...
    /// admonition components; they render like their `:::note` / `:::warning` equivalents.
    EXPECT_EQ(plainRenderer().render("<Note>\nBe careful.\n</Note>"), "NOTE:\nBe careful.\n");
    EXPECT_EQ(plainRenderer().render("<Warning>\nDo not do this.\n</Warning>"), "WARNING:\nDo not do this.\n");
    /// An open tag with no matching close is dropped alone; the content still renders.
    EXPECT_EQ(plainRenderer().render("<Tip>\nUnclosed."), "Unclosed.\n");
}

TEST(TerminalMarkdownRenderer, MintlifyTabsKeepTitlesAndContent)
{
    /// `<Tabs>`/`<Tab title="...">` wrappers are dropped, but each tab's title is kept as its own line:
    /// the tabs present alternatives (e.g. the syntax variants of `azureBlobStorage`), and without the
    /// titles the variants would run together indistinguishably.
    EXPECT_EQ(
        plainRenderer().render("<Tabs>\n<Tab title=\"Connection string\">\n\nUse a connection string.\n\n</Tab>\n"
                               "<Tab title=\"Account key\">\n\nUse an account key.\n\n</Tab>\n</Tabs>"),
        "Connection string\n\nUse a connection string.\n\nAccount key\n\nUse an account key.\n");
}

TEST(TerminalMarkdownRenderer, MintlifyCardKeepsTitleAndContent)
{
    EXPECT_EQ(
        plainRenderer().render("<Card title=\"Looking for a guide?\" href=\"/concepts/json\" icon=\"book\">\n"
                               "  Check out the JSON guide.\n</Card>"),
        "Looking for a guide?\nCheck out the JSON guide.\n");
}

TEST(TerminalMarkdownRenderer, SelfClosingComponentIsDropped)
{
    /// A self-closing component with no matching `import` is unknown, so it is simply dropped (see
    /// `KnownSnippetImportIsResolvedToContent` for a resolved documentation snippet import).
    EXPECT_EQ(plainRenderer().render("Before.\n\n<WhenToUseJson />\n\nAfter."), "Before.\n\nAfter.\n");
}

TEST(TerminalMarkdownRenderer, KnownSnippetImportIsResolvedToContent)
{
    /// A self-closing tag whose `import` resolves to a known documentation snippet (see `DOC_SNIPPETS`
    /// in TerminalMarkdownRenderer.cpp) is replaced by the snippet's actual content instead of being
    /// dropped, so a converted page does not lose a whole settings table on this help surface.
    const String out = plainRenderer(120).render(
        "import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';\n\n"
        "Before.\n\n<PrettyFormatSettings/>\n\nAfter.");
    EXPECT_EQ(out.find("PrettyFormatSettings"), String::npos);
    EXPECT_NE(out.find("output_format_pretty_max_rows"), String::npos);
    EXPECT_TRUE(out.starts_with("Before.\n\n"));
    EXPECT_TRUE(out.ends_with("After.\n"));
}

TEST(TerminalMarkdownRenderer, KnownSnippetImportResolvesRegardlessOfLocalAlias)
{
    /// The snippet is matched by its imported path, not by the local binding name: `Avro` and
    /// `AvroConfluent` import the same `data-types-matching.mdx` snippet under different local names.
    const String out = plainRenderer(200).render(
        "import DataTypesMatching from '/snippets/data-types-matching.mdx';\n\n<DataTypesMatching/>");
    EXPECT_NE(out.find("Apache Avro format"), String::npos);
}

TEST(TerminalMarkdownRenderer, UnknownOpenTagRendersLiterally)
{
    /// A non-self-closing tag outside the known component set is prose (e.g. a placeholder), not MDX.
    EXPECT_EQ(plainRenderer().render("<SearchPhrase>"), "<SearchPhrase>\n");
}
