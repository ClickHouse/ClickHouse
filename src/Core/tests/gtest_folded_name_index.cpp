#include <gtest/gtest.h>

#include <Core/FoldedNameIndex.h>

using namespace DB;

using Outcome = FoldedNameIndex::Outcome;

static IdentifierPart unquoted(const String & s)
{
    return {s, IdentifierPartQuote::Unquoted};
}

static IdentifierPart doubleQuoted(const String & s)
{
    return {s, IdentifierPartQuote::DoubleQuoted};
}

static IdentifierPart backticked(const String & s)
{
    return {s, IdentifierPartQuote::Backticked};
}

TEST(FoldedNameIndex, FoldHelper)
{
    EXPECT_EQ(foldIdentifierCaseASCII("FooBar"), "foobar");
    EXPECT_EQ(foldIdentifierCaseASCII("foo_bar1"), "foo_bar1");
    /// Non-ASCII bytes pass through untouched.
    EXPECT_EQ(foldIdentifierCaseASCII("Straße"), "straße");
    EXPECT_EQ(foldIdentifierCaseASCII("ТАБЛИЦА"), "ТАБЛИЦА");
}

TEST(FoldedNameIndex, SensitiveModeIsExact)
{
    FoldedNameIndex index;
    index.add("Foo");

    EXPECT_EQ(index.resolve(unquoted("Foo"), NameMatchMode::Sensitive).outcome, Outcome::Matched);
    EXPECT_EQ(index.resolve(unquoted("foo"), NameMatchMode::Sensitive).outcome, Outcome::Absent);
    EXPECT_EQ(index.resolve(doubleQuoted("Foo"), NameMatchMode::Sensitive).outcome, Outcome::Matched);
}

TEST(FoldedNameIndex, StandardModeFoldsWithoutExactPriority)
{
    FoldedNameIndex index;
    index.add("Val");
    index.add("val");

    /// No exact-first: any unquoted case variant of a case-sibling pair is ambiguous.
    for (const auto * spelling : {"Val", "val", "VAL"})
    {
        auto result = index.resolve(unquoted(spelling), NameMatchMode::Standard);
        EXPECT_EQ(result.outcome, Outcome::Ambiguous) << spelling;
        EXPECT_EQ(result.candidates, (std::vector<String>{"Val", "val"})) << spelling;
    }

    /// Double quotes force exact matching.
    EXPECT_EQ(index.resolve(doubleQuoted("Val"), NameMatchMode::Standard).outcome, Outcome::Matched);
    EXPECT_EQ(index.resolve(doubleQuoted("VAL"), NameMatchMode::Standard).outcome, Outcome::Absent);
}

TEST(FoldedNameIndex, StandardModeUniqueFold)
{
    FoldedNameIndex index;
    index.add("FooBar");

    auto result = index.resolve(unquoted("foobar"), NameMatchMode::Standard);
    EXPECT_EQ(result.outcome, Outcome::Matched);
    EXPECT_EQ(result.canonical, "FooBar");

    /// Backticks fold like unquoted.
    result = index.resolve(backticked("FOOBAR"), NameMatchMode::Standard);
    EXPECT_EQ(result.outcome, Outcome::Matched);
    EXPECT_EQ(result.canonical, "FooBar");

    EXPECT_EQ(index.resolve(unquoted("other"), NameMatchMode::Standard).outcome, Outcome::Absent);
}

TEST(FoldedNameIndex, PinnedObjectsMatchOnlyExactly)
{
    FoldedNameIndex index;
    index.add("MyCol", /*pinned=*/true);

    EXPECT_EQ(index.resolve(unquoted("mycol"), NameMatchMode::Standard).outcome, Outcome::Absent);
    EXPECT_EQ(index.resolve(unquoted("MyCol"), NameMatchMode::Standard).outcome, Outcome::Absent);
    EXPECT_EQ(index.resolve(doubleQuoted("MyCol"), NameMatchMode::Standard).outcome, Outcome::Matched);

    /// A pinned object does not make an unpinned sibling ambiguous.
    index.add("mycol");
    auto result = index.resolve(unquoted("MYCOL"), NameMatchMode::Standard);
    EXPECT_EQ(result.outcome, Outcome::Matched);
    EXPECT_EQ(result.canonical, "mycol");
}

TEST(FoldedNameIndex, RemoveAndRename)
{
    FoldedNameIndex index;
    index.add("Foo");
    index.add("FOO");

    EXPECT_EQ(index.resolve(unquoted("foo"), NameMatchMode::Standard).outcome, Outcome::Ambiguous);

    index.remove("FOO");
    auto result = index.resolve(unquoted("foo"), NameMatchMode::Standard);
    EXPECT_EQ(result.outcome, Outcome::Matched);
    EXPECT_EQ(result.canonical, "Foo");

    index.rename("Foo", "Bar");
    EXPECT_EQ(index.resolve(unquoted("foo"), NameMatchMode::Standard).outcome, Outcome::Absent);
    EXPECT_EQ(index.resolve(unquoted("bar"), NameMatchMode::Standard).canonical, "Bar");
    EXPECT_FALSE(index.contains("Foo"));
}

TEST(FoldedNameIndex, ThreeWayAmbiguityIsSortedAndComplete)
{
    FoldedNameIndex index;
    index.add("abc");
    index.add("ABC");
    index.add("Abc");

    auto result = index.resolve(unquoted("aBc"), NameMatchMode::Standard);
    EXPECT_EQ(result.outcome, Outcome::Ambiguous);
    EXPECT_EQ(result.candidates, (std::vector<String>{"ABC", "Abc", "abc"}));
}
