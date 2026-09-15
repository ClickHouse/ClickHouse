#include <gtest/gtest.h>

#include <Storages/NumberedFileName.h>

using namespace DB;

TEST(NumberedFileName, SetSequenceNumberInFileName)
{
    /// The number is placed after the name of the file and before its extension.
    EXPECT_EQ(setSequenceNumberInFileName("/dir/data.tsv", 1), "/dir/data.1.tsv");
    EXPECT_EQ(setSequenceNumberInFileName("/dir/data.tsv.gz", 2), "/dir/data.2.tsv.gz");

    /// An existing number is replaced.
    EXPECT_EQ(setSequenceNumberInFileName("/dir/data.5.tsv", 1), "/dir/data.1.tsv");

    /// A name without an extension gets the number appended.
    EXPECT_EQ(setSequenceNumberInFileName("/dir/data", 1), "/dir/data.1");

    /// A dot in a directory name is not the start of the extension.
    EXPECT_EQ(setSequenceNumberInFileName("/dir.v2/data.tsv", 1), "/dir.v2/data.1.tsv");
    EXPECT_EQ(setSequenceNumberInFileName("/dir.v2/data", 1), "/dir.v2/data.1");

    /// A non-numeric part of the name is not a sequence number.
    EXPECT_EQ(setSequenceNumberInFileName("/dir/data.v2.tsv", 1), "/dir/data.1.v2.tsv");

    /// Object storage keys may have no slash at all.
    EXPECT_EQ(setSequenceNumberInFileName("data.tsv", 1), "data.1.tsv");
    EXPECT_EQ(setSequenceNumberInFileName("data.5.tsv", 1), "data.1.tsv");
    EXPECT_EQ(setSequenceNumberInFileName("data", 1), "data.1");
}

TEST(NumberedFileName, GetStartSequenceNumber)
{
    /// A name without a number starts from the default.
    EXPECT_EQ(getStartSequenceNumber("/dir/data.tsv", 1), 1u);

    /// The numbering continues after an existing number, unless the default is larger.
    EXPECT_EQ(getStartSequenceNumber("/dir/data.5.tsv", 1), 6u);
    EXPECT_EQ(getStartSequenceNumber("/dir/data.5.tsv", 10), 10u);
    EXPECT_EQ(getStartSequenceNumber("/dir/data.0.tsv", 1), 1u);

    /// A non-numeric part of the name is not a sequence number.
    EXPECT_EQ(getStartSequenceNumber("/dir/data.v2.tsv", 1), 1u);

    /// Object storage keys may have no slash at all.
    EXPECT_EQ(getStartSequenceNumber("data.tsv", 1), 1u);
    EXPECT_EQ(getStartSequenceNumber("data.7.tsv", 1), 8u);
}
