#include <gtest/gtest.h>

#include <Storages/NumberedFileName.h>

using namespace DB;

TEST(NumberedFileName, AddSequenceNumberToFileName)
{
    /// The number is placed after the name of the file and before its extension.
    EXPECT_EQ(addSequenceNumberToFileName("/dir/data.tsv", 1), "/dir/data.1.tsv");
    EXPECT_EQ(addSequenceNumberToFileName("/dir/data.tsv.gz", 2), "/dir/data.2.tsv.gz");

    /// A number that is already in the name is kept: it is not taken for a sequence number.
    EXPECT_EQ(addSequenceNumberToFileName("/dir/data.5.tsv", 1), "/dir/data.1.5.tsv");
    EXPECT_EQ(addSequenceNumberToFileName("/dir/export.2026.csv", 1), "/dir/export.1.2026.csv");

    /// A name without an extension gets the number appended.
    EXPECT_EQ(addSequenceNumberToFileName("/dir/data", 1), "/dir/data.1");

    /// A dot in a directory name is not the start of the extension.
    EXPECT_EQ(addSequenceNumberToFileName("/dir.v2/data.tsv", 1), "/dir.v2/data.1.tsv");
    EXPECT_EQ(addSequenceNumberToFileName("/dir.v2/data", 1), "/dir.v2/data.1");

    /// A non-numeric part of the name stays where it is.
    EXPECT_EQ(addSequenceNumberToFileName("/dir/data.v2.tsv", 1), "/dir/data.1.v2.tsv");

    /// Object storage keys may have no slash at all.
    EXPECT_EQ(addSequenceNumberToFileName("data.tsv", 1), "data.1.tsv");
    EXPECT_EQ(addSequenceNumberToFileName("data.5.tsv", 1), "data.1.5.tsv");
    EXPECT_EQ(addSequenceNumberToFileName("data", 1), "data.1");
}
