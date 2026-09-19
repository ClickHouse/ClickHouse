#include <gtest/gtest.h>

#include <Storages/MergeTree/PatchParts/PatchPartIndex.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// A patch part carries the index of the parts it patches in `source_parts.dat`, and that file holds
/// nothing else. A zeroed block of the same size used to parse as a valid index: the first byte `0` is
/// the `V1` format version, the next eight zero bytes are `num_parts = 0`, and everything after them
/// was ignored. `readBinary` asserts that the file ends where the index ends, so the leftover bytes of
/// the zeroed block are what makes the difference between a loud and a silent failure now.
TEST(PatchPartIndexRead, RejectsBytesAfterTheIndex)
{
    PatchPartIndex index(MergeTreePatchPartsVersion::V1, "");
    index.addSourcePart("all_1_1_0", 2);
    index.addSourcePart("all_2_2_0", 3);

    String written;
    {
        WriteBufferFromString out(written);
        index.writeBinary(out);
    }

    {
        ReadBufferFromString in(written);
        auto read_index = PatchPartIndex::readBinary(in);
        EXPECT_FALSE(read_index.empty());
        EXPECT_EQ(read_index.getMinDataVersion("all_1_1_0"), 2);
        EXPECT_EQ(read_index.getMaxDataVersion("all_2_2_0"), 3);
    }

    /// The corruption shape from the issue: the file keeps its size, but its content is gone.
    String zero_filled(written.size(), '\0');
    ASSERT_GT(zero_filled.size(), 9u);
    {
        ReadBufferFromString in(zero_filled);
        EXPECT_ANY_THROW(PatchPartIndex::readBinary(in));
    }

    /// Any other trailing content is rejected the same way.
    {
        ReadBufferFromString in(written + String("\0\0\0", 3));
        EXPECT_ANY_THROW(PatchPartIndex::readBinary(in));
    }
}

/// An index without source parts is still well-formed on its own - it is what an empty covering part
/// carries - so the parser accepts it and the load path is what rejects it for a part that holds rows.
TEST(PatchPartIndexRead, AcceptsAnEmptyIndex)
{
    PatchPartIndex index(MergeTreePatchPartsVersion::V1, "");

    String written;
    {
        WriteBufferFromString out(written);
        index.writeBinary(out);
    }

    ReadBufferFromString in(written);
    auto read_index = PatchPartIndex::readBinary(in);
    EXPECT_TRUE(read_index.empty());
}
