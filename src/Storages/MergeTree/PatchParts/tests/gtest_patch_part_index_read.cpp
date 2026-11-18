#include <gtest/gtest.h>

#include <Storages/MergeTree/PatchParts/PatchPartIndex.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// A patch part carries the index of the parts it patches in `source_parts.dat`, and that file holds
/// nothing else. A zeroed block of the same size used to parse as a valid index: the first byte `0` is
/// the `V1` format version, the next eight zero bytes are `num_parts = 0`, and everything after them
/// was ignored. The load path now asserts that the file ends where the index ends, which relies on
/// `readBinary` consuming exactly the bytes of the index: the same parser reads the index out of larger
/// streams (the in-memory part data exchanged between replicas), where bytes do follow it.
TEST(PatchPartIndexRead, ConsumesExactlyTheIndex)
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
        EXPECT_TRUE(in.eof());
    }

    /// The corruption shape from the issue: the file keeps its size, but its content is gone. The
    /// parser stops after the nine bytes it understands, and what the loader does next is what turns
    /// the rest of the block into a loud failure instead of an accepted empty index.
    String zero_filled(written.size(), '\0');
    ASSERT_GT(zero_filled.size(), 9u);
    {
        ReadBufferFromString in(zero_filled);
        auto read_index = PatchPartIndex::readBinary(in);
        EXPECT_TRUE(read_index.empty());
        EXPECT_EQ(in.count(), 9u);
        EXPECT_FALSE(in.eof());
        EXPECT_ANY_THROW(assertEOF(in));
    }

    /// Bytes after a well-formed index are left in the stream for the caller.
    {
        /// `ReadBufferFromString` only borrows the bytes, so the string has to outlive the buffer.
        String with_trailing_bytes = written + String("tail");
        ReadBufferFromString in(with_trailing_bytes);
        auto read_index = PatchPartIndex::readBinary(in);
        EXPECT_FALSE(read_index.empty());
        EXPECT_EQ(in.count(), written.size());

        String rest;
        readStringUntilEOF(rest, in);
        EXPECT_EQ(rest, "tail");
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
    EXPECT_TRUE(in.eof());
}
