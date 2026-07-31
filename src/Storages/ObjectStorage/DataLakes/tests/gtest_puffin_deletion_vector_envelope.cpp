#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>

#include <string_view>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace
{

/// deletion-vector-v1 blob for positions {2, 5} (cardinality 2).
constexpr UInt8 two_position_dv_blob[] = {
    0x00, 0x00, 0x00, 0x24, 0xD1, 0xD3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x00, 0x2C, 0xDB, 0x9F, 0xC1,
};

/// Large declared length that would force a huge allocate if peeked after full read.
constexpr Int64 large_declared_length = 64 * 1024 * 1024;

}

TEST(PuffinDeletionVectorEnvelope, RejectsLengthBelowEnvelopeMinimum)
{
    const char zeros[16] = {};
    ReadBufferFromMemory file(zeros, sizeof(zeros));

    try
    {
        readDeletionVectorFromPuffin(file, 0, 11);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("too small"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorEnvelope, RejectsInvalidMagicBeforeFullAllocate)
{
    /// Only 8 bytes available; declared length is huge. Without envelope peek this would allocate
    /// `large_declared_length` (or fail mid-read after that allocate). ReadBufferFromMemory does not
    /// expose file size, so the absolute 2 GiB / bounds checks alone do not stop this.
    const UInt8 header[8] = {0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x00}; // wrong magic
    ReadBufferFromMemory file(header, sizeof(header));

    try
    {
        readDeletionVectorFromPuffin(file, 0, large_declared_length);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("Invalid deletion vector magic"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorEnvelope, RejectsCombinedLengthMismatchBeforeFullAllocate)
{
    /// Valid magic, but combined_length implies blob size 0x24+8=44, while caller length is huge.
    const UInt8 header[8] = {0x00, 0x00, 0x00, 0x24, 0xD1, 0xD3, 0x39, 0x64};
    ReadBufferFromMemory file(header, sizeof(header));

    try
    {
        readDeletionVectorFromPuffin(file, 0, large_declared_length);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("does not match combined length"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorEnvelope, ReadsValidBlobAfterEnvelopePeek)
{
    ReadBufferFromMemory file(two_position_dv_blob, sizeof(two_position_dv_blob));
    const auto positions = readDeletionVectorFromPuffin(
        file, 0, static_cast<Int64>(sizeof(two_position_dv_blob)), /*expected_cardinality=*/2);

    ASSERT_EQ(positions.size(), 2u);
    EXPECT_EQ(positions[0], 2u);
    EXPECT_EQ(positions[1], 5u);
}

TEST(PuffinDeletionVectorEnvelope, RejectsInternallyInconsistentRoaringAfterReadSafe)
{
    /// Valid DV envelope + CRC wrapping a portable roaring that `readSafe` accepts but
    /// `roaring_bitmap_internal_validate` rejects (duplicate values in an array container).
    /// Bytes from croaring's robust_deserialization_unit `deserialize_unsorted_array` fixture.
    constexpr UInt8 inconsistent_roaring_dv_blob[] = {
        0x00, 0x00, 0x00, 0x1D, 0xD1, 0xD3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x3B, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x6E, 0x0E, 0x9B, 0x12,
    };

    const std::string_view blob(
        reinterpret_cast<const char *>(inconsistent_roaring_dv_blob), sizeof(inconsistent_roaring_dv_blob));

    try
    {
        deserializeDeletionVectorV1Blob(blob, /*expected_cardinality=*/2);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("failed internal validation"), std::string::npos);
    }
}
