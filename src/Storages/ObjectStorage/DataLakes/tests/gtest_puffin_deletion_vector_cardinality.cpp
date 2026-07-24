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

constexpr UInt64 PUFFIN_DV_MAX_MATERIALIZED_POSITIONS = 100'000'000;
constexpr Int64 large_declared_length = 64 * 1024 * 1024;

}

TEST(PuffinDeletionVectorCardinality, RejectsCardinalityAboveMaterializationLimitBeforeParse)
{
    /// Ceiling is checked before payload validation, so even an empty blob must fail closed.
    try
    {
        deserializeDeletionVectorV1Blob(std::string_view{}, PUFFIN_DV_MAX_MATERIALIZED_POSITIONS + 1);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("exceeds materialization limit"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorCardinality, RejectsCardinalityAboveLimitBeforeFullAllocate)
{
    /// Huge declared length with only a tiny buffer: without an early ceiling check this would
    /// allocate `large_declared_length` (or fail mid-read after that allocate). ReadBufferFromMemory
    /// does not expose file size, so bounds checks alone do not stop this.
    const char header[8] = {};
    ReadBufferFromMemory file(header, sizeof(header));

    try
    {
        readDeletionVectorFromPuffin(
            file, 0, large_declared_length, PUFFIN_DV_MAX_MATERIALIZED_POSITIONS + 1);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("exceeds materialization limit"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorCardinality, RejectsBitmapExceedingDeclaredCardinality)
{
    const std::string_view blob(
        reinterpret_cast<const char *>(two_position_dv_blob), sizeof(two_position_dv_blob));

    try
    {
        deserializeDeletionVectorV1Blob(blob, /*expected_cardinality=*/1);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("exceeds declared cardinality"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorCardinality, AcceptsMatchingCardinality)
{
    const std::string_view blob(
        reinterpret_cast<const char *>(two_position_dv_blob), sizeof(two_position_dv_blob));

    const auto positions = deserializeDeletionVectorV1Blob(blob, /*expected_cardinality=*/2);
    ASSERT_EQ(positions.size(), 2u);
    EXPECT_EQ(positions[0], 2u);
    EXPECT_EQ(positions[1], 5u);
}
