#include <Columns/ColumnArray.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Absolute offsets, the encoding used by skip index granules (`position_independent_encoding = false`).
String absoluteOffsets(const std::vector<UInt64> & offsets)
{
    WriteBufferFromOwnString out;
    for (UInt64 offset : offsets)
        writeBinaryLittleEndian(offset, out);
    return out.str();
}

/// Read `limit` offsets into `offsets_column`, the way `MergeTreeIndexGranuleSet::deserializeBinary` does.
void readOffsets(ColumnPtr & offsets_column, const String & data, size_t limit)
{
    ReadBufferFromString in(data);
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&in](const ISerialization::SubstreamPath &) -> ReadBuffer * { return &in; };
    settings.position_independent_encoding = false;
    SerializationArray::deserializeOffsetsBinaryBulk(offsets_column, limit, settings, nullptr);
}

ColumnPtr emptyOffsets()
{
    return ColumnArray::ColumnOffsets::create();
}

const ColumnArray::Offsets & offsetValues(const ColumnPtr & column)
{
    return assert_cast<const ColumnArray::ColumnOffsets &>(*column).getData();
}

}

/// Decreasing absolute offsets make `offsetAt` exceed the row's own offset, so consumers that read the
/// nested column over that range index it out of bounds. Reject them where they are read.
TEST(SerializationArrayOffsets, RejectsDecreasingAbsoluteOffsets)
{
    auto offsets_column = emptyOffsets();
    try
    {
        readOffsets(offsets_column, absoluteOffsets({2, 1, 3}), 3);
        FAIL() << "Expected INCORRECT_DATA for decreasing offsets";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(ErrorCodes::INCORRECT_DATA, e.code());
    }
}

TEST(SerializationArrayOffsets, AcceptsNonDecreasingAbsoluteOffsets)
{
    auto offsets_column = emptyOffsets();
    readOffsets(offsets_column, absoluteOffsets({0, 2, 2, 5}), 4);
    ASSERT_EQ(offsetValues(offsets_column), (ColumnArray::Offsets{0, 2, 2, 5}));
}

/// Offsets accumulate across range reads, so each call only verifies what it appended. The scan still
/// starts one element early, which is what catches a drop across the boundary between two reads.
TEST(SerializationArrayOffsets, RejectsDecreaseAcrossRangeBoundary)
{
    auto offsets_column = emptyOffsets();
    readOffsets(offsets_column, absoluteOffsets({1, 4}), 2);
    ASSERT_EQ(offsetValues(offsets_column), (ColumnArray::Offsets{1, 4}));

    try
    {
        readOffsets(offsets_column, absoluteOffsets({3, 7}), 2);
        FAIL() << "Expected INCORRECT_DATA for an offset lower than the last one already read";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(ErrorCodes::INCORRECT_DATA, e.code());
    }
}

TEST(SerializationArrayOffsets, AcceptsNonDecreasingAcrossRangeBoundary)
{
    auto offsets_column = emptyOffsets();
    readOffsets(offsets_column, absoluteOffsets({1, 4}), 2);
    readOffsets(offsets_column, absoluteOffsets({4, 6}), 2);
    ASSERT_EQ(offsetValues(offsets_column), (ColumnArray::Offsets{1, 4, 4, 6}));
}

/// Sizes are accumulated with a non-negative step, so that encoding cannot produce decreasing offsets
/// and reading the same bytes as sizes stays accepted.
TEST(SerializationArrayOffsets, SizesEncodingIsMonotonicByConstruction)
{
    auto offsets_column = emptyOffsets();
    ReadBufferFromString in(absoluteOffsets({2, 1, 3}));
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&in](const ISerialization::SubstreamPath &) -> ReadBuffer * { return &in; };
    settings.position_independent_encoding = true;
    SerializationArray::deserializeOffsetsBinaryBulk(offsets_column, 3, settings, nullptr);
    ASSERT_EQ(offsetValues(offsets_column), (ColumnArray::Offsets{2, 3, 6}));
}

/// Repeated reads into one accumulating column keep appending, so a column filled over many calls ends
/// up with every offset that was read and stays accepted throughout.
TEST(SerializationArrayOffsets, AccumulatesAcrossManyReads)
{
    auto offsets_column = emptyOffsets();
    UInt64 offset = 0;
    for (size_t round = 0; round < 64; ++round)
    {
        std::vector<UInt64> chunk;
        for (size_t i = 0; i < 16; ++i)
            chunk.push_back(++offset);
        readOffsets(offsets_column, absoluteOffsets(chunk), chunk.size());
    }
    ASSERT_EQ(offsetValues(offsets_column).size(), 64u * 16u);
    ASSERT_EQ(offsetValues(offsets_column).back(), 64u * 16u);
}
