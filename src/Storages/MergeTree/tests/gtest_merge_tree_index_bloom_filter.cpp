#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>

#include <limits>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

String encodeRowCount(size_t row_count)
{
    WriteBufferFromOwnString out;
    writeVarUInt(row_count, out);
    return out.str();
}

void expectCorruptedRowCount(size_t bits_per_row, size_t row_count)
{
    MergeTreeIndexGranuleBloomFilter granule(bits_per_row, 3, 1);
    String encoded = encodeRowCount(row_count);
    ReadBufferFromString in(encoded);

    try
    {
        granule.deserializeBinary(in, 1);
        FAIL() << "Expected a corrupted Bloom-filter granule to be rejected";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::CORRUPTED_DATA);
    }
}

}

TEST(MergeTreeIndexBloomFilterTest, DeserializesValidGranule)
{
    String encoded = encodeRowCount(1);
    encoded.append(sizeof(BloomFilter::UnderType), '\0');

    MergeTreeIndexGranuleBloomFilter granule(12, 3, 1);
    ReadBufferFromString in(encoded);

    EXPECT_NO_THROW(granule.deserializeBinary(in, 1));
    EXPECT_FALSE(granule.empty());
}

TEST(MergeTreeIndexBloomFilterTest, RejectsZeroRowGranule)
{
    expectCorruptedRowCount(12, 0);
}

TEST(MergeTreeIndexBloomFilterTest, RejectsFilterSizeOverflow)
{
    constexpr size_t bits_per_row = 12;
    constexpr size_t overflowing_rows = std::numeric_limits<size_t>::max() / bits_per_row + 1;
    expectCorruptedRowCount(bits_per_row, overflowing_rows);
}

TEST(MergeTreeIndexBloomFilterTest, RejectsRoundedFilterSizeOverflow)
{
    expectCorruptedRowCount(1, std::numeric_limits<size_t>::max());
}
