#include <gtest/gtest.h>

#include <Columns/ColumnString.h>

#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>

using namespace DB;

static pcg64 rng(randomSeed());

constexpr size_t bytes_per_string = sizeof(uint64_t);
/// Column should have enough bytes to be compressed
constexpr size_t column_size = ColumnString::min_size_to_compress / bytes_per_string + 42;

TEST(ColumnString, Incompressible)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        const uint64_t value = rng();
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // When column is incompressible, we return the original column wrapped in CompressedColumn
    ASSERT_EQ(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_EQ(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_EQ(decompressed->allocatedBytes(), col->allocatedBytes());
}

TEST(ColumnString, CompressibleCharsAndIncompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const uint64_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
    }
    offsets.push_back(chars.size());

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // For actually compressed column only compressed `chars` and `offsets` arrays are stored.
    // Upon decompression, a new column is created.
    ASSERT_NE(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_LE(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_LE(decompressed->allocatedBytes(), col->allocatedBytes());
}

TEST(ColumnString, CompressibleCharsAndCompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const uint64_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // For actually compressed column only compressed `chars` and `offsets` arrays are stored.
    // Upon decompression, a new column is created.
    ASSERT_NE(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_LE(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_LE(decompressed->allocatedBytes(), col->allocatedBytes());
}
