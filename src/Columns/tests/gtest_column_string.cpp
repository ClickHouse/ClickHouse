#include <gtest/gtest.h>

#include <Columns/ColumnString.h>

#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>

using namespace DB;

static pcg64 rng(randomSeed());

constexpr size_t bytes_per_string = sizeof(size_t) + 1;
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
        auto value = rng();
        memcpy(&chars[i * bytes_per_string], &value, sizeof(size_t));
        chars[i * bytes_per_string + sizeof(size_t)] = '\0';
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    ASSERT_EQ(decompressed.get(), col.get());
}

TEST(ColumnString, CompressibleCharsAndIncompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const size_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(size_t));
        chars[i * bytes_per_string + sizeof(size_t)] = '\0';
    }
    offsets.push_back(chars.size());

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    ASSERT_NE(decompressed.get(), col.get());
}

TEST(ColumnString, CompressibleCharsAndCompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const size_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(size_t));
        chars[i * bytes_per_string + sizeof(size_t)] = '\0';
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    ASSERT_NE(decompressed.get(), col.get());
}
