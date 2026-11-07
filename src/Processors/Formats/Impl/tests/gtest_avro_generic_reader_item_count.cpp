#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include <DataFile.hh>
#include <Exception.hh>
#include <Generic.hh>
#include <GenericDatum.hh>
#include <Stream.hh>

namespace
{

/// The helpers below build a tiny Avro object container file by hand,
/// because the writer cannot produce a block whose item count exceeds the data that follows it.

void appendLong(std::vector<uint8_t> & out, int64_t value)
{
    uint64_t n = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
    while (n > 0x7f)
    {
        out.push_back(static_cast<uint8_t>((n & 0x7f) | 0x80));
        n >>= 7;
    }
    out.push_back(static_cast<uint8_t>(n));
}

void appendString(std::vector<uint8_t> & out, const std::string & value)
{
    appendLong(out, static_cast<int64_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

/// A file with a single data block holding one record whose only field is an array or a map
/// that declares `declared_item_count` items in its first block and carries no item data at all.
std::vector<uint8_t> makeFileWithDeclaredItemCount(const std::string & field_type, int64_t declared_item_count)
{
    const std::string schema = R"({"type":"record","name":"r","fields":[{"name":"f","type":)" + field_type + "}]}";

    std::vector<uint8_t> sync;
    for (uint8_t i = 0; i < 16; ++i)
        sync.push_back(i);

    std::vector<uint8_t> file = {'O', 'b', 'j', 1};
    appendLong(file, 1);
    appendString(file, "avro.schema");
    appendString(file, schema);
    appendLong(file, 0);
    file.insert(file.end(), sync.begin(), sync.end());

    std::vector<uint8_t> block_data;
    appendLong(block_data, declared_item_count);

    appendLong(file, 1);
    appendLong(file, static_cast<int64_t>(block_data.size()));
    file.insert(file.end(), block_data.begin(), block_data.end());
    file.insert(file.end(), sync.begin(), sync.end());
    return file;
}

void readAllDatums(const std::vector<uint8_t> & file)
{
    avro::DataFileReader<avro::GenericDatum> reader(avro::memoryInputStream(file.data(), file.size()));
    avro::GenericDatum datum(reader.dataSchema());
    while (reader.read(datum))
    {
    }
}

/// More items than `std::vector::max_size` of any element type used by `GenericDatum`, so that
/// resizing a container by the declared count throws `std::length_error` instead of `avro::Exception`.
constexpr int64_t huge_item_count = int64_t(1) << 62;

/// The number of items that take no bytes a single datum may declare, as set in `contrib/avro`.
/// 65536 `GenericDatum` objects of 32 bytes are 2 MiB.
constexpr int64_t max_zero_size_items = int64_t(1) << 16;

constexpr const char * array_of_null = R"({"type":"array","items":"null"})";
constexpr const char * array_of_empty_records = R"({"type":"array","items":{"type":"record","name":"e","fields":[]}})";

}

TEST(AvroGenericReader, ArrayDeclaringMoreItemsThanDataBacks)
{
    /// The block declares 2^62 array items and then ends. The reader must not resize the array by the declared count
    /// and must report the truncated block as `avro::Exception`.
    auto file = makeFileWithDeclaredItemCount(R"({"type":"array","items":"int"})", huge_item_count);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
}

TEST(AvroGenericReader, MapDeclaringMoreItemsThanDataBacks)
{
    auto file = makeFileWithDeclaredItemCount(R"({"type":"map","values":"int"})", huge_item_count);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
}

TEST(AvroGenericReader, ArrayOfItemsThatTakeNoBytesDeclaringHugeItemCount)
{
    /// Items of type `null` occupy no bytes, so reaching the end of the input never stops the loop that reads them.
    /// The reader must cap the number of such items and report `avro::Exception` instead of growing the array without bound.
    auto file = makeFileWithDeclaredItemCount(R"({"type":"array","items":"null"})", huge_item_count);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
}

TEST(AvroGenericReader, ArrayOfEmptyRecordsDeclaringHugeItemCount)
{
    auto file = makeFileWithDeclaredItemCount(R"({"type":"array","items":{"type":"record","name":"e","fields":[]}})", huge_item_count);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
}

TEST(AvroGenericReader, ZeroSizeItemsAtTheLimitReachTruncatedBlock)
{
    /// Exactly the limit passes the cap, and the reader then fails on the missing end of the block.
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(array_of_null, max_zero_size_items)), avro::Exception);
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(array_of_empty_records, max_zero_size_items)), avro::Exception);
}

TEST(AvroGenericReader, ZeroSizeItemsOverTheLimitAreRejectedByTheCap)
{
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(array_of_null, max_zero_size_items + 1)), avro::Exception);
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(array_of_empty_records, max_zero_size_items + 1)), avro::Exception);
}

#endif
