#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <Columns/IColumn.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <DataFile.hh>
#include <Decoder.hh>
#include <Encoder.hh>
#include <Exception.hh>
#include <Generic.hh>
#include <GenericDatum.hh>
#include <Compiler.hh>
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

std::string makeRecordSchema(const std::string & field_type)
{
    return R"({"type":"record","name":"r","fields":[{"name":"f","type":)" + field_type + "}]}";
}

/// A file with a single data block of `block_data` that holds one object.
std::vector<uint8_t> makeFile(const std::string & schema, const std::vector<uint8_t> & block_data)
{
    std::vector<uint8_t> sync;
    for (uint8_t i = 0; i < 16; ++i)
        sync.push_back(i);

    std::vector<uint8_t> file = {'O', 'b', 'j', 1};
    appendLong(file, 1);
    appendString(file, "avro.schema");
    appendString(file, schema);
    appendLong(file, 0);
    file.insert(file.end(), sync.begin(), sync.end());

    appendLong(file, 1);
    appendLong(file, static_cast<int64_t>(block_data.size()));
    file.insert(file.end(), block_data.begin(), block_data.end());
    file.insert(file.end(), sync.begin(), sync.end());
    return file;
}

/// A file with a single data block holding one record whose only field is an array or a map
/// that declares `declared_item_count` items in its first block and carries no item data at all.
std::vector<uint8_t> makeFileWithDeclaredItemCount(const std::string & field_type, int64_t declared_item_count)
{
    std::vector<uint8_t> block_data;
    appendLong(block_data, declared_item_count);
    return makeFile(makeRecordSchema(field_type), block_data);
}

void readAllDatums(const std::vector<uint8_t> & file)
{
    avro::DataFileReader<avro::GenericDatum> reader(avro::memoryInputStream(file.data(), file.size()));
    avro::GenericDatum datum(reader.dataSchema(), avro::ShallowTag());
    while (reader.read(datum))
    {
    }
}

/// Reads the file with schema resolution from the writer schema of the file to `reader_field_type`.
void readAllDatumsResolving(const std::vector<uint8_t> & file, const std::string & reader_field_type)
{
    avro::ValidSchema reader_schema = avro::compileJsonSchemaFromString(makeRecordSchema(reader_field_type));
    avro::DataFileReader<avro::GenericDatum> reader(avro::memoryInputStream(file.data(), file.size()), reader_schema);
    avro::GenericDatum datum(reader.readerSchema(), avro::ShallowTag());
    while (reader.read(datum))
    {
    }
}

/// `{"type":"array","items":<record with `fields_count` fields of type null>}`.
std::string arrayOfRecordsOfNulls(size_t fields_count)
{
    std::string fields;
    for (size_t i = 0; i < fields_count; ++i)
        fields += (i ? R"(,{"name":"n)" : R"({"name":"n)") + std::to_string(i) + R"(","type":"null"})";
    return R"({"type":"array","items":{"type":"record","name":"e","fields":[)" + fields + "]}}";
}

/// More items than `std::vector::max_size` of any element type used by `GenericDatum`, so that
/// resizing a container by the declared count throws `std::length_error` instead of `avro::Exception`.
constexpr int64_t huge_item_count = int64_t(1) << 62;

/// Zero-input expansion is capped at this many datum nodes. Actual storage is larger because containers and
/// schema/value representations also allocate; this is a construction budget, not an exact byte limit.
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

TEST(AvroGenericReader, ArrayOfRecordsOfManyNullsIsBoundedByDatumsNotItems)
{
    /// 65536 items of a record with 1000 `null` fields pass an item-count cap, yet would build 65 million datums
    /// (about 2 GiB) before the missing end of the block is noticed. They must be rejected after a few MiB.
    auto file = makeFileWithDeclaredItemCount(arrayOfRecordsOfNulls(1000), max_zero_size_items);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(arrayOfRecordsOfNulls(1000), huge_item_count)), avro::Exception);
}

TEST(AvroGenericReader, ArrayOfRecordsWithTooManyFieldsIsRejected)
{
    EXPECT_THROW(readAllDatums(makeFileWithDeclaredItemCount(arrayOfRecordsOfNulls(70000), 1)), avro::Exception);
}

TEST(AvroGenericReader, ResolvingArrayOfItemsThatTakeBytesIsNotLimited)
{
    /// With schema resolution (here an extra defaulted field in the reader schema) items that are read from the input
    /// are bounded by the input, however many there are.
    constexpr int64_t items = 70000;
    std::vector<uint8_t> block_data;
    appendLong(block_data, items);
    for (int64_t i = 0; i < items; ++i)
        appendLong(block_data, i);
    appendLong(block_data, 0);

    const std::string writer_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"x","type":"int"}]}})";
    const std::string reader_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"x","type":"int"},{"name":"y","type":"int","default":0}]}})";
    auto file = makeFile(makeRecordSchema(writer_type), block_data);
    EXPECT_NO_THROW(readAllDatumsResolving(file, reader_type));
}

TEST(AvroGenericReader, ResolvingArrayOfItemsMadeOfDefaultsIsLimited)
{
    /// The writer's empty records take no bytes, and so do the reader's records whose only field is filled by a default.
    const std::string writer_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[]}})";
    const std::string reader_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"y","type":"int","default":0}]}})";
    auto file = makeFile(makeRecordSchema(writer_type), [] { std::vector<uint8_t> d; appendLong(d, huge_item_count); return d; }());
    EXPECT_THROW(readAllDatumsResolving(file, reader_type), avro::Exception);
}

TEST(AvroGenericReader, ArrayOfRecursiveNullableRecordsIsRead)
{
    /// `GenericDatum` constructs only the first branch of a union, so the recursion through the second branch
    /// must not count against the limit of items that take no bytes.
    const std::string type = R"({"type":"array","items":{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}})";
    EXPECT_NO_THROW(readAllDatums(makeFile(makeRecordSchema(type), {0x02, 0x00, 0x00})));
    EXPECT_NO_THROW(readAllDatums(makeFile(makeRecordSchema(type), {0x02, 0x02, 0x00, 0x00})));
}

TEST(AvroGenericReader, NestedArraysOfItemsThatTakeNoBytesAreBoundedByTheInput)
{
    /// Every outer item is backed by the count and the end of its own array, so 70000 of them are valid however
    /// many items that take no bytes they contain in total.
    constexpr int64_t items = 70000;
    std::vector<uint8_t> block_data;
    appendLong(block_data, items);
    for (int64_t i = 0; i < items; ++i)
    {
        appendLong(block_data, 1);
        appendLong(block_data, 0);
    }
    appendLong(block_data, 0);

    const std::string type = R"({"type":"array","items":{"type":"array","items":"null"}})";
    EXPECT_NO_THROW(readAllDatums(makeFile(makeRecordSchema(type), block_data)));
}

TEST(AvroGenericReader, NestedArrayOfItemsThatTakeNoBytesWithoutInputIsStillLimited)
{
    /// An inner array that declares a huge count without any input is limited, though its enclosing items read input.
    std::vector<uint8_t> block_data;
    appendLong(block_data, 1);
    appendLong(block_data, huge_item_count);
    const std::string type = R"({"type":"array","items":{"type":"array","items":"null"}})";
    EXPECT_THROW(readAllDatums(makeFile(makeRecordSchema(type), block_data)), avro::Exception);
}

TEST(AvroGenericReader, ResolvingArrayOfItemsThatTakeBytesWithDefaultedArrayIsNotLimited)
{
    /// Each of the 70000 records reads `x` from the input, so the array `y` that is filled by a default is backed by it.
    constexpr int64_t items = 70000;
    std::vector<uint8_t> block_data;
    appendLong(block_data, items);
    for (int64_t i = 0; i < items; ++i)
        appendLong(block_data, i);
    appendLong(block_data, 0);

    const std::string writer_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"x","type":"int"}]}})";
    const std::string reader_type = R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"x","type":"int"},{"name":"y","type":{"type":"array","items":"null"},"default":[null]}]}})";
    EXPECT_NO_THROW(readAllDatumsResolving(makeFile(makeRecordSchema(writer_type), block_data), reader_type));
}

namespace
{

/// A file with `items` empty records of a record `e` without fields.
std::vector<uint8_t> makeFileOfEmptyRecords(int64_t items)
{
    std::vector<uint8_t> block_data;
    appendLong(block_data, items);
    appendLong(block_data, 0);
    return makeFile(makeRecordSchema(R"({"type":"array","items":{"type":"record","name":"e","fields":[]}})"), block_data);
}

/// The reader schema that adds to the empty record `e` a field `f` of type `field_type` with the given default.
std::string readerArrayOfRecordWithDefault(const std::string & field_type, const std::string & default_value)
{
    return R"({"type":"array","items":{"type":"record","name":"e","fields":[{"name":"f","type":)" + field_type
        + R"(,"default":)" + default_value + "}]}}";
}

std::string mapDefaultOfNulls(size_t keys)
{
    std::string result = "{";
    for (size_t i = 0; i < keys; ++i)
        result += (i ? R"(,"k)" : R"("k)") + std::to_string(i) + R"(":null)";
    return result + "}";
}

/// Reads `items` empty records of the record `e` through a resolving decoder, without a data file: `DataFileReader`
/// writes the reader schema back as JSON, which cannot print a default map of more than two entries.
void readEmptyRecordsResolving(int64_t items, const std::string & reader_type)
{
    std::vector<uint8_t> data;
    appendLong(data, items);
    appendLong(data, 0);
    avro::ValidSchema writer_schema = avro::compileJsonSchemaFromString(R"({"type":"array","items":{"type":"record","name":"e","fields":[]}})");
    avro::ValidSchema reader_schema = avro::compileJsonSchemaFromString(reader_type);
    avro::DecoderPtr decoder = avro::resolvingDecoder(writer_schema, reader_schema, avro::binaryDecoder());
    auto in = avro::memoryInputStream(data.data(), data.size());
    decoder->init(*in);
    avro::GenericDatum datum;
    avro::GenericReader::read(*decoder, datum, reader_schema);
}

}

TEST(AvroGenericReader, ResolvingDefaultMapIsBoundedByTheDatumsItCreates)
{
    /// 1000 empty records, each given a default map of 1000 keys, take 3 bytes and would create a million datums.
    const std::string reader_type = readerArrayOfRecordWithDefault(R"({"type":"map","values":"null"})", mapDefaultOfNulls(1000));
    EXPECT_THROW(readEmptyRecordsResolving(1000, reader_type), avro::Exception);
    EXPECT_NO_THROW(readEmptyRecordsResolving(10, reader_type));
}

TEST(AvroGenericReader, ResolvingDefaultStringIsBoundedByItsSize)
{
    /// 1000 empty records, each given a default string of 4 KiB, take 3 bytes and would create 4 MiB of strings.
    const std::string reader_type = readerArrayOfRecordWithDefault(R"("string")", '"' + std::string(4096, 'a') + '"');
    EXPECT_THROW(readAllDatumsResolving(makeFileOfEmptyRecords(1000), reader_type), avro::Exception);
    EXPECT_NO_THROW(readAllDatumsResolving(makeFileOfEmptyRecords(10), reader_type));
}

TEST(AvroGenericReader, ResolvingDefaultFixedIsBoundedByItsSize)
{
    /// 1000 empty records, each given a default `fixed` of 4 KiB, take 3 bytes and would create 4 MiB of bytes.
    const std::string reader_type = readerArrayOfRecordWithDefault(
        R"({"type":"fixed","name":"fx","size":4096})", '"' + std::string(4096, 'a') + '"');
    EXPECT_THROW(readEmptyRecordsResolving(1000, reader_type), avro::Exception);
    EXPECT_NO_THROW(readEmptyRecordsResolving(10, reader_type));
}

TEST(AvroGenericReader, ResolvingDefaultArrayOfArraysIsBoundedByTheDatumsItCreates)
{
    /// A default array that holds an array of 300 nulls creates 300 datums for every one of the 1000 empty records.
    std::string inner = "[null";
    for (int i = 1; i < 300; ++i)
        inner += ",null";
    const std::string reader_type = readerArrayOfRecordWithDefault(
        R"({"type":"array","items":{"type":"array","items":"null"}})", "[" + inner + "]]");
    EXPECT_THROW(readAllDatumsResolving(makeFileOfEmptyRecords(1000), reader_type), avro::Exception);
    EXPECT_NO_THROW(readAllDatumsResolving(makeFileOfEmptyRecords(10), reader_type));
}

namespace
{

/// Reads `json` as a value of `schema_json` with the JSON decoder.
void readJson(const std::string & schema_json, const std::string & json)
{
    avro::ValidSchema schema = avro::compileJsonSchemaFromString(schema_json);
    avro::DecoderPtr decoder = avro::jsonDecoder(schema);
    auto in = avro::memoryInputStream(reinterpret_cast<const uint8_t *>(json.data()), json.size());
    decoder->init(*in);
    avro::GenericDatum datum;
    avro::GenericReader::read(*decoder, datum, schema);
}

}

TEST(AvroGenericReader, JsonArrayOfScalarsIsBoundedByTheInput)
{
    /// The JSON decoder peeks at the next token to find the end of an array. The token is not consumed input yet.
    constexpr int items = 70000;
    std::string ints = "[";
    std::string nulls = "[";
    for (int i = 0; i < items; ++i)
    {
        ints += (i ? "," : "") + std::to_string(i);
        nulls += i ? ",null" : "null";
    }
    EXPECT_NO_THROW(readJson(R"({"type":"array","items":"int"})", ints + "]"));
    EXPECT_NO_THROW(readJson(R"({"type":"array","items":"null"})", nulls + "]"));
}

TEST(AvroGenericReader, JsonArrayOfArraysOfNullsIsBoundedByTheInput)
{
    std::string json = "[";
    for (int i = 0; i < 70000; ++i)
        json += i ? ",[null]" : "[null]";
    EXPECT_NO_THROW(readJson(R"({"type":"array","items":{"type":"array","items":"null"}})", json + "]"));
}

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
/// The limit of the memory of the thread is enforced by the tracking `operator new`, which ASan, MSan and TSan replace with
/// their own, so the tests that need it are not built under them.
namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}
#endif

namespace
{

/// `{"type":"record","name":<name>,"fields":[<fields_count fields of type null>]}`, with a default for each field if `with_defaults`.
std::string recordOfNulls(const std::string & name, size_t fields_count, bool with_defaults = false)
{
    std::string fields;
    for (size_t i = 0; i < fields_count; ++i)
        fields += std::string(i ? R"(,{"name":"n)" : R"({"name":"n)") + std::to_string(i) + R"(","type":"null")" + (with_defaults ? R"(,"default":null})" : "}");
    return R"({"type":"record","name":")" + name + R"(","fields":[)" + fields + "]}";
}

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
/// `A0` is a record of two nulls, and `Ai` is a record of two fields of type `A(i-1)`, the first of which defines it and the
/// second of which refers to it. The schema is a few KB, and a datum of `A<levels>` that is constructed in full has
/// `2^(levels + 2) - 1` datums, which are 262143 datums for 16 levels although the value that they hold takes no bytes.
std::string doublingRecords(size_t levels)
{
    auto record = [](const std::string & name, const std::string & first_type, const std::string & second_type)
    { return R"({"type":"record","name":")" + name + R"(","fields":[{"name":"a","type":)" + first_type + R"(},{"name":"b","type":)" + second_type + "}]}"; };
    std::string type = record("A0", R"("null")", R"("null")");
    for (size_t level = 1; level <= levels; ++level)
        type = record("A" + std::to_string(level), type, '"' + std::string("A") + std::to_string(level - 1) + '"');
    return type;
}

constexpr size_t doubling_levels = 16;
#endif

/// A record `W` of `wide_record_fields` records `S` of `wide_record_ints` ints, which has 1 + 300 * (1 + 250) = 75301 datums, of
/// which 75000 are ints, and of which every one is backed by a byte of the input.
constexpr size_t wide_record_fields = 300;
constexpr size_t wide_record_ints = 250;

std::string wideRecord()
{
    std::string inner;
    for (size_t i = 0; i < wide_record_ints; ++i)
        inner += std::string(i ? R"(,{"name":"i)" : R"({"name":"i)") + std::to_string(i) + R"(","type":"int"})";
    std::string fields = R"({"name":"s0","type":{"type":"record","name":"S","fields":[)" + inner + "]}}";
    for (size_t i = 1; i < wide_record_fields; ++i)
        fields += R"(,{"name":"s)" + std::to_string(i) + R"(","type":"S"})";
    return R"({"type":"record","name":"W","fields":[)" + fields + "]}";
}

/// The int of the position `position` of `wideRecord`, which is less than 64, so that its encoding is one byte.
int32_t wideRecordInt(size_t position)
{
    return static_cast<int32_t>((position * 7) % 50);
}

/// The encoding of the value of `wideRecord`, which is `wide_record_fields * wide_record_ints` bytes.
std::vector<uint8_t> wideRecordData()
{
    std::vector<uint8_t> data;
    for (size_t i = 0; i < wide_record_fields * wide_record_ints; ++i)
        appendLong(data, wideRecordInt(i));
    return data;
}

/// The number of ints of a value of `wideRecord` that differ from `wideRecordInt`, and 1 if its shape differs.
size_t wideRecordMismatches(const avro::GenericDatum & datum)
{
    if (datum.type() != avro::AVRO_RECORD)
        return 1;
    const auto & record = datum.value<avro::GenericRecord>();
    if (record.fieldCount() != wide_record_fields)
        return 1;
    size_t mismatches = 0;
    for (size_t s = 0; s < wide_record_fields; ++s)
    {
        const auto & inner = record.fieldAt(s).value<avro::GenericRecord>();
        if (inner.fieldCount() != wide_record_ints)
            return 1;
        for (size_t i = 0; i < wide_record_ints; ++i)
            mismatches += inner.fieldAt(i).value<int32_t>() != wideRecordInt(s * wide_record_ints + i);
    }
    return mismatches;
}

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
/// What reading ended with.
enum class ReadOutcome
{
    Read,
    AvroException,
    MemoryLimitExceeded,
};

/// Limits the memory of the thread until it is destroyed, in the way `gtest_reverse_index_exception_safety` does, and records
/// the most that the thread held above what it held at the start.
class MemoryLimit
{
public:
    explicit MemoryLimit(Int64 limit)
        : thread_tracker(DB::CurrentThread::get().memory_tracker)
        , saved_untracked_limit(DB::CurrentThread::get().untracked_memory_limit)
        , saved_thread_hard_limit(thread_tracker.getHardLimit())
        , saved_min_alloc_to_throw(CurrentMemoryTracker::getMinAllocationSizeBytesToThrow())
    {
        /// Every allocation reaches the tracker, and an allocation of 256 KiB or more is refused above the limit.
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(256 * 1024);
        DB::CurrentThread::get().untracked_memory_limit = 0;
        DB::CurrentThread::flushUntrackedMemory();
        thread_tracker.resetCounters();
        thread_tracker.setHardLimit(limit);
    }

    ~MemoryLimit()
    {
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        DB::CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    }

    MemoryLimit(const MemoryLimit &) = delete;
    MemoryLimit & operator=(const MemoryLimit &) = delete;

    Int64 peak() const { return thread_tracker.getPeak(); }

private:
    MemoryTracker & thread_tracker;
    Int64 saved_untracked_limit;
    Int64 saved_thread_hard_limit;
    UInt64 saved_min_alloc_to_throw;
};
#endif

/// A binary-encoded value that is read without a data file, so that the schema, the decoder and the datum
/// are built before the read is limited. Reads with the schema `schema`, or resolves `writer_schema` to `schema`.
class PreparedRead
{
public:
    PreparedRead(const std::string & schema_json, std::vector<uint8_t> data_)
        : data(std::move(data_)), schema(avro::compileJsonSchemaFromString(schema_json)), decoder(avro::binaryDecoder())
    {
        init(schema);
    }

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
    PreparedRead(const std::string & writer_schema_json, const std::string & reader_schema_json, std::vector<uint8_t> data_)
        : data(std::move(data_)), schema(avro::compileJsonSchemaFromString(reader_schema_json))
    {
        avro::ValidSchema writer_schema = avro::compileJsonSchemaFromString(writer_schema_json);
        decoder = avro::resolvingDecoder(writer_schema, schema, avro::binaryDecoder());
        init(schema);
    }
#endif

    /// Reads into the datum that was constructed from the schema.
    void read() { avro::GenericReader::read(*decoder, datum); }

    /// Reads with `GenericReader::read(datum)`, which constructs the root from the schema before it reads.
    void readWithReader() { avro::GenericReader(schema, decoder).read(datum); }

    /// Reads with `GenericReader::read(decoder, datum, schema)`, which constructs the root from the schema before it reads.
    void readWithRootSchema() { avro::GenericReader::read(*decoder, datum, schema); }

    const avro::GenericDatum & result() const { return datum; }
    uint64_t inputPosition() const { return decoder->inputPosition(); }
    size_t inputSize() const { return data.size(); }

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
    /// Reads while the thread may hold at most `limit` bytes of what the read allocates, and records the most that it held.
    ReadOutcome readWithMemoryLimit(Int64 limit)
    {
        return readWithMemoryLimit(limit, [this] { read(); });
    }

    template <typename Read>
    ReadOutcome readWithMemoryLimit(Int64 limit, Read && do_read)
    {
        MemoryLimit memory_limit(limit);
        ReadOutcome outcome = ReadOutcome::Read;
        try
        {
            do_read();
        }
        catch (const avro::Exception &)
        {
            outcome = ReadOutcome::AvroException;
        }
        catch (const DB::Exception & e)
        {
            if (e.code() != DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;
            outcome = ReadOutcome::MemoryLimitExceeded;
        }
        peak_memory = memory_limit.peak();
        return outcome;
    }

    /// The most that the thread held above its memory at the start of the last `readWithMemoryLimit`.
    Int64 peakMemory() const { return peak_memory; }
#endif

private:
    void init(const avro::ValidSchema & datum_schema)
    {
        DB::MainThreadStatus::getInstance();
        in = avro::memoryInputStream(data.data(), data.size());
        decoder->init(*in);
        datum = avro::GenericDatum(datum_schema, avro::ShallowTag());
    }

    std::vector<uint8_t> data;
    avro::ValidSchema schema;
    avro::DecoderPtr decoder;
    std::unique_ptr<avro::InputStream> in;
    avro::GenericDatum datum;
#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
    Int64 peak_memory = 0;
#endif
};

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
/// 1.5 MiB, below the 100 MiB that a schema may declare for a `fixed`.
constexpr Int64 memory_limit_below_a_declared_fixed = Int64(3) << 19;

/// What reading may hold when it is rejected for having created the datums of the whole budget of 65536, as it is for every
/// stretch of the input that does not advance: about 4 MiB for datums of the kind that the tests create, and then a vector
/// that doubles. The datums of a schema that is constructed in full, as the doubling records are, are 18 MiB.
constexpr Int64 memory_of_a_full_budget = Int64(12) << 20;

/// Datums in a branch that does not fit the budget, and in one that does.
constexpr size_t branch_over_the_budget = 70000;
constexpr size_t branch_in_the_budget = 40000;
#endif

std::string wideUnion(size_t fields_count)
{
    return R"(["null",)" + recordOfNulls("wide", fields_count) + "]";
}

}

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)

TEST(AvroGenericReader, FixedDeclaringMoreBytesThanDataBacksIsRejectedBeforeItIsAllocated)
{
    /// The schema declares a `fixed` of 100 MiB, and the input has none of its bytes. Neither constructing the datum nor reading
    /// may allocate the declared size, which exceeds the limit, so the read ends with `avro::Exception`.
    PreparedRead read(makeRecordSchema(R"({"type":"fixed","name":"fx","size":104857600})"), {});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_declared_fixed), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, ShallowRootOfRecordsThatRepeatIsNotConstructedFromTheSchema)
{
    /// A shallow datum holds a record and no fields, and not the 262143 datums that the schema describes, which are 18 MiB for a
    /// schema of 1.5 KB.
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(doublingRecords(doubling_levels));
    DB::MainThreadStatus::getInstance();
    MemoryLimit limit(Int64(1) << 30);
    const avro::GenericDatum datum(schema, avro::ShallowTag());
    EXPECT_LT(limit.peak(), Int64(64) << 10);
    EXPECT_EQ(datum.value<avro::GenericRecord>().fieldCount(), 0U);
}

TEST(AvroGenericReader, RootWithoutTheTagIsConstructedInFullWithItsDefaults)
{
    /// The constructor without the tag is as it was: it constructs every datum of the schema, here the 16383 of 12 levels, with
    /// the default value of its type, which a const access returns. That is outside what `GenericReader` bounds.
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(doublingRecords(12));
    DB::MainThreadStatus::getInstance();
    MemoryLimit limit(Int64(1) << 30);
    const avro::GenericDatum datum(schema);
    EXPECT_LT(Int64(512) << 10, limit.peak());

    const avro::GenericDatum * record = &datum;
    size_t depth = 0;
    while (record->type() == avro::AVRO_RECORD)
    {
        const auto & fields = record->value<avro::GenericRecord>();
        ASSERT_EQ(fields.fieldCount(), 2U);
        EXPECT_EQ(fields.fieldAt(1).type(), depth < 12 ? avro::AVRO_RECORD : avro::AVRO_NULL);
        record = &fields.fieldAt(0);
        ++depth;
    }
    EXPECT_EQ(depth, 13U);
    EXPECT_EQ(record->type(), avro::AVRO_NULL);
}

TEST(AvroGenericReader, RecordsThatRepeatAreRejectedAfterTheBudgetNotAfterTheSchema)
{
    /// A value of 262143 datums that takes no bytes is read from no input by each way of reading a root that `GenericReader`
    /// constructs or is given shallow, and is rejected after the datums of one budget.
    const std::string schema = doublingRecords(doubling_levels);
    {
        PreparedRead read(schema, {});
        EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
        EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
    }
    {
        PreparedRead read(schema, {});
        EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget, [&] { read.readWithReader(); }), ReadOutcome::AvroException);
        EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
    }
    {
        PreparedRead read(schema, {});
        EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget, [&] { read.readWithRootSchema(); }), ReadOutcome::AvroException);
        EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
    }
}

TEST(AvroGenericReader, DataFileOfRecordsThatRepeatIsRejectedAfterTheBudgetNotAfterTheSchema)
{
    /// The way `Iceberg::forEachAvroEntry` reads: a `DataFileReader` that reads into a shallow datum that the caller constructs
    /// from the schema of the file, which is 1.5 KB, and an object that takes no bytes.
    const auto file = makeFile(doublingRecords(doubling_levels), {});
    DB::MainThreadStatus::getInstance();
    MemoryLimit limit(memory_of_a_full_budget);
    EXPECT_THROW(readAllDatums(file), avro::Exception);
    EXPECT_LT(limit.peak(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, SchemaWithDefaultOfRecordsThatRepeatIsCompiledFromTheDefaultNotFromTheSchema)
{
    /// A field of the 262143 datums of `A16` whose default is `{}` is rejected for the field that the default lacks, and a
    /// schema of 1.7 KB does not make the compiler construct those datums to find that out.
    const std::string type = R"({"type":"record","name":"R","fields":[{"name":"x","type":)" + doublingRecords(doubling_levels) + R"(,"default":{}}]})";
    DB::MainThreadStatus::getInstance();
    MemoryLimit limit(Int64(1) << 30);
    EXPECT_THROW(avro::compileJsonSchemaFromString(type), avro::Exception);
    EXPECT_LT(limit.peak(), Int64(1) << 20);
}

TEST(AvroGenericReader, ResolvedDefaultOfRecordsThatRepeatIsRead)
{
    /// The default of `A3` is given in full, and takes the 4 levels of records that the schema describes.
    std::string value = R"({"a":null,"b":null})";
    for (size_t level = 1; level <= 3; ++level)
        value = R"({"a":)" + value + R"(,"b":)" + value + "}";
    const std::string writer_type = R"({"type":"record","name":"R","fields":[]})";
    const std::string reader_type = R"({"type":"record","name":"R","fields":[{"name":"x","type":)" + doublingRecords(3) + R"(,"default":)" + value + "}]}";
    PreparedRead read(writer_type, reader_type, {});
    read.read();

    const avro::GenericDatum * datum = &read.result().value<avro::GenericRecord>().field("x");
    size_t depth = 0;
    while (datum->type() == avro::AVRO_RECORD)
    {
        const auto & record = datum->value<avro::GenericRecord>();
        ASSERT_EQ(record.fieldCount(), 2U);
        EXPECT_EQ(record.fieldAt(1).type(), depth < 3 ? avro::AVRO_RECORD : avro::AVRO_NULL);
        datum = &record.fieldAt(0);
        ++depth;
    }
    EXPECT_EQ(depth, 4U);
    EXPECT_EQ(datum->type(), avro::AVRO_NULL);
}

TEST(AvroGenericReader, RecordsThatRepeatWithinTheBudgetAreRead)
{
    /// The same schema with 12 levels has 16383 datums, which fit the budget, and every one of them is read.
    PreparedRead read(doublingRecords(12), {});
    EXPECT_NO_THROW(read.readWithRootSchema());
    const auto * record = &read.result();
    size_t depth = 0;
    while (record->type() == avro::AVRO_RECORD)
    {
        const auto & fields = record->value<avro::GenericRecord>();
        ASSERT_EQ(fields.fieldCount(), 2U);
        record = &fields.fieldAt(0);
        ++depth;
    }
    EXPECT_EQ(depth, 13U);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetInRecordFieldIsRejected)
{
    PreparedRead read(makeRecordSchema(wideUnion(branch_over_the_budget)), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAsArrayItemIsRejected)
{
    /// One item, which selects the second branch of its union.
    PreparedRead read(makeRecordSchema(R"({"type":"array","items":)" + wideUnion(branch_over_the_budget) + "}"), {0x02, 0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAsMapValueIsRejected)
{
    /// One entry with the key "k", whose value selects the second branch of its union.
    PreparedRead read(makeRecordSchema(R"({"type":"map","values":)" + wideUnion(branch_over_the_budget) + "}"), {0x02, 0x02, 'k', 0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAtTheRootIsRejected)
{
    PreparedRead read(wideUnion(branch_over_the_budget), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, ResolvedUnionBranchOverTheBudgetIsRejected)
{
    /// The writer's empty record resolves to the second branch of the reader's union without any input,
    /// and every field of the branch is given a default.
    const std::string writer_type = R"({"type":"array","items":{"type":"record","name":"wide","fields":[]}})";
    const std::string reader_type = R"({"type":"array","items":["null",)" + recordOfNulls("wide", branch_over_the_budget, true) + "]}";
    PreparedRead read(writer_type, reader_type, {0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(read.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, ResolvedUnionBranchesThatDoNotFitTheBudgetTogetherAreRejected)
{
    /// Two fields take no input, and each resolves the empty record of the writer to the second branch of its union. The
    /// branches have 12000 and 54000 datums: each fits the budget of 65536 alone, and the two do not fit it together.
    const std::string first_writer_field = R"({"name":"f1","type":{"type":"record","name":"first","fields":[]}})";
    const std::string second_writer_field = R"({"name":"f2","type":{"type":"record","name":"second","fields":[]}})";
    const std::string first_reader_field = R"({"name":"f1","type":["null",)" + recordOfNulls("first", 12000, true) + "]}";
    const std::string second_reader_field = R"({"name":"f2","type":["null",)" + recordOfNulls("second", 54000, true) + "]}";
    auto record = [](const std::string & fields) { return R"({"type":"record","name":"r","fields":[)" + fields + "]}"; };

    EXPECT_NO_THROW(PreparedRead(record(first_writer_field), record(first_reader_field), {0x00}).read());
    EXPECT_NO_THROW(PreparedRead(record(second_writer_field), record(second_reader_field), {0x00}).read());

    PreparedRead both(record(first_writer_field + "," + second_writer_field), record(first_reader_field + "," + second_reader_field), {0x00});
    EXPECT_EQ(both.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::AvroException);
    EXPECT_LT(both.peakMemory(), memory_of_a_full_budget);
}

TEST(AvroGenericReader, UnionBranchInTheBudgetIsRead)
{
    /// A wide branch that fits the budget is not rejected, and does hold the memory that the rejected ones would.
    PreparedRead read(makeRecordSchema(wideUnion(branch_in_the_budget)), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(memory_of_a_full_budget), ReadOutcome::Read);

    PreparedRead over_the_limit(makeRecordSchema(wideUnion(branch_in_the_budget)), {0x02});
    EXPECT_EQ(over_the_limit.readWithMemoryLimit(Int64(1) << 20), ReadOutcome::MemoryLimitExceeded);
}

TEST(AvroGenericReader, ResolvedWideRecordBackedByTheInputIsRead)
{
    /// The 75301 datums of the record are more than the budget, and every int of them is backed by a byte.
    const std::string type = wideRecord();
    const std::vector<uint8_t> data = wideRecordData();
    {
        PreparedRead read(type, type, data);
        read.read();
        EXPECT_EQ(wideRecordMismatches(read.result()), 0U);
        EXPECT_EQ(read.inputPosition(), read.inputSize());
    }
    {
        PreparedRead read(type, type, data);
        read.readWithReader();
        EXPECT_EQ(wideRecordMismatches(read.result()), 0U);
    }
    {
        PreparedRead read(type, type, data);
        read.readWithRootSchema();
        EXPECT_EQ(wideRecordMismatches(read.result()), 0U);
    }
}

TEST(AvroGenericReader, ResolvedFieldsOfAnotherOrderAndDefaultsAreRead)
{
    /// The writer's fields come in the order c, b, a, and the reader's in the order a, b, c, so that `a` is read last and the
    /// fields that precede it are constructed first. The reader also adds a record with a `fixed`, a map and an array
    /// as defaults.
    const std::string writer_type = R"({"type":"record","name":"r","fields":[{"name":"c","type":"int"},{"name":"b","type":"int"},{"name":"a","type":"int"}]})";
    const std::string reader_type = R"({"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"int"},)"
        R"({"name":"d","type":{"type":"record","name":"d","fields":[{"name":"x","type":"int"},{"name":"f","type":{"type":"fixed","name":"fx","size":4}}]},"default":{"x":7,"f":"abcd"}},)"
        R"({"name":"m","type":{"type":"map","values":"string"},"default":{"k":"v"}},)"
        R"({"name":"l","type":{"type":"array","items":"long"},"default":[5,6]}]})";
    PreparedRead read(writer_type, reader_type, {0x02, 0x04, 0x06});
    read.read();
    EXPECT_EQ(read.inputPosition(), read.inputSize());

    const auto & record = read.result().value<avro::GenericRecord>();
    EXPECT_EQ(record.field("a").value<int32_t>(), 3);
    EXPECT_EQ(record.field("b").value<int32_t>(), 2);
    EXPECT_EQ(record.field("c").value<int32_t>(), 1);

    const auto & d = record.field("d").value<avro::GenericRecord>();
    EXPECT_EQ(d.field("x").value<int32_t>(), 7);
    const std::vector<uint8_t> expected_fixed = {'a', 'b', 'c', 'd'};
    EXPECT_TRUE(d.field("f").value<avro::GenericFixed>().value() == expected_fixed);

    const auto & m = record.field("m").value<avro::GenericMap>().value();
    ASSERT_EQ(m.size(), 1U);
    EXPECT_TRUE(m[0].first == "k");
    EXPECT_TRUE(m[0].second.value<std::string>() == "v");

    const auto & l = record.field("l").value<avro::GenericArray>().value();
    ASSERT_EQ(l.size(), 2U);
    EXPECT_EQ(l[0].value<int64_t>(), 5);
    EXPECT_EQ(l[1].value<int64_t>(), 6);
}

namespace
{

/// A record of `fields` fields `f0`, `f1`, ... of the type `type`, which are in the reverse order if `reversed`.
std::string recordOfFields(size_t fields, const std::string & type, bool reversed)
{
    std::string result = R"({"type":"record","name":"r","fields":[)";
    for (size_t i = 0; i < fields; ++i)
        result += std::string(i ? "," : "") + R"({"name":"f)" + std::to_string(reversed ? fields - 1 - i : i) + R"(","type":)" + type + "}";
    return result + "]}";
}

constexpr size_t fields_over_the_budget = 70000;

}

TEST(AvroGenericReader, ResolvedRecordOfMoreFieldsThanTheBudgetIsReadInAnotherOrder)
{
    /// The writer has 70000 ints in the reverse order of the reader. The first field that is read is the last one of the reader's
    /// schema, which is 69999 fields down from the first, and every field is backed by a byte. (Building the resolving decoder
    /// of such a record takes seconds, so it is read once.)
    std::vector<uint8_t> data;
    for (size_t i = 0; i < fields_over_the_budget; ++i)
        appendLong(data, wideRecordInt(fields_over_the_budget - 1 - i));

    PreparedRead read(recordOfFields(fields_over_the_budget, R"("int")", true), recordOfFields(fields_over_the_budget, R"("int")", false), data);
    read.readWithRootSchema();

    EXPECT_EQ(read.inputPosition(), read.inputSize());
    const auto & record = read.result().value<avro::GenericRecord>();
    ASSERT_EQ(record.fieldCount(), fields_over_the_budget);
    size_t mismatches = 0;
    for (size_t i = 0; i < fields_over_the_budget; ++i)
        mismatches += record.fieldAt(i).value<int32_t>() != wideRecordInt(i);
    EXPECT_EQ(mismatches, 0U);
}

TEST(AvroGenericReader, ResolvedFieldThatIsReadBeforeTheOnesThatPrecedeItIsBounded)
{
    /// The writer has the array of nulls `a` first, and the reader has it after the int `x`, so that it is read into a datum of
    /// its own. It declares more items than the budget and has no input for them, so it is rejected as it is in its own place.
    const std::string writer_type = R"({"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":"null"}},{"name":"x","type":"int"}]})";
    const std::string reader_type = R"({"type":"record","name":"r","fields":[{"name":"x","type":"int"},{"name":"a","type":{"type":"array","items":"null"}}]})";
    std::vector<uint8_t> data;
    appendLong(data, huge_item_count);
    EXPECT_THROW(PreparedRead(writer_type, reader_type, data).read(), avro::Exception);

    /// With few items it is read: three items that take no bytes, the end of the array, and `x` that follows it in the input.
    const std::vector<uint8_t> valid = {0x06, 0x00, 0x14};
    PreparedRead read(writer_type, reader_type, valid);
    read.read();
    const auto & record = read.result().value<avro::GenericRecord>();
    EXPECT_EQ(record.field("x").value<int32_t>(), 10);
    EXPECT_EQ(record.field("a").value<avro::GenericArray>().value().size(), 3U);
}

#endif

namespace
{

/// Reads the value of `schema` that `data` encodes by each way of reading a root, and checks that `select` finds in the datum
/// the values of `wideRecord` that `data` holds, and that the whole input was read.
void expectWideRecordsAreRead(
    const std::string & schema,
    const std::vector<uint8_t> & data,
    const std::function<std::vector<const avro::GenericDatum *>(const avro::GenericDatum &)> & select,
    size_t expected_count)
{
    for (int way = 0; way < 3; ++way)
    {
        PreparedRead read(schema, data);
        if (way == 0)
            read.read();
        else if (way == 1)
            read.readWithReader();
        else
            read.readWithRootSchema();

        const auto records = select(read.result());
        ASSERT_EQ(records.size(), expected_count);
        for (const auto * record : records)
            EXPECT_EQ(wideRecordMismatches(*record), 0U);
        EXPECT_EQ(read.inputPosition(), read.inputSize());
    }
}

std::vector<uint8_t> concat(const std::vector<std::vector<uint8_t>> & parts)
{
    std::vector<uint8_t> result;
    for (const auto & part : parts)
        result.insert(result.end(), part.begin(), part.end());
    return result;
}

std::vector<uint8_t> encode(const avro::GenericDatum & datum)
{
    auto out = avro::memoryOutputStream();
    auto encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::GenericWriter::write(*encoder, datum);
    encoder->flush();
    return *avro::snapshot(*out);
}

}

TEST(AvroGenericReader, WideRecordBackedByTheInputIsReadAtTheRoot)
{
    /// A record of 75301 datums, of which 75000 are ints that are one byte each, is backed by its input however it is read.
    expectWideRecordsAreRead(wideRecord(), wideRecordData(), [](const avro::GenericDatum & datum) { return std::vector{&datum}; }, 1);
}

TEST(AvroGenericReader, WideRecordBackedByTheInputIsReadAsRecordField)
{
    expectWideRecordsAreRead(
        makeRecordSchema(wideRecord()),
        wideRecordData(),
        [](const avro::GenericDatum & datum) { return std::vector{&datum.value<avro::GenericRecord>().fieldAt(0)}; },
        1);
}

TEST(AvroGenericReader, WideRecordBackedByTheInputIsReadAsUnionBranchAtTheRoot)
{
    expectWideRecordsAreRead(
        R"(["null",)" + wideRecord() + "]",
        concat({{0x02}, wideRecordData()}),
        [](const avro::GenericDatum & datum) { return std::vector{&datum}; },
        1);
}

TEST(AvroGenericReader, WideRecordBackedByTheInputIsReadAsUnionBranchOfField)
{
    expectWideRecordsAreRead(
        makeRecordSchema(R"(["null",)" + wideRecord() + "]"),
        concat({{0x02}, wideRecordData()}),
        [](const avro::GenericDatum & datum)
        {
            const avro::GenericDatum & field = datum.value<avro::GenericRecord>().fieldAt(0);
            EXPECT_EQ(field.unionBranch(), 1U);
            return std::vector{&field};
        },
        1);
}

TEST(AvroGenericReader, WideRecordsBackedByTheInputAreReadAsArrayItems)
{
    /// Two items that are 150602 datums together.
    const std::vector<uint8_t> data = wideRecordData();
    expectWideRecordsAreRead(
        makeRecordSchema(R"({"type":"array","items":)" + wideRecord() + "}"),
        concat({{0x04}, data, data, {0x00}}),
        [](const avro::GenericDatum & datum)
        {
            const auto & items = datum.value<avro::GenericRecord>().fieldAt(0).value<avro::GenericArray>().value();
            std::vector<const avro::GenericDatum *> result;
            for (const auto & item : items)
                result.push_back(&item);
            return result;
        },
        2);
}

TEST(AvroGenericReader, WideRecordsBackedByTheInputAreReadAsMapValues)
{
    const std::vector<uint8_t> data = wideRecordData();
    expectWideRecordsAreRead(
        makeRecordSchema(R"({"type":"map","values":)" + wideRecord() + "}"),
        concat({{0x04, 0x02, 'a'}, data, {0x02, 'b'}, data, {0x00}}),
        [](const avro::GenericDatum & datum)
        {
            const auto & entries = datum.value<avro::GenericRecord>().fieldAt(0).value<avro::GenericMap>().value();
            std::vector<const avro::GenericDatum *> result;
            for (const auto & entry : entries)
                result.push_back(&entry.second);
            EXPECT_TRUE(entries.size() == 2 && entries[0].first == "a" && entries[1].first == "b");
            return result;
        },
        2);
}

TEST(AvroGenericReader, WideRecordsAreReadAsUnionBranchesOfArrayItems)
{
    /// Each of the two items selects the second branch, whose 75301 datums are the same record.
    const std::vector<uint8_t> data = wideRecordData();
    expectWideRecordsAreRead(
        makeRecordSchema(R"({"type":"array","items":["null",)" + wideRecord() + "]}"),
        concat({{0x04, 0x02}, data, {0x02}, data, {0x00}}),
        [](const avro::GenericDatum & datum)
        {
            const auto & items = datum.value<avro::GenericRecord>().fieldAt(0).value<avro::GenericArray>().value();
            std::vector<const avro::GenericDatum *> result;
            for (const auto & item : items)
                result.push_back(&item);
            return result;
        },
        2);
}

TEST(AvroGenericReader, DatumWithoutTheTagKeepsItsDefaultsAndItsConstAccess)
{
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(
        R"({"type":"record","name":"o","fields":[{"name":"a","type":"int"},)"
        R"({"name":"i","type":{"type":"record","name":"in","fields":[{"name":"x","type":"long"},{"name":"s","type":"string"}]}},)"
        R"({"name":"u","type":["null","int"]},{"name":"d","type":"double"}]})");

    /// Every field of the record and of the records in it is there, with the default value of its type, to a const access.
    const avro::GenericDatum datum(schema);
    const auto & record = datum.value<avro::GenericRecord>();
    EXPECT_EQ(record.fieldCount(), 4U);
    EXPECT_EQ(record.fieldAt(0).value<int32_t>(), 0);
    const auto & inner = record.field("i").value<avro::GenericRecord>();
    EXPECT_EQ(inner.fieldCount(), 2U);
    EXPECT_EQ(inner.field("x").value<int64_t>(), 0);
    EXPECT_TRUE(inner.field("s").value<std::string>().empty());
    EXPECT_EQ(record.field("u").unionBranch(), 0U);
    EXPECT_EQ(record.field("d").value<double>(), 0.0);

    /// A field that is set is the one that a copy has.
    avro::GenericDatum other(schema);
    other.value<avro::GenericRecord>().setFieldAt(3, avro::GenericDatum(2.5));
    const avro::GenericDatum copy = other;
    EXPECT_EQ(copy.value<avro::GenericRecord>().fieldCount(), 4U);
    EXPECT_EQ(copy.value<avro::GenericRecord>().fieldAt(3).value<double>(), 2.5);
    EXPECT_EQ(copy.value<avro::GenericRecord>().fieldAt(0).value<int32_t>(), 0);
}

TEST(AvroGenericReader, ShallowDatumIsFilledByTheReadAndRefusedByTheWriterBefore)
{
    const std::string type = R"({"type":"record","name":"o","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]})";
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(type);
    const avro::GenericDatum unread(schema, avro::ShallowTag());
    EXPECT_EQ(unread.value<avro::GenericRecord>().fieldCount(), 0U);
    EXPECT_THROW(encode(unread), avro::Exception);

    PreparedRead read(type, {0x02, 0x04, 'h', 'i'});
    read.read();
    const auto & record = read.result().value<avro::GenericRecord>();
    EXPECT_EQ(record.fieldCount(), 2U);
    EXPECT_EQ(record.field("a").value<int32_t>(), 1);
    EXPECT_TRUE(record.field("b").value<std::string>() == "hi");
    const std::vector<uint8_t> expected = {0x02, 0x04, 'h', 'i'};
    EXPECT_TRUE(encode(read.result()) == expected);
}

TEST(AvroGenericReader, WriterWritesTheDefaultsOfFieldsThatNobodyAccessed)
{
    const std::string type
        = R"({"type":"record","name":"o","fields":[{"name":"a","type":"int"},)"
          R"({"name":"i","type":{"type":"record","name":"in","fields":[{"name":"x","type":"long"},{"name":"s","type":"string"}]}},)"
          R"({"name":"u","type":["null","int"]},{"name":"d","type":"double"}]})";
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(type);

    avro::GenericDatum datum(schema);
    auto & record = datum.value<avro::GenericRecord>();
    record.field("a") = avro::GenericDatum(int32_t(7));

    /// `a` is 7, the long and the empty string of `i` and the null branch of `u` are one byte each, and `d` is 8 bytes of 0.0.
    const std::vector<uint8_t> expected = {0x0e, 0x00, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_TRUE(encode(datum) == expected);

    /// The datum is const to the writer and was not changed by it.
    EXPECT_EQ(record.fieldCount(), 4U);

    PreparedRead read(type, expected);
    read.read();
    const auto & result = read.result().value<avro::GenericRecord>();
    EXPECT_EQ(result.field("a").value<int32_t>(), 7);
    EXPECT_EQ(result.field("i").value<avro::GenericRecord>().field("x").value<int64_t>(), 0);
    EXPECT_EQ(result.field("u").unionBranch(), 0U);
    EXPECT_EQ(result.field("d").value<double>(), 0.0);

    /// Setting a field of the nested record, and one after the fields that nobody accessed.
    record.field("i").value<avro::GenericRecord>().field("s") = avro::GenericDatum(std::string("hi"));
    record.field("d") = avro::GenericDatum(1.0);
    EXPECT_EQ(encode(datum).size(), 1U + 1U + 3U + 1U + 8U);
}

TEST(AvroGenericReader, WriterWritesAFixedThatIsSetAndRefusesOneThatIsNot)
{
    /// The contents of a `fixed` are empty when the datum is constructed, so a written one has the size of its schema or
    /// nothing is written for it.
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(
        R"({"type":"record","name":"o","fields":[{"name":"f","type":{"type":"fixed","name":"fx","size":4}},)"
        R"({"name":"g","type":{"type":"fixed","name":"gx","size":2}}]})");

    avro::GenericDatum datum(schema);
    auto & record = datum.value<avro::GenericRecord>();
    record.field("f").value<avro::GenericFixed>().value() = {1, 2, 3, 4};
    EXPECT_THROW(encode(datum), avro::Exception);

    record.field("g").value<avro::GenericFixed>().value() = {5, 6};
    const std::vector<uint8_t> expected = {1, 2, 3, 4, 5, 6};
    EXPECT_TRUE(encode(datum) == expected);
}

TEST(AvroGenericReader, RecordThatContainsItselfIsNotBuiltOrReadToTheEnd)
{
    /// A value of this record would have infinitely many fields. Building the datum in full and reading it end with
    /// `avro::Exception`, and not with a stack that overflows.
    const std::string type = R"({"type":"record","name":"R","fields":[{"name":"f","type":"R"}]})";
    EXPECT_THROW(avro::GenericDatum(avro::compileJsonSchemaFromString(type)), avro::Exception);
    EXPECT_THROW(PreparedRead(type, {}).read(), avro::Exception);
}

TEST(AvroGenericReader, ReusedDatumIsReadAgainAndAfterAReadThatFailed)
{
    /// `DataFileReader` reads every object into the same datum, whether it is shallow or constructed in full. A read that fails
    /// leaves some of the fields of a shallow datum constructed.
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(
        R"({"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},)"
        R"({"name":"c","type":{"type":"array","items":"int"}},)"
        R"({"name":"d","type":{"type":"record","name":"d","fields":[{"name":"x","type":"int"}]}}]})");

    for (const bool shallow : {true, false})
    {
        avro::GenericDatum datum = shallow ? avro::GenericDatum(schema, avro::ShallowTag()) : avro::GenericDatum(schema);
        auto read = [&](const std::vector<uint8_t> & data)
        {
            auto in = avro::memoryInputStream(data.data(), data.size());
            auto decoder = avro::binaryDecoder();
            decoder->init(*in);
            avro::GenericReader::read(*decoder, datum);
        };
        auto expect_first = [&]
        {
            const auto & record = datum.value<avro::GenericRecord>();
            EXPECT_EQ(record.fieldCount(), 4U);
            EXPECT_EQ(record.field("a").value<int32_t>(), 1);
            EXPECT_TRUE(record.field("b").value<std::string>() == "xy");
            const auto & c = record.field("c").value<avro::GenericArray>().value();
            ASSERT_EQ(c.size(), 2U);
            EXPECT_EQ(c[0].value<int32_t>() + c[1].value<int32_t>(), 11);
            EXPECT_EQ(record.field("d").value<avro::GenericRecord>().field("x").value<int32_t>(), 9);
        };

        const std::vector<uint8_t> first = {0x02, 0x04, 'x', 'y', 0x04, 0x0a, 0x0c, 0x00, 0x12};
        read(first);
        expect_first();

        read({0x04, 0x00, 0x00, 0x06});
        const auto & second = datum.value<avro::GenericRecord>();
        EXPECT_EQ(second.field("a").value<int32_t>(), 2);
        EXPECT_TRUE(second.field("b").value<std::string>().empty());
        EXPECT_TRUE(second.field("c").value<avro::GenericArray>().value().empty());
        EXPECT_EQ(second.field("d").value<avro::GenericRecord>().field("x").value<int32_t>(), 3);

        /// `a` is 3, and the string `b` of 1 byte is missing.
        EXPECT_THROW(read({0x06, 0x02}), avro::Exception);
        read(first);
        expect_first();
    }
}

TEST(AvroGenericReader, SmallUnionBranchesBackedByTheInputAreNotLimited)
{
    /// 70000 items select the second branch of a union of 11 datums. Each item reads its own index of the branch,
    /// so the input advances and the budget refills, although the branches add up to far more than it holds.
    constexpr int64_t items = 70000;
    std::vector<uint8_t> data;
    appendLong(data, items);
    for (int64_t i = 0; i < items; ++i)
        appendLong(data, 1);
    appendLong(data, 0);
    EXPECT_NO_THROW(PreparedRead(makeRecordSchema(R"({"type":"array","items":)" + wideUnion(10) + "}"), data).read());
}

TEST(AvroGenericReader, RecursiveUnionBranchesAreRead)
{
    /// A list of 3 nodes, as a record whose union selects the recursive branch twice.
    const std::string node = R"({"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]})";
    EXPECT_NO_THROW(PreparedRead(node, {0x02, 0x02, 0x00}).read());

    /// The same behind a union at the root and a recursive record that is the second branch of an item.
    EXPECT_NO_THROW(PreparedRead(R"(["null",)" + node + "]", {0x02, 0x02, 0x02, 0x00}).read());
    EXPECT_NO_THROW(PreparedRead(
        makeRecordSchema(R"({"type":"array","items":["null",)" + node + "]}"), {0x02, 0x02, 0x02, 0x00, 0x00}).read());
}

TEST(AvroGenericReader, DeserializerRetainsFixedSizeAfterTruncatedRead)
{
    /// A truncated `fixed` read leaves `tmp_fixed` partially resized inside `readSizedInto`. Reusing the same
    /// `AvroDeserializer` on a valid value must still read the full declared width rather than `tmp_fixed.size()`.
    constexpr size_t fixed_size = 131072;
    const avro::ValidSchema schema = avro::compileJsonSchemaFromString(
        R"({"type":"record","name":"r","fields":[{"name":"x","type":{"type":"fixed","name":"f","size":)" + std::to_string(fixed_size) + "}}]}");

    const DB::DataTypePtr type = std::make_shared<DB::DataTypeFixedString>(fixed_size);
    const DB::Block header({DB::ColumnWithTypeAndName(type->createColumn(), type, "x")});
    const DB::FormatSettings format_settings;
    DB::AvroDeserializer deserializer(header, schema, /*allow_missing_fields=*/false, /*null_as_default=*/false, format_settings);

    const std::vector<uint8_t> truncated(65536, 'a');
    {
        auto in = avro::memoryInputStream(truncated.data(), truncated.size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);
        auto columns = header.cloneEmptyColumns();
        DB::RowReadExtension ext;
        EXPECT_THROW(deserializer.deserializeRow(columns, *decoder, ext), avro::Exception);
    }

    std::vector<uint8_t> valid(fixed_size, 'b');
    valid.back() = 'z';
    {
        auto in = avro::memoryInputStream(valid.data(), valid.size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);
        auto columns = header.cloneEmptyColumns();
        DB::RowReadExtension ext;
        EXPECT_NO_THROW(deserializer.deserializeRow(columns, *decoder, ext));
        ASSERT_EQ(columns[0]->size(), 1U);
        EXPECT_EQ(columns[0]->getDataAt(0), std::string_view(reinterpret_cast<const char *>(valid.data()), valid.size()));
        EXPECT_EQ(decoder->inputPosition(), valid.size());
    }
}

#endif
