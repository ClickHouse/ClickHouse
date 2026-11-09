#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <cstdint>
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
    avro::GenericDatum datum(reader.dataSchema());
    while (reader.read(datum))
    {
    }
}

/// Reads the file with schema resolution from the writer schema of the file to `reader_field_type`.
void readAllDatumsResolving(const std::vector<uint8_t> & file, const std::string & reader_field_type)
{
    avro::ValidSchema reader_schema = avro::compileJsonSchemaFromString(makeRecordSchema(reader_field_type));
    avro::DataFileReader<avro::GenericDatum> reader(avro::memoryInputStream(file.data(), file.size()), reader_schema);
    avro::GenericDatum datum(reader.readerSchema());
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

/// The number of datums that items taking no bytes may create in a single datum, as set in `contrib/avro`.
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
/// What reading ended with.
enum class ReadOutcome
{
    Read,
    AvroException,
    MemoryLimitExceeded,
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

    void read() { avro::GenericReader::read(*decoder, datum); }

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
    /// Reads while the thread may hold at most `limit` bytes of what the read allocates, and records the most that it held.
    /// The datum of a union branch is a single large allocation, so a read that constructs the branch before it is charged to
    /// the budget ends with `MemoryLimitExceeded`, and one that charges it first ends with `AvroException` having not made it.
    ReadOutcome readWithMemoryLimit(Int64 limit)
    {
        MemoryLimit memory_limit(limit);
        ReadOutcome outcome = ReadOutcome::Read;
        try
        {
            read();
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
#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(THREAD_SANITIZER)
    /// Limits the memory of the thread until it is destroyed, in the way `gtest_reverse_index_exception_safety` does.
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

    void init(const avro::ValidSchema & datum_schema)
    {
        DB::MainThreadStatus::getInstance();
        in = avro::memoryInputStream(data.data(), data.size());
        decoder->init(*in);
        datum = avro::GenericDatum(datum_schema);
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
/// 1.5 MiB, below the datums of a branch of 70000 datums (more than 2 MiB), and above what a read that rejects such a branch
/// allocates, which is the exception and not the branch, so that constructing the branch exhausts it.
constexpr Int64 memory_limit_below_a_wide_branch = Int64(3) << 19;

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
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetInRecordFieldIsRejectedBeforeItIsConstructed)
{
    PreparedRead read(makeRecordSchema(wideUnion(branch_over_the_budget)), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAsArrayItemIsRejectedBeforeItIsConstructed)
{
    /// One item, which selects the second branch of its union.
    PreparedRead read(makeRecordSchema(R"({"type":"array","items":)" + wideUnion(branch_over_the_budget) + "}"), {0x02, 0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAsMapValueIsRejectedBeforeItIsConstructed)
{
    /// One entry with the key "k", whose value selects the second branch of its union.
    PreparedRead read(makeRecordSchema(R"({"type":"map","values":)" + wideUnion(branch_over_the_budget) + "}"), {0x02, 0x02, 'k', 0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, UnionBranchOverTheBudgetAtTheRootIsRejectedBeforeItIsConstructed)
{
    PreparedRead read(wideUnion(branch_over_the_budget), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, ResolvedUnionBranchOverTheBudgetIsRejectedBeforeItIsConstructed)
{
    /// The writer's empty record resolves to the second branch of the reader's union without any input,
    /// and every field of the branch is given a default.
    const std::string writer_type = R"({"type":"array","items":{"type":"record","name":"wide","fields":[]}})";
    const std::string reader_type = R"({"type":"array","items":["null",)" + recordOfNulls("wide", branch_over_the_budget, true) + "]}";
    PreparedRead read(writer_type, reader_type, {0x02, 0x00});
    EXPECT_EQ(read.readWithMemoryLimit(memory_limit_below_a_wide_branch), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, ResolvedUnionBranchThatDoesNotFitTheRemainingBudgetIsRejectedBeforeItIsConstructed)
{
    /// Two fields take no input, and each resolves the empty record of the writer to the second branch of its union. The
    /// branches have 12000 and 54000 datums: each fits the budget of 65536 alone, and the second does not fit what the
    /// first leaves. Only the second branch is much larger than what reading the first holds, so a read that constructs
    /// it before charging it holds the most, and one that charges it first never allocates it.
    constexpr size_t first_branch = 12000;
    constexpr size_t second_branch = 54000;
    const std::string first_writer_field = R"({"name":"f1","type":{"type":"record","name":"first","fields":[]}})";
    const std::string second_writer_field = R"({"name":"f2","type":{"type":"record","name":"second","fields":[]}})";
    const std::string first_reader_field = R"({"name":"f1","type":["null",)" + recordOfNulls("first", first_branch, true) + "]}";
    const std::string second_reader_field = R"({"name":"f2","type":["null",)" + recordOfNulls("second", second_branch, true) + "]}";
    auto record = [](const std::string & fields) { return R"({"type":"record","name":"r","fields":[)" + fields + "]}"; };

    /// What reading the first field alone holds. The second alone fits the budget.
    PreparedRead first_only(record(first_writer_field), record(first_reader_field), {0x00});
    EXPECT_EQ(first_only.readWithMemoryLimit(Int64(1) << 40), ReadOutcome::Read);
    EXPECT_NO_THROW(PreparedRead(record(second_writer_field), record(second_reader_field), {0x00}).read());

    /// Reading both holds what reading the first does and a quarter of the second branch more, if the second is not constructed.
    const Int64 limit = first_only.peakMemory() + static_cast<Int64>(sizeof(avro::GenericDatum) * second_branch / 4);
    PreparedRead both(record(first_writer_field + "," + second_writer_field), record(first_reader_field + "," + second_reader_field), {0x00});
    EXPECT_EQ(both.readWithMemoryLimit(limit), ReadOutcome::AvroException);
}

TEST(AvroGenericReader, UnionBranchInTheBudgetIsRead)
{
    /// A wide branch that fits the budget is not rejected, and does hold the memory that the rejected ones would.
    PreparedRead read(makeRecordSchema(wideUnion(branch_in_the_budget)), {0x02});
    EXPECT_EQ(read.readWithMemoryLimit(Int64(4) << 20), ReadOutcome::Read);

    PreparedRead over_the_limit(makeRecordSchema(wideUnion(branch_in_the_budget)), {0x02});
    EXPECT_EQ(over_the_limit.readWithMemoryLimit(Int64(1) << 20), ReadOutcome::MemoryLimitExceeded);
}

#endif

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
