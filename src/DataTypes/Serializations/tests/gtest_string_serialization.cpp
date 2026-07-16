#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/SerializationStringSize.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>

#include <gtest/gtest.h>

#include <cstring>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
        extern const int CANNOT_READ_ALL_DATA;
        extern const int TOO_LARGE_STRING_SIZE;
    }
}

using namespace DB;

TEST(StringSerialization, IncorrectStateAfterMemoryLimitExceeded)
{
    MainThreadStatus::getInstance();

    constexpr size_t rows = 1'000'000;

    WriteBufferFromOwnString out;

    auto src_column = ColumnString::create();
    src_column->insertMany("foobar", rows);

    auto type_string = std::make_shared<DataTypeString>();

    {
        auto serialization = type_string->getDefaultSerialization();
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = [&out](const auto &) { return &out; };
        serialization->serializeBinaryBulkWithMultipleStreams(*src_column, 0, src_column->size(), settings, state);
    }

    size_t memory_limit_exceeded_errors = 0;
    auto run_with_memory_failures = [&](auto cb)
    {
        total_memory_tracker.setFaultProbability(0.2);
        try
        {
            cb();
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;

            ++memory_limit_exceeded_errors;
            total_memory_tracker.setFaultProbability(0);
        }
        total_memory_tracker.setFaultProbability(0);
    };

    size_t non_empty_result = 0;
    while (memory_limit_exceeded_errors < 10 || non_empty_result < 10)
    {
        ColumnPtr result_column = type_string->createColumn();
        ReadBufferFromOwnString in(out.str());

        auto serialization = type_string->getDefaultSerialization();
        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = [&in](const auto &) { return &in; };

        run_with_memory_failures([&]() { serialization->deserializeBinaryBulkWithMultipleStreams(result_column, 0, src_column->size(), settings, state, nullptr); });

        /// A `MEMORY_LIMIT_EXCEEDED` thrown while deserializing may leave `result_column` null: the COW-safe
        /// deserialize path moves the column into a mutable clone and only assigns it back to `result_column`
        /// on success. That is acceptable — we only require that any column that does survive stays consistent.
        if (!result_column)
            continue;

        auto & result = assert_cast<ColumnString &>(*result_column->assumeMutable());
        if (!result.empty())
        {
            ++non_empty_result;
            ASSERT_EQ(result.getDataAt(0), "foobar");
            ASSERT_EQ(result.getDataAt(result.size() - 1), "foobar");
        }
    }
}

namespace
{

/// A column of varied-length strings, including some large ones so the data stream is non-trivial.
ColumnString::MutablePtr makeVariedStringColumn(size_t rows)
{
    auto column = ColumnString::create();
    for (size_t i = 0; i < rows; ++i)
    {
        std::string value(i % 37 == 0 ? 5000 + (i % 113) : (i % 11), static_cast<char>('a' + (i % 26)));
        column->insertData(value.data(), value.size());
    }
    return column;
}

/// Route the two WITH_SIZE_STREAM substreams (sizes vs char data) to separate buffers.
template <typename BufferPtr, typename SizesBuf, typename DataBuf>
auto makeSizeStreamGetter(SizesBuf & sizes_buffer, DataBuf & data_buffer)
{
    return [&sizes_buffer, &data_buffer](const ISerialization::SubstreamPath & path) -> BufferPtr
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::StringSizes)
            return &sizes_buffer;
        return &data_buffer;
    };
}

}

/// A faithful WITH_SIZE_STREAM round-trip (including a seeked read with rows_offset > 0) must keep the
/// reconstructed column internally consistent: offsets.back() == chars.size(). This establishes that the
/// inconsistency in the test below is caused by the streams disagreeing, not by normal operation.
TEST(StringSerialization, WithSizeStreamFaithfulRoundTripIsConsistent)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Read the whole column back, then a seeked subrange, and verify both are consistent.
    for (size_t rows_offset : {size_t{0}, size_t{123}})
    {
        ReadBufferFromString sizes_in(sizes_out.str());
        ReadBufferFromString data_in(data_out.str());

        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

        ColumnPtr result = ColumnString::create();
        serialization->deserializeBinaryBulkWithMultipleStreams(result, rows_offset, rows - rows_offset, settings, state, nullptr);

        const auto & result_string = assert_cast<const ColumnString &>(*result);
        ASSERT_EQ(result_string.getOffsets().back(), result_string.getChars().size());
        ASSERT_EQ(result_string.size(), rows - rows_offset);
        ASSERT_EQ(result_string.getDataAt(0), src->getDataAt(rows_offset));
    }
}

/// The producer of the corrupted column. When the data stream delivers fewer bytes than the sizes stream
/// claims (the two streams are stored separately and a seek/version/desync makes them disagree),
/// deserializeBinaryBulkWithSizeStream would previously commit offsets from the sizes stream while shrinking
/// chars to the short read, yielding offsets.back() > chars.size() with no error. That inconsistent column
/// then over-reads in ColumnString::insertRangeFrom during a merge (the observed CI/production crash).
/// On the data path the short read is caught by readBigStrict, which fails loudly with CANNOT_READ_ALL_DATA
/// at the point of deserialization instead of producing the inconsistent column.
TEST(StringSerialization, WithSizeStreamShortDataStreamThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Sizes intact, data stream truncated: the streams now disagree on the total byte count.
    std::string data_bytes = data_out.str();
    ASSERT_GT(data_bytes.size(), 64u);
    std::string truncated_data = data_bytes.substr(0, data_bytes.size() - 64);

    ReadBufferFromString sizes_in(sizes_out.str());
    ReadBufferFromString data_in(truncated_data);

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "deserialize accepted a short data stream and produced offsets.back()="
               << assert_cast<const ColumnString &>(*result).getOffsets().back()
               << " vs chars.size()=" << assert_cast<const ColumnString &>(*result).getChars().size();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }
}

/// The symmetric corruption: the SIZES stream (not the data stream) is the corrupt one. The two
/// substreams are stored separately, so a bad granule / version skew / seek can desync them with a
/// garbage per-row length. A single length with bit 63 set makes the accumulated offset (and hence
/// bytes_to_read) >= 2^63; the pre-read data.resize() then reaches Allocator::checkSize, which
/// throws a LOGICAL_ERROR ("Too large size ... passed to allocator") that aborts under
/// debug/sanitizer builds (the observed CI crash). A corrupt part must fail loudly at the point of
/// deserialization (TOO_LARGE_STRING_SIZE) rather than abort the server -- mirroring the bound the
/// single-stream path already enforces in deserializeBinaryImpl.
TEST(StringSerialization, WithSizeStreamCorruptSizeStreamThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// The sizes stream is a sequence of raw little-endian UInt64 lengths. Overwrite the first one
    /// with a value that has bit 63 set; the data stream is left intact.
    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), sizeof(UInt64));
    const UInt64 corrupt_size = 0x8000000000000000ULL | 1ULL;
    memcpy(sizes_bytes.data(), &corrupt_size, sizeof(UInt64));

    ReadBufferFromString sizes_in(sizes_bytes);
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "deserialize accepted a corrupt sizes stream and produced offsets.back()="
               << assert_cast<const ColumnString &>(*result).getOffsets().back()
               << " vs chars.size()=" << assert_cast<const ColumnString &>(*result).getChars().size();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// The same corruption, but the corrupt length lands in the SKIPPED PREFIX of a seeked read
/// (rows_offset > 0). deserializeBinaryBulkWithSizeStream first sums the skipped-prefix sizes into
/// `bytes_to_skip` before it validates the requested rows; without a bound there, a bit-63 size (or a
/// pair of them, whose sum wraps to 0 on 64-bit) would misalign the data stream and read the result
/// from the wrong byte position instead of throwing. The skipped-prefix sizes must be validated (and
/// accumulated with checked addition) with the same bound, so a corrupt part fails loudly here too.
TEST(StringSerialization, WithSizeStreamCorruptSkippedPrefixSizeThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    constexpr size_t rows_offset = 123;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Corrupt a size that falls inside the skipped prefix (index < rows_offset). Use two bit-63
    /// values so the naive skip sum would wrap to 0 rather than merely being large.
    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), 2 * sizeof(UInt64));
    const UInt64 corrupt_size = 0x8000000000000000ULL;
    memcpy(sizes_bytes.data(), &corrupt_size, sizeof(UInt64));
    memcpy(sizes_bytes.data() + sizeof(UInt64), &corrupt_size, sizeof(UInt64));

    ReadBufferFromString sizes_in(sizes_bytes);
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, rows_offset, rows - rows_offset, settings, state, nullptr);
        FAIL() << "deserialize accepted a corrupt size in the skipped prefix and produced offsets.back()="
               << assert_cast<const ColumnString &>(*result).getOffsets().back()
               << " vs chars.size()=" << assert_cast<const ColumnString &>(*result).getChars().size();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// A short/truncated `.size` sub-stream (fewer UInt64 lengths than requested). The size stream is
/// deserialized with `SerializationNumber<UInt64>::deserializeBinaryBulk`, which appends whatever it
/// finds and returns without demanding the full requested prefix. Before the fix, `num_read_rows <
/// rows_offset + limit` was not checked, so the read either walked past `sizes_data` (when the
/// skipped prefix exceeded what was read) or silently produced fewer rows than requested. A corrupt
/// part must fail loudly, symmetric to the short data-stream path (CANNOT_READ_ALL_DATA).
TEST(StringSerialization, WithSizeStreamShortSizeStreamThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Keep only the first 100 UInt64 lengths; the data stream is left intact. The reader requests
    /// all `rows` from offset 0, so `num_read_rows` (100) is short of the requested prefix.
    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), 100 * sizeof(UInt64));
    std::string truncated_sizes = sizes_bytes.substr(0, 100 * sizeof(UInt64));

    ReadBufferFromString sizes_in(truncated_sizes);
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "deserialize accepted a short size stream and produced "
               << assert_cast<const ColumnString &>(*result).size() << " rows for " << rows << " requested";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }
}

/// A corrupt per-row size that lands in the MIDDLE of the requested (appended) range, not at index 0
/// and not in the skipped prefix. Trace analogue: sizes `[good, ..., 0x8000...1, ...]`. Before the
/// fix, `deserializeBinaryBulkWithSizeStream` appended the good leading offsets into the column and
/// only threw when it reached the corrupt one, leaving the caller with `offsets.back() > chars.size()`
/// (the override mutates `column` directly, so there is no clone-and-assign rollback). The whole size
/// slice is now validated before any offset is pushed or `data.resize()` runs, so the throw path
/// leaves the column internally consistent.
TEST(StringSerialization, WithSizeStreamMidRangeCorruptSizeLeavesColumnConsistent)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    constexpr size_t corrupt_index = 250;
    auto src = makeVariedStringColumn(rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Corrupt a size in the middle of the requested range (rows_offset == 0). The leading rows are
    /// all valid, so a non-atomic implementation would have committed their offsets before throwing.
    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), (corrupt_index + 1) * sizeof(UInt64));
    const UInt64 corrupt_size = 0x8000000000000000ULL | 1ULL;
    memcpy(sizes_bytes.data() + corrupt_index * sizeof(UInt64), &corrupt_size, sizeof(UInt64));

    ReadBufferFromString sizes_in(sizes_bytes);
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "deserialize accepted a mid-range corrupt size and produced offsets.back()="
               << assert_cast<const ColumnString &>(*result).getOffsets().back()
               << " vs chars.size()=" << assert_cast<const ColumnString &>(*result).getChars().size();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_LARGE_STRING_SIZE);
    }

    /// The key assertion: whatever the caller is left holding must be internally consistent -- no
    /// offsets committed without matching chars.
    if (result)
    {
        const auto & result_string = assert_cast<const ColumnString &>(*result);
        const IColumn::Offset last_offset = result_string.getOffsets().empty() ? 0 : result_string.getOffsets().back();
        ASSERT_LE(last_offset, result_string.getChars().size());
    }
}

/// The `s.size` subcolumn is read by SerializationStringSize::deserializeBinaryBulkWithSizeStream, a
/// SEPARATE reader of the same `.size` substream. It must reject a short/truncated size stream the same
/// way the String path does: before the fix, `num_read_rows < rows_offset + limit` reached the
/// compaction step, where `actual_new_size = size() - rows_offset` underflows (with rows_offset >
/// num_read_rows) and the loop walks past the end of `data`; with rows_offset == 0 it silently returns
/// too few `s.size` rows.
TEST(StringSerialization, StringSizeSubcolumnShortSizeStreamThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    auto src = makeVariedStringColumn(rows);

    auto string_serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        string_serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// Keep only the first 100 UInt64 lengths. The `s.size` reader requests all `rows` from offset 0.
    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), 100 * sizeof(UInt64));
    std::string truncated_sizes = sizes_bytes.substr(0, 100 * sizeof(UInt64));

    ReadBufferFromString sizes_in(truncated_sizes);
    ReadBufferFromString data_in(data_out.str());

    auto size_serialization = SerializationStringSize::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    size_serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnUInt64::create();
    try
    {
        size_serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "s.size deserialize accepted a short size stream and produced "
               << assert_cast<const ColumnUInt64 &>(*result).size() << " rows for " << rows << " requested";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }
}

/// An oversized per-row size in the `.size` substream must fail with the same corruption exception on
/// the `s.size` subcolumn path instead of being exposed as subcolumn data.
TEST(StringSerialization, StringSizeSubcolumnOversizeThrows)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    constexpr size_t corrupt_index = 250;
    auto src = makeVariedStringColumn(rows);

    auto string_serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        string_serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    std::string sizes_bytes = sizes_out.str();
    ASSERT_GE(sizes_bytes.size(), (corrupt_index + 1) * sizeof(UInt64));
    const UInt64 corrupt_size = 0x8000000000000000ULL | 1ULL;
    memcpy(sizes_bytes.data() + corrupt_index * sizeof(UInt64), &corrupt_size, sizeof(UInt64));

    ReadBufferFromString sizes_in(sizes_bytes);
    ReadBufferFromString data_in(data_out.str());

    auto size_serialization = SerializationStringSize::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    size_serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnUInt64::create();
    try
    {
        size_serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        FAIL() << "s.size deserialize exposed an oversized `.size` entry as subcolumn data";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

/// The WITH_SIZE_STREAM analogue of IncorrectStateAfterMemoryLimitExceeded. After the size slice passes
/// validation, the override still appends offsets and then runs fallible in-place ops (data.resize,
/// ignore, readBigStrict) on the caller-owned column. A MEMORY_LIMIT_EXCEEDED fault injected at
/// data.resize (assumeMutable does not clone) would leave the committed offsets with the old chars
/// buffer, so offsets.back() > chars.size() reappears on the exception path. The append/read block is
/// now rolled back on any throw, so any column the caller is left holding stays internally consistent.
TEST(StringSerialization, WithSizeStreamConsistentAfterMemoryLimitExceeded)
{
    MainThreadStatus::getInstance();
    /// Large enough that the offsets/chars allocations are reliably tracked and faulted (small columns
    /// produce too few tracked allocations to ever inject a fault).
    constexpr size_t rows = 1'000'000;
    auto src = ColumnString::create();
    src->insertMany("foobar", rows);

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString data_out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<WriteBuffer *>(sizes_out, data_out);
        serialization->serializeBinaryBulkWithMultipleStreams(*src, 0, src->size(), settings, state);
    }

    /// On every iteration a MEMORY_LIMIT_EXCEEDED may be injected at any tracked allocation (offsets
    /// reserve / data.resize). Whatever the caller is left holding must stay internally consistent
    /// regardless of where the throw landed. Accumulate both a throw path and a clean-success path.
    size_t memory_limit_exceeded_errors = 0;
    size_t consistent_success = 0;
    while (memory_limit_exceeded_errors < 10 || consistent_success < 10)
    {
        ReadBufferFromString sizes_in(sizes_out.str());
        ReadBufferFromString data_in(data_out.str());

        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

        ColumnPtr result = ColumnString::create();

        bool threw = false;
        total_memory_tracker.setFaultProbability(0.2);
        try
        {
            serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, rows, settings, state, nullptr);
        }
        catch (Exception & e)
        {
            total_memory_tracker.setFaultProbability(0);
            if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;
            ++memory_limit_exceeded_errors;
            threw = true;
        }
        total_memory_tracker.setFaultProbability(0);

        /// The key assertion: no offsets committed without matching chars, on both the throw and the
        /// success path.
        if (result)
        {
            const auto & result_string = assert_cast<const ColumnString &>(*result);
            const IColumn::Offset last_offset = result_string.getOffsets().empty() ? 0 : result_string.getOffsets().back();
            ASSERT_LE(last_offset, result_string.getChars().size());
            if (!threw && !result_string.empty() && last_offset == result_string.getChars().size())
                ++consistent_success;
        }
    }

    /// The test only exercises the exception path if faults were actually injected; the successful runs
    /// confirm the rollback did not corrupt the normal path.
    ASSERT_GT(memory_limit_exceeded_errors, 0u);
    ASSERT_GT(consistent_success, 0u);
}
