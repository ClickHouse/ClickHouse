#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>
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
