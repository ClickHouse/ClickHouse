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

namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
        extern const int CANNOT_READ_ALL_DATA;
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

/// A WITH_SIZE_STREAM read with rows_offset > 0 that goes through a substreams cache must cache the data
/// column under its true row growth (num_read_rows - rows_offset), not the full num_read_rows: the skipped
/// rows_offset rows are only ignored in the data stream, never inserted into the column. When the same range
/// is later served from the cache (insertDataFromCachedColumn takes the last num_read_rows rows off the
/// column's tail), an over-count underflows `cached_column->size() - num_read_rows` and drives insertRangeFrom
/// out of bounds. This is the release-active bug fixed in this change; the faithful round-trip above passes a
/// null cache, so it never fills or reuses a cache entry and cannot regress it.
/// See https://github.com/ClickHouse/ClickHouse/issues/105626.
TEST(StringSerialization, WithSizeStreamRowsOffsetSubstreamsCacheReuse)
{
    MainThreadStatus::getInstance();
    constexpr size_t rows = 500;
    constexpr size_t rows_offset = 123;
    constexpr size_t limit = rows - rows_offset;
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

    ReadBufferFromString sizes_in(sizes_out.str());
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    /// First, read a seeked subrange (rows_offset > 0) through a real substreams cache. This fills the cache
    /// entry for the `Regular` (data) substream. With the bug it would be stored under a row count of
    /// rows_offset + limit while the column only holds `limit` rows (the chassert in
    /// addColumnWithNumReadRowsToSubstreamsCache catches this directly in debug builds).
    ISerialization::SubstreamsCache cache;
    ColumnPtr first = ColumnString::create();
    serialization->deserializeBinaryBulkWithMultipleStreams(first, rows_offset, limit, settings, state, &cache);

    const auto & first_string = assert_cast<const ColumnString &>(*first);
    ASSERT_EQ(first_string.size(), limit);
    ASSERT_EQ(first_string.getOffsets().back(), first_string.getChars().size());
    ASSERT_EQ(first_string.getDataAt(0), src->getDataAt(rows_offset));

    /// Now serve the same range from the cache while forcing the "insert into the result column" path
    /// (insert_only_rows_in_current_range_from_substreams_cache), which inserts exactly num_read_rows rows
    /// from the tail of the cached column. An over-counted cache entry reads out of bounds here (release), so
    /// this second lookup is what makes the bug observable even where the chassert above is compiled out.
    settings.insert_only_rows_in_current_range_from_substreams_cache = true;
    ColumnPtr second = ColumnString::create();
    serialization->deserializeBinaryBulkWithMultipleStreams(second, rows_offset, limit, settings, state, &cache);

    const auto & second_string = assert_cast<const ColumnString &>(*second);
    ASSERT_EQ(second_string.size(), limit);
    ASSERT_EQ(second_string.getOffsets().back(), second_string.getChars().size());
    for (size_t i = 0; i < limit; ++i)
        ASSERT_EQ(second_string.getDataAt(i), src->getDataAt(rows_offset + i));
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
