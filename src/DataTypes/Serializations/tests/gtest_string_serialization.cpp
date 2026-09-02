#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>

#include <gtest/gtest.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int MEMORY_LIMIT_EXCEEDED;
        extern const int CANNOT_READ_ALL_DATA;
        extern const int INCORRECT_DATA;
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
        auto result_column = type_string->createColumn();
        ReadBufferFromOwnString in(out.str());

        auto serialization = type_string->getDefaultSerialization();
        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = [&in](const auto &) { return &in; };

        run_with_memory_failures([&]() { serialization->deserializeBinaryBulkWithMultipleStreams(*result_column, src_column->size(), settings, state, nullptr); });

        auto & result = assert_cast<ColumnString &>(*result_column);
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

/// A faithful WITH_SIZE_STREAM round-trip must keep the reconstructed column internally consistent:
/// offsets.back() == chars.size(). This establishes that the inconsistency in the test below is caused
/// by the streams disagreeing, not by normal operation.
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

    ReadBufferFromString sizes_in(sizes_out.str());
    ReadBufferFromString data_in(data_out.str());

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto result = ColumnString::create();
    serialization->deserializeBinaryBulkWithMultipleStreams(*result, rows, settings, state, nullptr);

    const auto & result_string = assert_cast<const ColumnString &>(*result);
    ASSERT_EQ(result_string.getOffsets().back(), result_string.getChars().size());
    ASSERT_EQ(result_string.size(), rows);
    ASSERT_EQ(result_string.getDataAt(0), src->getDataAt(0));
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

    auto result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(*result, rows, settings, state, nullptr);
        FAIL() << "deserialize accepted a short data stream and produced offsets.back()="
               << result->getOffsets().back()
               << " vs chars.size()=" << result->getChars().size();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }
}

namespace
{

/// A deserialization attempt over a hand-crafted (corrupted) sizes stream. The values in the sizes stream
/// come straight from the data, so nothing bounds them implicitly; the deserialization has to reject the
/// ones that would overflow the offsets instead of wrapping around.
void expectSizesStreamRejected(const std::vector<UInt64> & sizes_values, size_t limit)
{
    WriteBufferFromOwnString sizes_out;
    for (UInt64 size : sizes_values)
        writeBinaryLittleEndian(size, sizes_out);

    /// The corrupted size must be rejected before any data is read, so the content of the data stream is irrelevant.
    ReadBufferFromString sizes_in(sizes_out.str());
    ReadBufferFromString data_in(std::string(64, 'x'));

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = true;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(*result, limit, settings, state, nullptr);
        FAIL() << "deserialize accepted a corrupted sizes stream";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
    }
}

}

/// The sizes in the sizes stream come straight from the data, and accumulating them into the offsets
/// of the column can overflow: sizes close to 2^64 wrap the offsets around and the spans computed from
/// them then point outside the data.
TEST(StringSerialization, WithSizeStreamHugeSizeIsRejected)
{
    MainThreadStatus::getInstance();
    expectSizesStreamRejected({10, std::numeric_limits<UInt64>::max(), 5}, 3);
    /// A sum that is exactly 2^65 and therefore wraps to zero when accumulated in 64 bits.
    expectSizesStreamRejected({std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), 2}, 3);
    expectSizesStreamRejected({(1ULL << 48) + 1}, 1);
}

namespace
{

/// A deserialization attempt over a hand-crafted (corrupted) stream of cumulative offsets, which is what
/// the size stream carries over the network (`position_independent_encoding = false`) instead of the
/// per-row sizes. The offsets come straight from the data as well.
void expectOffsetsStreamRejected(const std::vector<UInt64> & offset_values, size_t limit, int expected_error_code)
{
    WriteBufferFromOwnString offsets_out;
    for (UInt64 offset : offset_values)
        writeBinaryLittleEndian(offset, offsets_out);

    /// The corrupted offset must be rejected before any data is read, so the content of the data stream is irrelevant.
    ReadBufferFromString offsets_in(offsets_out.str());
    ReadBufferFromString data_in(std::string(64, 'x'));

    auto serialization = SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = false;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(offsets_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    auto result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(*result, limit, settings, state, nullptr);
        FAIL() << "deserialize accepted a corrupted stream of offsets";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), expected_error_code);
    }
}

}

/// The stream of cumulative offsets is the `Native` carrier of the string sizes. The offsets address the
/// characters of the column, so they have to increase monotonically and to stay within the limit on the
/// size of the whole column.
TEST(StringSerialization, OffsetsStreamHugeSizeIsRejected)
{
    MainThreadStatus::getInstance();

    expectOffsetsStreamRejected({10, 5}, 2, ErrorCodes::INCORRECT_DATA);
    expectOffsetsStreamRejected({std::numeric_limits<UInt64>::max()}, 1, ErrorCodes::INCORRECT_DATA);
    expectOffsetsStreamRejected({10, (1ULL << 48) + 11}, 2, ErrorCodes::INCORRECT_DATA);
}
