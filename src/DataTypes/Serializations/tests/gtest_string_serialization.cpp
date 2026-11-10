#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
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

    {
        auto serialization = std::make_shared<SerializationString>();
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

    auto type_string = std::make_shared<DataTypeString>();
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

/// A deserialization attempt over a hand-crafted (corrupted) sizes stream. The values in the sizes stream
/// come straight from the data, so nothing bounds them implicitly; the deserialization has to reject the
/// ones that would overflow the offsets instead of wrapping around.
void expectSizesStreamRejected(const std::vector<UInt64> & sizes_values, size_t rows_offset, size_t limit)
{
    WriteBufferFromOwnString sizes_out;
    for (UInt64 size : sizes_values)
        writeBinaryLittleEndian(size, sizes_out);

    /// The corrupted size must be rejected before any data is read, so the content of the data stream is irrelevant.
    ReadBufferFromString sizes_in(sizes_out.str());
    ReadBufferFromString data_in(std::string(64, 'x'));

    auto serialization = std::make_shared<SerializationString>(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);

    ISerialization::DeserializeBinaryBulkSettings settings;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = true;
    settings.getter = makeSizeStreamGetter<ReadBuffer *>(sizes_in, data_in);
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

    ColumnPtr result = ColumnString::create();
    try
    {
        serialization->deserializeBinaryBulkWithMultipleStreams(result, rows_offset, limit, settings, state, nullptr);
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
    expectSizesStreamRejected({10, std::numeric_limits<UInt64>::max(), 5}, 0, 3);
    /// A sum that is exactly 2^65 and therefore wraps to zero when accumulated in 64 bits.
    expectSizesStreamRejected({std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), 2}, 0, 3);
    expectSizesStreamRejected({(1ULL << 48) + 1}, 0, 1);
}

/// The sizes of the rows skipped by a seeked read (`rows_offset`) come from the same stream and are
/// summed up to know how many bytes of the data stream to skip; that sum has to be validated as well.
TEST(StringSerialization, WithSizeStreamHugeSkippedSizeIsRejected)
{
    MainThreadStatus::getInstance();
    expectSizesStreamRejected({std::numeric_limits<UInt64>::max(), 10, 5}, 1, 2);
    expectSizesStreamRejected({(1ULL << 48) + 1, 10, 5}, 1, 2);
}
