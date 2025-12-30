#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <IO/ReadBuffer.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/TrackedString.h>

namespace DB
{
class FileLogConsumer
{
public:
    FileLogConsumer(
        StorageFileLog & storage_,
        size_t max_batch_size,
        size_t poll_timeout_,
        ContextPtr context_,
        size_t stream_number_,
        size_t max_streams_number_);

    auto pollTimeout() const { return poll_timeout; }

    bool hasMorePolledRecords() const { return current != records.end(); }

    ReadBufferPtr consume();

    bool noRecords() { return buffer_status == BufferStatus::NO_RECORD_RETURNED; }

    auto getFileName() const { return current[-1].file_name; }
    auto getOffset() const { return current[-1].offset; }
    const auto & getCurrentRecord() const { return current[-1].data; }

private:
    enum class BufferStatus : uint8_t
    {
        INIT,
        NO_RECORD_RETURNED,
        POLLED_OK,
    };

    BufferStatus buffer_status = BufferStatus::INIT;

    LoggerPtr log;

    StorageFileLog & storage;

    bool stream_out = false;

    size_t batch_size;
    size_t poll_timeout;

    ContextPtr context;

    size_t stream_number;
    size_t max_streams_number;

    struct Record
    {
        TrackedString data;
        std::string file_name;
        /// Offset is the start of a row, which is needed for virtual columns.
        UInt64 offset;
    };
    using Records = std::vector<Record, AllocatorWithMemoryTracking<Record>>;

    Records records;
    Records::const_iterator current;

    using TaskThread = BackgroundSchedulePoolTaskHolder;

    Records pollBatch(size_t batch_size_);

    void readNewRecords(Records & new_records, size_t batch_size_);

    ReadBufferPtr getNextRecord();
};
}
