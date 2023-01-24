#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <IO/ReadBuffer.h>
#include <Storages/FileLog/StorageFileLog.h>

#include <fstream>
#include <mutex>

namespace Poco
{
    class Logger;
}

namespace DB
{
class ReadBufferFromFileLog : public ReadBuffer
{
public:
    ReadBufferFromFileLog(
        StorageFileLog & storage_,
        size_t max_batch_size,
        size_t poll_timeout_,
        ContextPtr context_,
        size_t stream_number_,
        size_t max_streams_number_);

    ~ReadBufferFromFileLog() override = default;

    auto pollTimeout() const { return poll_timeout; }

    bool hasMorePolledRecords() const { return current != records.end(); }

    bool poll();

    bool noRecords() { return buffer_status == BufferStatus::NO_RECORD_RETURNED; }

    auto getFileName() const { return current_file; }
    auto getOffset() const { return current_offset; }

private:
    enum class BufferStatus
    {
        INIT,
        NO_RECORD_RETURNED,
        POLLED_OK,
    };

    BufferStatus buffer_status = BufferStatus::INIT;

    Poco::Logger * log;

    StorageFileLog & storage;

    bool stream_out = false;

    size_t batch_size;
    size_t poll_timeout;

    ContextPtr context;

    size_t stream_number;
    size_t max_streams_number;

    bool allowed = true;

    using RecordData = std::string;
    struct Record
    {
        RecordData data;
        std::string file_name;
        /// Offset is the start of a row, which is needed for virtual columns.
        UInt64 offset;
    };
    using Records = std::vector<Record>;

    Records records;
    Records::const_iterator current;

    String current_file;
    UInt64 current_offset = 0;

    using TaskThread = BackgroundSchedulePool::TaskHolder;

    Records pollBatch(size_t batch_size_);

    void readNewRecords(Records & new_records, size_t batch_size_);

    bool nextImpl() override;
};
}
