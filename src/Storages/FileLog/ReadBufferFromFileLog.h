#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <common/types.h>

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

private:
    enum class BufferStatus
    {
        NO_RECORD_RETURNED,
        POLLED_OK,
    };

    BufferStatus buffer_status;

    Poco::Logger * log;

    StorageFileLog & storage;

    size_t batch_size;
    size_t poll_timeout;

    ContextPtr context;

    size_t stream_number;
    size_t max_streams_number;

    bool allowed = true;

    using Record = std::string;
    using Records = std::vector<Record>;

    Records records;
    Records::const_iterator current;

    using TaskThread = BackgroundSchedulePool::TaskHolder;

    TaskThread wait_task;

    Records pollBatch(size_t batch_size_);

    void readNewRecords(Records & new_records, size_t batch_size_);

    void cleanUnprocessed();

    bool nextImpl() override;
};
}
