#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <common/types.h>

#include <fstream>
#include <mutex>
#include <unordered_map>

namespace Poco
{
    class Logger;
}

namespace DB
{
class ReadBufferFromFileLog : public ReadBuffer
{
public:
    ReadBufferFromFileLog(const String & path_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_, ContextPtr context_);

    ~ReadBufferFromFileLog() override = default;

    void open();
    void close(); 

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledRecords() const { return current != records.end(); }

    bool poll();

private:
    enum class FileStatus
    {
        BEGIN,
        NO_CHANGE,
        UPDATED,
        REMOVED
    };

    struct FileContext
    {
        FileStatus status = FileStatus::BEGIN;
        std::ifstream reader;
    };

    const String path;

    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;

    bool time_out = false;

    using NameToFile = std::unordered_map<String, FileContext>;
    NameToFile file_status;

    std::mutex status_mutex;

    ContextPtr context;

    bool allowed = true;

    using Record = std::string;
    using Records = std::vector<Record>;

    Records records;
    Records::const_iterator current;

    using TaskThread = BackgroundSchedulePool::TaskHolder;

    TaskThread wait_task;
    TaskThread select_task;

    Records pollBatch(size_t batch_size_);

    void readNewRecords(Records & new_records, size_t batch_size_);

    void cleanUnprocessed();

    bool nextImpl() override;

    void waitFunc();

    [[noreturn ]] void watchFunc(FileLogDirectoryWatcher & dw);
};
}
