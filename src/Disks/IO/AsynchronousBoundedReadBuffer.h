#pragma once

#include <chrono>
#include <utility>
#include <IO/AsynchronousReader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include "config.h"

namespace Poco { class Logger; }

namespace DB
{

struct AsyncReadCounters;
using AsyncReadCountersPtr = std::shared_ptr<AsyncReadCounters>;
class ReadBufferFromRemoteFSGather;

class AsynchronousBoundedReadBuffer : public ReadBufferFromFileBase
{
public:
    using Impl = ReadBufferFromFileBase;
    using ImplPtr = std::unique_ptr<Impl>;

    explicit AsynchronousBoundedReadBuffer(
        ImplPtr impl_,
        IAsynchronousReader & reader_,
        const ReadSettings & settings_,
        AsyncReadCountersPtr async_read_counters_ = nullptr,
        FilesystemReadPrefetchesLogPtr prefetches_log_ = nullptr);

    ~AsynchronousBoundedReadBuffer() override;

    String getFileName() const override { return impl->getFileName(); }

    std::optional<size_t> tryGetFileSize() override { return impl->tryGetFileSize(); }

    String getInfoForLog() override { return impl->getInfoForLog(); }

    off_t seek(off_t offset_, int whence) override;

    void prefetch(Priority priority) override;

    void setReadUntilPosition(size_t position) override; /// [..., position).

    void setReadUntilEnd() override { setReadUntilPosition(getFileSize()); }

    size_t getFileOffsetOfBufferEnd() const override  { return file_offset_of_buffer_end; }

    off_t getPosition() override { return file_offset_of_buffer_end - available() + bytes_to_ignore; }

private:
    const ImplPtr impl;
    const ReadSettings read_settings;
    IAsynchronousReader & reader;

    size_t file_offset_of_buffer_end = 0;
    std::optional<size_t> read_until_position;
    /// If nonzero then working_buffer is empty.
    /// If a prefetch is in flight, the prefetch task has been instructed to ignore this many bytes.
    size_t bytes_to_ignore = 0;

    Memory<> prefetch_buffer;
    std::future<IAsynchronousReader::Result> prefetch_future;

    const std::string query_id;
    const std::string current_reader_id;

    LoggerPtr log;

    AsyncReadCountersPtr async_read_counters;
    FilesystemReadPrefetchesLogPtr prefetches_log;

    struct LastPrefetchInfo
    {
        std::chrono::system_clock::time_point submit_time;
        Priority priority;
    };
    LastPrefetchInfo last_prefetch_info;

    bool nextImpl() override;

    void finalize();

    bool hasPendingDataToRead();

    void appendToPrefetchLog(
        FilesystemPrefetchState state,
        int64_t size,
        const std::unique_ptr<Stopwatch> & execution_watch);

    std::future<IAsynchronousReader::Result> readAsync(char * data, size_t size, Priority priority);

    IAsynchronousReader::Result readSync(char * data, size_t size);

    void resetPrefetch(FilesystemPrefetchState state);
};

}
