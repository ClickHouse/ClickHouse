#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/AsynchronousReader.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Common/IThrottler.h>
#include <Common/Priority.h>

#include <optional>
#include <unistd.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class AsynchronousReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    IAsynchronousReader & reader;
    Priority base_priority;

    /// Buffer to prefetch into. Note that internal_buffer is initialized in constructor and never
    /// changed; working_buffer doesn't necessarily point into internal_buffer.
    /// The 3 buffers (internal_buffer, memory, prefetch_buffer) all have equal length when not empty.
    /// How buffers are used:
    ///  * If prefetching is used (prefetch() called), we swap buffers `prefetch` and `memory`
    ///    after each prefetched. `existing_memory` passed to constructor ends up ignored.
    ///  * If prefetching is not used (prefetch() never called), we read synchronously into
    ///    `internal_buffer`, which points either into `memory` or to `existing_memory`.
    ///  * If prefetch() is only sometimes called, the non-prefetched synchronous reads use `memory`
    ///    and ignore `existing_memory`.
    Memory<> prefetch_buffer;
    std::future<IAsynchronousReader::Result> prefetch_future;

    const size_t required_alignment = 0;  /// For O_DIRECT both file offsets and memory addresses have to be aligned.
    size_t file_offset_of_buffer_end = 0; /// What offset in file corresponds to working_buffer.end().
    size_t bytes_to_ignore = 0;           /// How many bytes should we ignore upon a new read request.
    int fd;
    bool direct_io;
    ThrottlerPtr throttler;

    bool nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

    void finalize();

public:
    AsynchronousReadBufferFromFileDescriptor(
        IAsynchronousReader & reader_,
        Priority priority_,
        int fd_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler_ = {},
        FilesystemReadPrefetchesLogPtr prefetches_log_ = nullptr);

    ~AsynchronousReadBufferFromFileDescriptor() override;

    void prefetch(Priority priority) override;

    int getFD() const
    {
        return fd;
    }

    off_t getPosition() override
    {
        return file_offset_of_buffer_end - (working_buffer.end() - pos);
    }

    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t seek(off_t off, int whence) override;

    /// Seek to the beginning, discarding already read data if any. Useful to reread file that changes on every read.
    void rewind();

    std::optional<size_t> tryGetFileSize() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

private:
    std::future<IAsynchronousReader::Result> asyncReadInto(char * data, size_t size, Priority priority);

    const std::string query_id;
    const std::string current_reader_id;

    struct LastPrefetchInfo
    {
        std::chrono::system_clock::time_point submit_time;
        Priority priority;
    };
    LastPrefetchInfo last_prefetch_info;

    FilesystemReadPrefetchesLogPtr prefetches_log;

    void appendToPrefetchLog(FilesystemPrefetchState state, int64_t size, const std::unique_ptr<Stopwatch> & execution_watch);
};

}
