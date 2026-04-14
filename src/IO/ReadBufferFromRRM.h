#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/RemoteReadingManager.h>
#include <Common/logger_useful.h>

#include <future>


namespace DB
{

/// Read buffer backed by RRM prefetch.
///
/// On construction, kicks off an async fetch of the required byte range
/// using the provided `RemoteConnectionFactory`. The first `nextImpl` call
/// blocks until the data arrives (or returns immediately if the prefetch
/// already completed). After that, all reads and seeks operate over the
/// in-memory buffer — no further I/O.
class ReadBufferFromRRM : public ReadBufferFromFileBase
{
public:
    ReadBufferFromRRM(
        String object_key_,
        size_t range_begin_,
        size_t range_size_,
        std::future<Memory<>> prefetch_future_,
        ThreadGroupPtr thread_group_);

    ~ReadBufferFromRRM() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    String getFileName() const override { return object_key; }

    std::optional<size_t> tryGetFileSize() override { return range_size; }

    size_t getFileOffsetOfBufferEnd() const override { return range_begin + data_offset; }

    bool supportsRightBoundedReads() const override { return true; }

private:
    bool nextImpl() override;

    /// Block until prefetch completes, set up `internal_buffer` / `working_buffer`.
    void waitPrefetch();

    String object_key;
    size_t range_begin;  /// Absolute offset in the remote object.
    size_t range_size;

    std::future<Memory<>> prefetch_future;
    Memory<> prefetched_data;
    bool prefetch_consumed = false;

    /// Current logical offset within `prefetched_data`.
    size_t data_offset = 0;

    /// Query's thread group — attached when the buffer does work,
    /// so that log messages and metrics are attributed to the right query.
    ThreadGroupPtr thread_group;

    LoggerPtr log = getLogger("ReadBufferFromRRM");
};

}
