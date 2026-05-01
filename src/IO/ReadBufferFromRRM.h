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
        ThreadGroupPtr thread_group_,
        ReadScopePtr scope_ = nullptr);

    ~ReadBufferFromRRM() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    String getFileName() const override { return object_key; }

    std::optional<size_t> tryGetFileSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

    bool supportsRightBoundedReads() const override { return true; }

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

private:
    bool nextImpl() override;

    /// Block until prefetch completes, set up `internal_buffer` / `working_buffer`.
    void waitPrefetch();

    /// Check that an absolute seek position falls within one of the
    /// reading_ranges (extended by cache_pre_padding_bytes).
    /// Logs a warning if the position doesn't match any range.
    void validateSeekPosition(size_t absolute_pos) const;

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

    /// Scope with reading_ranges and cache_pre_padding_bytes for seek validation.
    ReadScopePtr scope;

    /// Upper bound set by the cache layer via `setReadUntilPosition`.
    /// `nextImpl` will not serve data at or beyond this absolute offset.
    std::optional<size_t> read_until_position;

    LoggerPtr log = getLogger("ReadBufferFromRRM");
};

}
