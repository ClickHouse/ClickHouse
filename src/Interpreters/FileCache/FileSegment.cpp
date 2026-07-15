#include <Interpreters/FileCache/FileSegment.h>

#include <filesystem>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <base/EnumReflection.h>
#include <base/getThreadId.h>
#include <base/hex.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FileSegmentWaitMicroseconds;
    extern const Event FileSegmentCompleteMicroseconds;
    extern const Event FileSegmentLockMicroseconds;
    extern const Event FileSegmentWriteMicroseconds;
    extern const Event FileSegmentIncreasePriorityMicroseconds;
    extern const Event FileSegmentHolderCompleteMicroseconds;
    extern const Event FileSegmentFailToIncreasePriority;
    extern const Event FilesystemCacheHoldFileSegments;
    extern const Event FilesystemCacheUnusedHoldFileSegments;
    extern const Event FilesystemCacheBackgroundDownloadQueuePush;
}

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheHoldFileSegments;
    extern const Metric CacheFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char cache_filesystem_failure[];
}

String toString(FileSegmentKind kind)
{
    return String(magic_enum::enum_name(kind));
}

FileSegment::FileSegment(
        const Key & key_,
        size_t offset_,
        size_t size_,
        State download_state_,
        const CreateFileSegmentSettings & settings,
        bool background_download_enabled_,
        FileCache * cache_,
        std::weak_ptr<KeyMetadata> key_metadata_,
        Priority::IteratorPtr queue_iterator_)
    : file_key(key_)
    , segment_range(offset_, offset_ + size_ - 1)
    , segment_kind(settings.kind)
    , is_unbound(settings.unbounded)
    , background_download_enabled(background_download_enabled_)
    , download_state(download_state_)
    , key_metadata(key_metadata_)
    , queue_iterator(queue_iterator_)
    , cache(cache_)
#ifdef DEBUG_OR_SANITIZER_BUILD
    , log(getLogger(fmt::format("FileSegment({}) : {}", key_.toString(), range().toString())))
#endif
{
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (download_state)
    {
        /// EMPTY is used when file segment is not in cache and
        /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
        case (State::EMPTY):
        {
            chassert(key_metadata.lock());
            break;
        }
        /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
        case (State::DOWNLOADED):
        {
            reserved_size = downloaded_size = size_;
            chassert(fs::file_size(getPath()) == size_);
            chassert(queue_iterator);
            chassert(key_metadata.lock());
            break;
        }
        case (State::DETACHED):
        {
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can only create file segment with either EMPTY, DOWNLOADED or DETACHED state");
        }
    }

    CurrentMetrics::add(CurrentMetrics::CacheFileSegments);
}

FileSegment::Range::Range(size_t left_, size_t right_) : left(left_), right(right_)
{
    if (left > right)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to create incorrect range: [{}, {}]", left, right);
}

const LoggerPtr & FileSegment::getLog() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    return log;
#else
    static const LoggerPtr log = getLogger("FileSegment");
    return log;
#endif
}

FileSegment::State FileSegment::state() const
{
    /// Read without lock. This is safe because every terminal state is published as the last write
    /// of its transition: in particular DOWNLOADED is set only after the segment is fully
    /// finalized (writer flushed and closed, reader released, range/size settled - see
    /// `setDownloadedUnlocked` and `shrinkFileSegmentToDownloadedSize`). So an observer of a state
    /// here is guaranteed to also see all the state that belongs to it.
    return download_state.load();
}

String FileSegment::getPath() const
{
    return getKeyMetadata()->getFileSegmentPath(*this);
}

String FileSegment::tryGetPath() const
{
    auto metadata = tryGetKeyMetadata();
    if (!metadata)
        return "";
    return metadata->getFileSegmentPath(*this);
}

FileSegmentGuard::Lock FileSegment::lock() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentLockMicroseconds);
    return segment_guard.lock();
}

void FileSegment::setDownloadState(State state, const FileSegmentGuard::Lock & lock)
{
    if (isCompleted(false) && state != State::DETACHED)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Updating state to {} of file segment is not allowed, because it is already completed ({})",
            stateToString(state), getInfoForLogUnlocked(lock));
    }

    LOG_TEST(getLog(),"Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getReservedSize() const
{
    return reserved_size.load();
}

FileSegment::Priority::IteratorPtr FileSegment::getQueueIterator() const
{
    auto lk = lock();
    return queue_iterator;
}

void FileSegment::setQueueIterator(Priority::IteratorPtr iterator)
{
    auto lk = lock();
    if (queue_iterator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Queue iterator cannot be set twice");
    chassert(!on_delayed_removal);
    queue_iterator = iterator;
}

void FileSegment::markDelayedRemovalAndResetQueueIterator()
{
    auto lk = lock();
    on_delayed_removal = true;
    queue_iterator = {};
}

void FileSegment::restoreQueueIteratorAfterDelayedRemoval(
    Priority::IteratorPtr iterator)
{
    auto lk = lock();
    chassert(iterator);
    chassert(on_delayed_removal);
    chassert(!queue_iterator);
    queue_iterator = std::move(iterator);
    on_delayed_removal = false;
}

size_t FileSegment::getCurrentWriteOffset() const
{
    return range().left + downloaded_size;
}

size_t FileSegment::getDownloadedSize() const
{
    return downloaded_size;
}

bool FileSegment::isDownloaded() const
{
    /// Read without lock, see the comment in `state`: DOWNLOADED is published last, so observing it here
    /// implies a fully-downloaded, consistent segment.
    return download_state.load() == State::DOWNLOADED;
}

time_t FileSegment::getFinishedDownloadTime() const
{
    auto lk = lock();
    return download_finished_time;
}

String FileSegment::getCallerId()
{
    if (!CurrentThread::isInitialized() || CurrentThread::getQueryId().empty())
        return fmt::format("None:{}:{}", getThreadName(), toString(getThreadId()));

    return std::string(CurrentThread::getQueryId()) + ":" + toString(getThreadId());
}

String FileSegment::getDownloader() const
{
    return getDownloaderUnlocked(lock());
}

String FileSegment::getDownloaderUnlocked(const FileSegmentGuard::Lock &) const
{
    return download_data ? download_data->downloader_id : "";
}

FileSegment::DownloadState & FileSegment::getOrCreateDownloadDataUnlocked(const FileSegmentGuard::Lock &)
{
    if (!download_data)
        download_data = std::make_unique<DownloadState>();
    return *download_data;
}

void FileSegment::resetDownloadDataUnlocked(const FileSegmentGuard::Lock &)
{
    download_data.reset();
}

String FileSegment::getOrSetDownloader()
{
    auto lk = lock();

    assertNotDetachedUnlocked(lk);

    auto current_downloader = getDownloaderUnlocked(lk);

    if (current_downloader.empty())
    {
        const auto caller_id = getCallerId();
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED;
        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(download_state);

        current_downloader = getOrCreateDownloadDataUnlocked(lk).downloader_id = caller_id;
        setDownloadState(State::DOWNLOADING, lk);
        chassert(key_metadata.lock());
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked(const FileSegmentGuard::Lock & lock)
{
    chassert(isDownloaderUnlocked(lock));
    chassert(download_state == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSize();
    /// range().size() can equal 0 in case of write-though cache.
    if (!is_unbound && current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(lock);
    else if (current_downloaded_size)
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);
    else
        setDownloadState(State::EMPTY, lock);
}

void FileSegment::resetDownloader()
{
    auto lk = lock();

    SCOPE_EXIT({ cv.notify_all(); });

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("resetDownloader", lk);

    resetDownloadingStateUnlocked(lk);
    resetDownloaderUnlocked(lk);
}

void FileSegment::resetDownloaderUnlocked(const FileSegmentGuard::Lock &)
{
    if (!download_data || download_data->downloader_id.empty())
        return;

    LOG_TEST(getLog(),"Resetting downloader from {}", download_data->downloader_id);
    download_data->downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock & lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(lock);

    if (caller != current_downloader)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Operation `{}` can be done only by downloader. "
            "(CallerId: {}, downloader id: {})",
            operation, caller, current_downloader);
    }
}

bool FileSegment::isDownloader() const
{
    auto lk = lock();
    return isDownloaderUnlocked(lk);
}

bool FileSegment::isDownloaderUnlocked(const FileSegmentGuard::Lock & lock) const
{
    return getCallerId() == getDownloaderUnlocked(lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("getRemoteFileReader", lk);
    return download_data ? download_data->remote_file_reader : nullptr;
}

FileSegment::LocalCacheWriterPtr FileSegment::getLocalCacheWriter()
{
    auto lk = lock();
    return download_data ? download_data->cache_writer : nullptr;
}

void FileSegment::resetRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("resetRemoteFileReader", lk);
    if (download_data)
        download_data->remote_file_reader.reset();
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    auto lk = lock();
    if (download_data && download_data->remote_file_reader
        && (download_state == State::DOWNLOADED
            || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION))
    {
        return std::move(download_data->remote_file_reader);
    }
    return nullptr;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setRemoteFileReader", lk);

    auto & download = getOrCreateDownloadDataUnlocked(lk);
    if (download.remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    download.remote_file_reader = remote_file_reader_;
}

void FileSegment::write(char * from, size_t size, size_t offset_in_file)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWriteMicroseconds);
    auto file_segment_path = getPath();
    DownloadState * download = nullptr;
    {
        if (!size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing zero size is not allowed");

        {
            auto lk = lock();
            assertIsDownloaderUnlocked("write", lk);
            assertNotDetachedUnlocked(lk);
            download = &getOrCreateDownloadDataUnlocked(lk);
        }

        if (download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        const size_t first_non_downloaded_offset = getCurrentWriteOffset();

        if (offset_in_file != first_non_downloaded_offset)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to write {} bytes to offset: {}, but current write offset is {} ({})",
                size, offset_in_file, first_non_downloaded_offset, getInfoForLog());
        }

        const size_t current_downloaded_size = getDownloadedSize();
        chassert(reserved_size >= current_downloaded_size);

        const size_t free_reserved_size = reserved_size - current_downloaded_size;
        if (free_reserved_size < size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (!is_unbound)
        {
            if (current_downloaded_size == range().size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is already fully downloaded");

            if (current_downloaded_size + size > range().size())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot download beyond file segment boundaries: {}. Write offset: {}, size: {}, downloaded size: {}",
                    range().size(), first_non_downloaded_offset, size, current_downloaded_size);
            }
        }
    }

    try
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        /// This mutex is only needed to have a valid assertion in assertCacheCorrectness(),
        /// which is only executed in debug/sanitizer builds (under DEBUG_OR_SANITIZER_BUILD).
        std::lock_guard lock(download->write_mutex);
#endif

        if (!download->cache_writer)
        {
            int flags = -1;
            if (downloaded_size > 0)
                flags = O_WRONLY | O_APPEND | O_CLOEXEC;
            download->cache_writer = std::make_unique<WriteBufferFromFile>(getPath(), /* buf_size */0, flags);
        }

        fiu_do_on(FailPoints::cache_filesystem_failure,
        {
            throw ErrnoException(EIO, "Failpoint: simulated cache disk IO failure");
        });

        /// Size is equal to offset as offset for write buffer points to data end.
        download->cache_writer->set(from, /* size */size, /* offset */size);
        /// Reset the buffer when finished.
        SCOPE_EXIT({ download->cache_writer->set(nullptr, 0); });
        /// Flush the buffer.
        download->cache_writer->next();

        downloaded_size += size;
        chassert(std::filesystem::file_size(file_segment_path) == downloaded_size);
    }
    catch (ErrnoException & e)
    {
        const int code = e.getErrno();
        const bool is_no_space_left_error = code == /* No space left on device */28 || code == /* Quota exceeded */122;

        auto lk = lock();

        e.addMessage(fmt::format("{}, current cache state: {}", e.what(), getInfoForLogUnlocked(lk)));
        setDownloadFailedUnlocked(lk);

        if (fs::exists(file_segment_path))
        {
            if (downloaded_size == 0)
            {
                fs::remove(file_segment_path);
            }
            else if (is_no_space_left_error)
            {
                const auto file_size = fs::file_size(file_segment_path);

                LOG_TRACE(getLog(), "Failed to write to file: no space left on device "
                          "(file size: {}, downloaded size: {}, reserved size: {})",
                          file_size, downloaded_size.load(), reserved_size.load());

                chassert(downloaded_size <= file_size && file_size <= reserved_size);
                if (downloaded_size != file_size)
                    downloaded_size = file_size;
            }
        }

        throw;
    }
    catch (Exception & e)
    {
        auto lk = lock();
        e.addMessage(fmt::format("{}, current cache state: {}", e.what(), getInfoForLogUnlocked(lk)));
        setDownloadFailedUnlocked(lk);
        throw;
    }
    catch (const fs::filesystem_error & e)
    {
        auto lk = lock();
        setDownloadFailedUnlocked(lk);
        throw ErrnoException(e.code().value(),
            "Filesystem error in cache write ({}), current cache state: {}",
            e.what(), getInfoForLogUnlocked(lk));
    }

    chassert(getCurrentWriteOffset() == offset_in_file + size);
}

FileSegment::State FileSegment::wait(size_t offset)
{
    OpenTelemetry::SpanHolder span("FileSegment::wait");
    span.addAttribute("clickhouse.key", key().toString());
    span.addAttribute("clickhouse.offset", offset);

    auto lk = lock();

    if (getDownloaderUnlocked(lk).empty() || offset < getCurrentWriteOffset())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(getLog(),"{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), getDownloaderUnlocked(lk));
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWaitMicroseconds);

        chassert(!getDownloaderUnlocked(lk).empty());
        chassert(!isDownloaderUnlocked(lk));

        /// Wait for the download in short slices so that cancellation of the waiting query
        /// (KILL QUERY, max_execution_time, a dropped/stopped refreshable materialized view, ...)
        /// is observed promptly. The condition variable is only notified on download progress, so a
        /// stalled or dead downloader would otherwise pin this thread — and anything blocked on it,
        /// e.g. RefreshTask::shutdown() -> deactivate() — until the full timeout. throwIfKilled()
        /// re-raises the query's original cancellation reason rather than a generic one.
        QueryStatusPtr query_status;
        if (auto query_context = CurrentThread::tryGetQueryContext())
            query_status = query_context->getProcessListElementSafe();

        auto downloaded = [&, this]()
        {
            return download_state != State::DOWNLOADING || offset < getCurrentWriteOffset();
        };
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (true)
        {
            if (query_status)
                query_status->throwIfKilled();
            if (cv.wait_for(lk, std::chrono::seconds(1), downloaded))
                break;
            if (std::chrono::steady_clock::now() >= deadline)
                break;
        }
    }

    return download_state;
}

KeyMetadataPtr FileSegment::getKeyMetadata() const
{
    auto metadata = tryGetKeyMetadata();
    if (metadata)
        return metadata;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot lock key, key metadata is not set ({})", stateToString(download_state));
}

KeyMetadataPtr FileSegment::tryGetKeyMetadata() const
{
    auto metadata = key_metadata.lock();
    if (metadata)
        return metadata;
    return nullptr;
}

LockedKeyPtr FileSegment::lockKeyMetadata(bool assert_exists) const
{
    if (assert_exists)
        return getKeyMetadata()->lock();

    auto metadata = tryGetKeyMetadata();
    if (!metadata)
        return nullptr;
    return metadata->tryLock();
}

bool FileSegment::reserve(
    size_t size_to_reserve,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason,
    FileCacheReserveStat * reserve_stat,
    size_t reserve_hint)
{
    if (!size_to_reserve)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Zero space reservation is not allowed");

    size_t current_downloaded_size = 0;

    bool is_file_segment_size_exceeded = false;
    {
        auto lk = lock();

        assertNotDetachedUnlocked(lk);
        assertIsDownloaderUnlocked("reserve", lk);

        current_downloaded_size = getDownloadedSize();

        is_file_segment_size_exceeded = current_downloaded_size + size_to_reserve > range().size();
        if (is_file_segment_size_exceeded && !is_unbound)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                size_to_reserve, range().toString(), downloaded_size.load());
        }

        chassert(reserved_size >= current_downloaded_size);
    }

    chassert(range().size() >= reserved_size);

    if (reserved_size > current_downloaded_size)
    {
        const size_t available_reserved = reserved_size - current_downloaded_size;
        if (available_reserved >= size_to_reserve)
            return true;
        size_to_reserve -= available_reserved;
    }

    const size_t minimum_reserve_size = size_to_reserve;

    if (!is_unbound)
    {
        const auto reserve_granularity = cache->getReserveGranularity();
        if (reserve_granularity && reserve_granularity > size_to_reserve)
        {
            size_to_reserve = reserved_size + reserve_granularity > range().size()
                ? range().size() - reserved_size
                : reserve_granularity;

            /// `reserve_hint` is measured from the current download offset, so the read ends at
            /// `read_horizon` in segment-relative terms. Don't reserve ahead past it.
            const size_t read_horizon = current_downloaded_size + reserve_hint;
            if (reserve_hint
                && read_horizon > reserved_size
                && read_horizon < reserved_size + size_to_reserve)
                size_to_reserve = read_horizon - reserved_size;
        }
    }

    /// The reserve-ahead caps above (segment range, read horizon) are only an upper bound; they
    /// must never reserve less than the current write needs, otherwise the write would exceed the
    /// reservation. A bare assert would not protect release builds, so clamp explicitly.
    size_to_reserve = std::max(size_to_reserve, minimum_reserve_size);

    /// This (resizable file segments) is allowed only for single threaded use of file segment.
    /// Currently it is used only for temporary files through cache.
    if (is_unbound && is_file_segment_size_exceeded)
        /// Note: segment_range.right is inclusive.
        segment_range.right = range().left + current_downloaded_size + size_to_reserve - 1;

    /// if reserve_stat is not passed then use dummy stat and discard the result.
    FileCacheReserveStat dummy_stat;
    if (!reserve_stat)
        reserve_stat = &dummy_stat;

    bool reserved = cache->tryReserve(
        *this, size_to_reserve, *reserve_stat, *getKeyMetadata()->origin, lock_wait_timeout_milliseconds, failure_reason);

    if (!reserved)
        setDownloadFailedUnlocked(lock());

    return reserved;
}

void FileSegment::setDownloadedUnlocked(const FileSegmentGuard::Lock & lock)
{
    if (download_state == State::DOWNLOADED)
        return;

    download_finished_time = timeInSeconds(std::chrono::system_clock::now());

    if (download_data && download_data->cache_writer)
    {
        try
        {
            download_data->cache_writer->finalize();
        }
        catch (...)
        {
            tryLogCurrentException(getLog(), "Failed to finalize cache writer while marking file segment as downloaded");
            setDownloadFailedUnlocked(lock);
            return;
        }
    }

    resetDownloadDataUnlocked(lock);

    chassert(downloaded_size > 0);
    chassert(fs::file_size(getPath()) == downloaded_size);

    download_state = State::DOWNLOADED;
}

void FileSegment::setDownloadFailed()
{
    auto lk = lock();
    setDownloadFailedUnlocked(lk);
}

void FileSegment::setDownloadFinishedWithoutContinuation()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setDownloadFinishedWithoutContinuation", lk);
    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lk);
    cv.notify_all();
}

void FileSegment::setDownloadFailedUnlocked(const FileSegmentGuard::Lock & lock)
{
    LOG_INFO(getLog(), "Setting download as failed: {}", getInfoForLogUnlocked(lock));

    SCOPE_EXIT({ cv.notify_all(); });

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lock);

    /// Keep `download_data` (downloader_id) so the same downloader can still complete the segment.
    if (download_data)
    {
        if (download_data->cache_writer)
        {
            download_data->cache_writer->cancel();
            download_data->cache_writer.reset();
        }
        download_data->remote_file_reader.reset();
    }
}

void FileSegment::completePartAndResetDownloader()
{
    auto lk = lock();

    SCOPE_EXIT({ cv.notify_all(); });

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", lk);

    chassert(download_state == State::DOWNLOADING
             || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

    if (download_state == State::DOWNLOADING)
        resetDownloadingStateUnlocked(lk);

    resetDownloaderUnlocked(lk);

    LOG_TEST(getLog(),"Complete batch. ({})", getInfoForLogUnlocked(lk));
}

void FileSegment::shrinkFileSegmentToDownloadedSize(const LockedKey & locked_key, const FileSegmentGuard::Lock & lock, bool force_shrink_to_downloaded_size)
{
    chassert(downloaded_size);
    chassert(fs::file_size(getPath()) > 0);

    if (downloaded_size == range().size())
    {
        /// Nothing to resize;
        return;
    }

    if (!locked_key.isLastOwnerOfFileSegment(offset()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Shrinking of file segment can be done only by the last holder: {}",
            getInfoForLog());
    }

    size_t result_size = downloaded_size;
    if (!force_shrink_to_downloaded_size)
    {
        size_t aligned_downloaded_size = FileCacheUtils::roundUpToMultiple(downloaded_size, cache->getBoundaryAlignment());
        result_size = std::min(aligned_downloaded_size, range().size());
    }

    chassert(result_size <= range().size());
    chassert(result_size >= downloaded_size);

    /// Return the reserve-ahead surplus (reserved but not downloaded, see `FileSegment::reserve`)
    /// to the cache: the segment is complete, nothing will fill the rest, so the surplus must not
    /// stay charged against the quota. Done before the `result_size == range().size()` early return
    /// below, since with `reserve_granularity == boundary_alignment` a tiny read rounds up to the
    /// whole range and would otherwise keep a full granule charged.
    chassert(reserved_size >= downloaded_size);
    if (reserved_size > downloaded_size)
    {
        queue_iterator->decrementSize(reserved_size - downloaded_size);
        reserved_size = downloaded_size.load();
    }

    if (result_size == range().size())
    {
        /// Nothing to resize;
        return;
    }

    LOG_TEST(getLog(),"Shrinking file segment {} -> {} (downloaded size: {})",
             range().size(), result_size, downloaded_size.load());

    segment_range.right = segment_range.left + result_size - 1;

    if (downloaded_size == result_size)
    {
        /// Terminal state: free the download-only state so it is not leaked on an
        /// already-cached segment (and to uphold the `!download_data` invariant).
        resetDownloadDataUnlocked(lock);
        setDownloadState(State::DOWNLOADED, lock);
    }
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);
}

size_t FileSegment::getSizeForBackgroundDownload() const
{
    auto lk = lock();
    return getSizeForBackgroundDownloadUnlocked(lk);
}

size_t FileSegment::getSizeForBackgroundDownloadUnlocked(const FileSegmentGuard::Lock &) const
{
    if (!background_download_enabled
        || !downloaded_size
        || !download_data
        || !download_data->remote_file_reader)
    {
        return 0;
    }

    chassert(downloaded_size <= range().size());

    const size_t background_download_max_file_segment_size = cache->getBackgroundDownloadMaxFileSegmentSize();
    size_t desired_size = 0;
    if (downloaded_size >= background_download_max_file_segment_size)
        desired_size = FileCacheUtils::roundUpToMultiple(downloaded_size, cache->getBoundaryAlignment());
    else
        desired_size = FileCacheUtils::roundUpToMultiple(background_download_max_file_segment_size, cache->getBoundaryAlignment());

    desired_size = std::min(desired_size, range().size());
    chassert(desired_size >= downloaded_size);

    return desired_size - downloaded_size;
}

void FileSegment::complete(FileSegmentPtr && file_segment, bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    if (!file_segment)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is nullptr");

    if (file_segment->isCompleted())
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentCompleteMicroseconds);

    auto locked_key = file_segment->lockKeyMetadata(false);
    if (!locked_key)
    {
        /// If we failed to lock a key, it must be in detached state.
        if (file_segment->isDetached())
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot complete file segment: {}", file_segment->getInfoForLog());
    }

    SCOPE_EXIT_SAFE(
        file_segment.reset();
    );

    file_segment->complete(locked_key, allow_background_download, force_shrink_to_downloaded_size);
}

void FileSegment::complete(const LockedKeyPtr & locked_key, bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    auto segment_lock = lock();

    if (isCompleted(false))
        return;

    const bool is_downloader = isDownloaderUnlocked(segment_lock);
    const bool is_last_holder = locked_key->isLastOwnerOfFileSegment(offset());
    const size_t current_downloaded_size = getDownloadedSize();

    SCOPE_EXIT({
        if (is_downloader)
            cv.notify_all();
    });

    LOG_TEST(
        getLog(), "Complete based on current state (is_last_holder: {}, force shrink: {}, {})",
        is_last_holder, force_shrink_to_downloaded_size, getInfoForLogUnlocked(segment_lock));

    if (is_downloader)
    {
        if (download_state == State::DOWNLOADING)
            resetDownloadingStateUnlocked(segment_lock);
        resetDownloaderUnlocked(segment_lock);
    }

    if (segment_kind == FileSegmentKind::Ephemeral && is_last_holder)
    {
        LOG_TEST(getLog(),"Removing temporary file segment: {}", getInfoForLogUnlocked(segment_lock));
        locked_key->removeFileSegment(offset(), segment_lock);
        return;
    }

    switch (download_state)
    {
        case State::DOWNLOADED:
        {
            chassert(current_downloaded_size == range().size());
            chassert(current_downloaded_size == fs::file_size(getPath()));
            chassert(!download_data);
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!is_last_holder);
            break;
        }
        case State::EMPTY:
        {
            if (is_last_holder)
                locked_key->removeFileSegment(offset(), segment_lock);
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        {
            chassert(current_downloaded_size > 0);
            chassert(fs::exists(getPath()));
            chassert(fs::file_size(getPath()) > 0);

            if (is_last_holder)
            {
                bool added_to_download_queue = false;
                size_t background_download_size = allow_background_download ? getSizeForBackgroundDownloadUnlocked(segment_lock) : 0;
                if (background_download_size)
                {
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundDownloadQueuePush);
                    added_to_download_queue = locked_key->addToDownloadQueue(offset(), segment_lock); /// Finish download in background.
                }

                if (!added_to_download_queue)
                {
                    /// Reset the writer and reader to reduce memory usage and not keep the
                    /// remote request alive, because we do not know when download will be
                    /// continued next time.
                    if (download_data)
                    {
                        if (download_data->cache_writer)
                        {
                            try
                            {
                                download_data->cache_writer->finalize();
                            }
                            catch (...)
                            {
                                tryLogCurrentException(getLog(), "Failed to finalize cache writer on complete");
                            }
                            download_data->cache_writer.reset();
                        }
                        download_data->remote_file_reader.reset();
                    }

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            chassert(current_downloaded_size != range().size());

            if (is_last_holder)
            {
                if (current_downloaded_size == 0)
                {
                    locked_key->removeFileSegment(offset(), segment_lock);
                }
                else
                {
                    LOG_TEST(getLog(),"Resize file segment {} to downloaded: {}", range().toString(), current_downloaded_size);

                    /// Reset the writer and reader to reduce memory usage and not keep the
                    /// remote request alive, because we do not know when download will be
                    /// continued next time.
                    if (download_data)
                    {
                        if (download_data->cache_writer)
                        {
                            try
                            {
                                download_data->cache_writer->finalize();
                            }
                            catch (...)
                            {
                                tryLogCurrentException(getLog(), "Failed to finalize cache writer on complete");
                            }
                            download_data->cache_writer.reset();
                        }
                        download_data->remote_file_reader.reset();
                    }

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state while completing file segment");
    }

    LOG_TEST(getLog(),"Completed file segment: {}", getInfoForLogUnlocked(segment_lock));

    if (download_state != State::DETACHED)
        chassert(assertCorrectnessUnlocked(segment_lock));
}

String FileSegment::getInfoForLog() const
{
    auto lk = lock();
    return getInfoForLogUnlocked(lk);
}

String FileSegment::getInfoForLogUnlocked(const FileSegmentGuard::Lock & lock) const
{
    const auto downloader_id = getDownloaderUnlocked(lock);
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "key: " << key().toString() << ", ";
    info << "state: " << download_state.load() << ", ";
    info << "downloaded size: " << getDownloadedSize() << ", ";
    info << "reserved size: " << reserved_size.load() << ", ";
    info << "downloader id: " << (downloader_id.empty() ? "None" : downloader_id) << ", ";
    info << "current write offset: " << getCurrentWriteOffset() << ", ";
    info << "caller id: " << getCallerId() << ", ";
    info << "kind: " << toString(segment_kind) << ", ";
    info << "unbound: " << is_unbound << ", ";
    info << "background download: " << background_download_enabled;

    return info.str();
}

String FileSegment::stateToString(FileSegment::State state)
{
    switch (state)
    {
        case FileSegment::State::DOWNLOADED:
            return "DOWNLOADED";
        case FileSegment::State::EMPTY:
            return "EMPTY";
        case FileSegment::State::DOWNLOADING:
            return "DOWNLOADING";
        case FileSegment::State::PARTIALLY_DOWNLOADED:
            return "PARTIALLY DOWNLOADED";
        case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            return "PARTIALLY DOWNLOADED NO CONTINUATION";
        case FileSegment::State::DETACHED:
            return "DETACHED";
    }
}

bool FileSegment::assertCorrectness() const
{
    return assertCorrectnessUnlocked(lock());
}

bool FileSegment::assertCorrectnessUnlocked(const FileSegmentGuard::Lock & lock) const
{
    auto throw_logical = [&](const std::string & error)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}. File segment info: {}", error, getInfoForLogUnlocked(lock));
    };

    auto check_iterator = [&](const Priority::IteratorPtr & it)
    {
        UNUSED(this);
        if (!it)
            return;

        auto entry = it->getEntry();
        auto entry_size = entry->size.load(std::memory_order_relaxed);
        if (entry_size == 0)
        {
            /// A race in case of SLRU eviction is possible here
            /// when we do setIterator during downgrade.
            /// Then as entry is invalidated right after we set a new iterator
            /// - just fetch entry once more.
            entry = it->getEntry();
            entry_size = entry->size;
        }
        if (download_state != State::DOWNLOADING && entry_size != reserved_size)
            throw_logical(
                fmt::format("Expected entry.size == reserved_size ({} == {}, entry: {})",
                            entry_size, reserved_size.load(), entry->toString()));

        chassert(entry->key == key());
        chassert(entry->offset == offset());
    };

    const auto file_path = getPath();

    {
        /// `write_mutex` only exists while downloading; otherwise no writer can race here.
        std::unique_lock<std::mutex> write_lk;
        if (download_data)
            write_lk = std::unique_lock(download_data->write_mutex);

        if (downloaded_size == 0)
        {
            if (download_state != State::DOWNLOADING && fs::exists(file_path))
                throw_logical("Expected file " + file_path + " not to exist");
        }
        else if (!fs::exists(file_path))
        {
            throw_logical("Expected file " + file_path + " to exist");
        }
    }

    /// A restored queue iterator must clear the delayed-removal state.
    if (queue_iterator)
        chassert(!on_delayed_removal);

    switch (download_state.load())
    {
        case State::EMPTY:
        {
            chassert(getDownloaderUnlocked(lock).empty());
            chassert(!fs::exists(getPath()));
            chassert(!queue_iterator);
            break;
        }
        case State::DOWNLOADED:
        {
            chassert(!download_data);

            chassert(downloaded_size == reserved_size);
            chassert(downloaded_size == range().size());
            chassert(downloaded_size > 0);

            auto file_size = fs::file_size(getPath());
            UNUSED(file_size);

            chassert(file_size == range().size());
            chassert(downloaded_size == range().size());

            chassert(queue_iterator || on_delayed_removal);
            check_iterator(queue_iterator);
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!getDownloaderUnlocked(lock).empty());
            if (downloaded_size)
            {
                chassert(queue_iterator);
                chassert(fs::file_size(getPath()) > 0);
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        {
            chassert(getDownloaderUnlocked(lock).empty());

            chassert(reserved_size >= downloaded_size);
            chassert(downloaded_size > 0);

            auto file_size = fs::file_size(getPath());
            UNUSED(file_size);

            chassert(file_size > 0);
            chassert(file_size <= range().size());
            chassert(downloaded_size <= range().size());

            chassert(queue_iterator || on_delayed_removal);
            check_iterator(queue_iterator);
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            chassert(reserved_size >= downloaded_size);
            check_iterator(queue_iterator);
            break;
        }
        case State::DETACHED:
        {
            break;
        }
    }

    return true;
}

void FileSegment::assertNotDetached() const
{
    auto lk = lock();
    assertNotDetachedUnlocked(lk);
}

void FileSegment::assertNotDetachedUnlocked(const FileSegmentGuard::Lock & lock) const
{
    if (download_state == State::DETACHED)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache file segment is in detached state, operation not allowed. "
            "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
            "Please, retry. File segment info: {}", getInfoForLogUnlocked(lock));
    }
}

FileSegment::Info FileSegment::getInfo(const FileSegmentPtr & file_segment)
{
    auto lock = file_segment->lock();
    auto key_metadata = file_segment->tryGetKeyMetadata();
    return Info{
        .key = file_segment->key(),
        .offset = file_segment->offset(),
        .path = file_segment->tryGetPath(),
        .range_left = file_segment->range().left,
        .range_right = file_segment->range().right,
        .kind = file_segment->segment_kind,
        .state = file_segment->download_state,
        .size = file_segment->range().size(),
        .downloaded_size = file_segment->downloaded_size,
        .download_finished_time = file_segment->download_finished_time,
        .cache_hits = file_segment->hits_count,
        .references = static_cast<uint64_t>(file_segment.use_count()),
        .is_unbound = file_segment->is_unbound,
        .queue_entry_type = file_segment->queue_iterator ? file_segment->queue_iterator->getType() : QueueEntryType::None,
        .origin = *key_metadata->origin,
    };
}

bool FileSegment::isDetached() const
{
    /// Keep the lock: `complete` uses `isDetached` to confirm a benign concurrent detach when
    /// `lockKeyMetadata` fails. `setDetachedState` sets DETACHED and resets `key_metadata` under
    /// the segment lock, so only taking the lock here guarantees we observe DETACHED once the key
    /// metadata is gone - a bare atomic load could race and turn the detach into a `LOGICAL_ERROR`.
    auto lk = lock();
    return download_state == State::DETACHED;
}

bool FileSegment::isCompleted(bool sync) const
{
    auto is_completed_state = [this]() -> bool
    {
        return download_state == State::DOWNLOADED || download_state == State::DETACHED;
    };

    if (sync)
    {
        if (is_completed_state())
            return true;

        auto lk = lock();
        return is_completed_state();
    }

    return is_completed_state();
}

void FileSegment::setDetachedState(const FileSegmentGuard::Lock & lock)
{
    setDownloadState(State::DETACHED, lock);
    key_metadata.reset();
    queue_iterator = nullptr;
    if (download_data && download_data->cache_writer)
        download_data->cache_writer->cancel();
    resetDownloadDataUnlocked(lock);
}

void FileSegment::detach(const FileSegmentGuard::Lock & lock, const LockedKey &)
{
    if (download_state == State::DETACHED)
        return;

    if (!getDownloaderUnlocked(lock).empty())
        resetDownloaderUnlocked(lock);
    setDetachedState(lock);
}

void FileSegment::increasePriority()
{
    if (!cache)
    {
        chassert(isDetached());
        return;
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentIncreasePriorityMicroseconds);

    /// In case of concurrently called increasePriority()
    /// we want to increase a priority only once
    /// (because it does not really make any sense
    /// to do it immediately again after we've just done it)
    if (!increasing_priority.test_and_set(std::memory_order_acquire))
    {
        SCOPE_EXIT({ increasing_priority.clear(std::memory_order_release); });

        auto it = getQueueIterator();
        if (it)
        {
            if (!cache->tryIncreasePriority(*this))
                ProfileEvents::increment(ProfileEvents::FileSegmentFailToIncreasePriority);

            /// Used only for system.filesystem_cache.
            ++hits_count;
        }
    }
}

FileSegment::~FileSegment()
{
    try
    {
        /// Can be non-finalized in case it was push to background download
        /// but not executed before server shutdown.
        if (download_data && download_data->cache_writer)
            download_data->cache_writer->finalize();
    }
    catch (...)
    {
        tryLogCurrentException(getLog());
    }

    CurrentMetrics::sub(CurrentMetrics::CacheFileSegments);
}

FileSegmentsHolder::FileSegmentsHolder(FileSegments && file_segments_)
    : file_segments(std::move(file_segments_))
{
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments, file_segments.size());
    ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments, file_segments.size());
}

FileSegmentPtr FileSegmentsHolder::getSingleFileSegment() const
{
    if (file_segments.size() != 1)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected single file segment, got: {} in holder {}",
            file_segments.size(), toString());
    }
    return file_segments.front();
}

void FileSegmentsHolder::reset()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentHolderCompleteMicroseconds);

    ProfileEvents::increment(ProfileEvents::FilesystemCacheUnusedHoldFileSegments, file_segments.size());
    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        try
        {
            /// One might think it would have been more correct to do `false` here,
            /// not to allow background download for file segments that we actually did not start reading.
            /// But actually we would only do that, if those file segments were already read partially by some other thread/query
            /// but they were not put to the download queue, because current thread was holding them in Holder.
            /// So as a culprit, we need to allow to happen what would have happened if we did not exist.
            file_segment_it = completeAndPopFrontImpl(/*allow_background_download=*/true, /*force_shrink_to_downloaded_size=*/false);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }
    file_segments.clear();
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    reset();
}

FileSegments::iterator FileSegmentsHolder::completeAndPopFrontImpl(bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    auto file_segment_it = file_segments.begin();
    FileSegment::complete(std::move(*file_segment_it), allow_background_download, force_shrink_to_downloaded_size);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheHoldFileSegments);
    return file_segments.erase(file_segment_it);
}

FileSegment & FileSegmentsHolder::add(FileSegmentPtr && file_segment)
{
    file_segments.push_back(file_segment);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments);
    return *file_segments.back();
}

String FileSegmentsHolder::toString(bool with_state) const
{
    return DB::toString(file_segments, with_state);
}

String toString(const FileSegments & file_segments, bool with_state)
{
    String ranges;
    for (const auto & file_segment : file_segments)
    {
        if (!ranges.empty())
            ranges += ", ";
        ranges += file_segment->range().toString();
        if (file_segment->isUnbound())
            ranges += "(unbound)";
        if (with_state)
            ranges += "(" + FileSegment::stateToString(file_segment->state()) + ")";
    }
    return ranges;
}

}
