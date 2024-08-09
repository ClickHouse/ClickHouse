#include "FileSegment.h"

#include <filesystem>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cache/FileCache.h>
#include <base/getThreadId.h>
#include <base/hex.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

#include <magic_enum.hpp>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FileSegmentWaitMicroseconds;
    extern const Event FileSegmentCompleteMicroseconds;
    extern const Event FileSegmentLockMicroseconds;
    extern const Event FileSegmentWriteMicroseconds;
    extern const Event FileSegmentUseMicroseconds;
    extern const Event FileSegmentHolderCompleteMicroseconds;
    extern const Event FileSegmentFailToIncreasePriority;
    extern const Event FilesystemCacheHoldFileSegments;
    extern const Event FilesystemCacheUnusedHoldFileSegments;
}

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheHoldFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
#ifdef ABORT_ON_LOGICAL_ERROR
    , log(getLogger(fmt::format("FileSegment({}) : {}", key_.toString(), range().toString())))
#else
    , log(getLogger("FileSegment"))
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
        /// or on shrinkFileSegmentToDownloadedSize() -- when file segment object is updated.
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
}

FileSegment::Range::Range(size_t left_, size_t right_) : left(left_), right(right_)
{
    if (left > right)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to create incorrect range: [{}, {}]", left, right);
}

FileSegment::State FileSegment::state() const
{
    auto lk = lock();
    return download_state;
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

    LOG_TEST(log, "Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getReservedSize() const
{
    auto lk = lock();
    return reserved_size;
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
    queue_iterator = iterator;
}

void FileSegment::resetQueueIterator()
{
    auto lk = lock();
    queue_iterator.reset();
}

size_t FileSegment::getCurrentWriteOffset() const
{
    return range().left + downloaded_size;
}

size_t FileSegment::getDownloadedSize() const
{
    return downloaded_size;
}

void FileSegment::setDownloadedSize(size_t delta)
{
    auto lk = lock();
    downloaded_size += delta;
    assert(downloaded_size == std::filesystem::file_size(getPath()));
}

bool FileSegment::isDownloaded() const
{
    auto lk = lock();
    return download_state == State::DOWNLOADED;
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
    return downloader_id;
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

        current_downloader = downloader_id = caller_id;
        setDownloadState(State::DOWNLOADING, lk);
        chassert(key_metadata.lock());
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked(const FileSegmentGuard::Lock & lock)
{
    assert(isDownloaderUnlocked(lock));
    assert(download_state == State::DOWNLOADING);

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
    if (downloader_id.empty())
        return;

    LOG_TEST(log, "Resetting downloader from {}", downloader_id);
    downloader_id.clear();
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
            operation, caller, downloader_id);
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
    return remote_file_reader;
}

void FileSegment::resetRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("resetRemoteFileReader", lk);
    remote_file_reader.reset();
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    auto lk = lock();
    if (remote_file_reader && (download_state == State::DOWNLOADED
        || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION))
    {
        return std::move(remote_file_reader);
    }
    return nullptr;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setRemoteFileReader", lk);

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::write(char * from, size_t size, size_t offset)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWriteMicroseconds);

    if (!size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing zero size is not allowed");

    {
        auto lk = lock();
        assertIsDownloaderUnlocked("write", lk);
        assertNotDetachedUnlocked(lk);
    }

    const auto file_segment_path = getPath();

    {
        if (download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        const size_t first_non_downloaded_offset = getCurrentWriteOffset();
        if (offset != first_non_downloaded_offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to write {} bytes to offset: {}, but current write offset is {}",
                size, offset, first_non_downloaded_offset);

        const size_t current_downloaded_size = getDownloadedSize();
        chassert(reserved_size >= current_downloaded_size);

        const size_t free_reserved_size = reserved_size - current_downloaded_size;
        if (free_reserved_size < size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (!is_unbound && current_downloaded_size == range().size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is already fully downloaded");

        if (!cache_writer && current_downloaded_size > 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cache writer was finalized (downloaded size: {}, state: {})",
                current_downloaded_size, stateToString(download_state));
    }

    try
    {
#ifdef ABORT_ON_LOGICAL_ERROR
        /// This mutex is only needed to have a valid assertion in assertCacheCorrectness(),
        /// which is only executed in debug/sanitizer builds (under ABORT_ON_LOGICAL_ERROR).
        std::lock_guard lock(write_mutex);
#endif

        if (!cache_writer)
            cache_writer = std::make_unique<WriteBufferFromFile>(file_segment_path, /* buf_size */0);

        /// Size is equal to offset as offset for write buffer points to data end.
        cache_writer->set(from, size, /* offset */size);
        /// Reset the buffer when finished.
        SCOPE_EXIT({ cache_writer->set(nullptr, 0); });
        /// Flush the buffer.
        cache_writer->next();

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

        if (downloaded_size == 0 && fs::exists(file_segment_path))
        {
            fs::remove(file_segment_path);
        }
        else if (is_no_space_left_error)
        {
            const auto file_size = fs::file_size(file_segment_path);

            chassert(downloaded_size <= file_size);
            chassert(reserved_size >= file_size);
            chassert(file_size <= range().size());

            if (downloaded_size != file_size)
                downloaded_size = file_size;
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

    chassert(getCurrentWriteOffset() == offset + size);
}

FileSegment::State FileSegment::wait(size_t offset)
{
    OpenTelemetry::SpanHolder span("FileSegment::wait");
    span.addAttribute("clickhouse.key", key().toString());
    span.addAttribute("clickhouse.offset", offset);

    auto lk = lock();

    if (downloader_id.empty() || offset < getCurrentWriteOffset())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWaitMicroseconds);

        chassert(!getDownloaderUnlocked(lk).empty());
        chassert(!isDownloaderUnlocked(lk));

        [[maybe_unused]] const auto ok = cv.wait_for(lk, std::chrono::seconds(60), [&, this]()
        {
            return download_state != State::DOWNLOADING || offset < getCurrentWriteOffset();
        });
        /// chassert(ok);
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

bool FileSegment::reserve(size_t size_to_reserve, size_t lock_wait_timeout_milliseconds, FileCacheReserveStat * reserve_stat)
{
    if (!size_to_reserve)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Zero space reservation is not allowed");

    size_t expected_downloaded_size;

    bool is_file_segment_size_exceeded;
    {
        auto lk = lock();

        assertNotDetachedUnlocked(lk);
        assertIsDownloaderUnlocked("reserve", lk);

        expected_downloaded_size = getDownloadedSize();

        is_file_segment_size_exceeded = expected_downloaded_size + size_to_reserve > range().size();
        if (is_file_segment_size_exceeded && !is_unbound)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                size_to_reserve, range().toString(), downloaded_size);
        }

        chassert(reserved_size >= expected_downloaded_size);
    }

    /**
     * It is possible to have downloaded_size < reserved_size when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */

    size_t already_reserved_size = reserved_size - expected_downloaded_size;

    if (already_reserved_size >= size_to_reserve)
        return true;

    size_to_reserve = size_to_reserve - already_reserved_size;

    /// This (resizable file segments) is allowed only for single threaded use of file segment.
    /// Currently it is used only for temporary files through cache.
    if (is_unbound && is_file_segment_size_exceeded)
        /// Note: segment_range.right is inclusive.
        segment_range.right = range().left + expected_downloaded_size + size_to_reserve - 1;

    /// if reserve_stat is not passed then use dummy stat and discard the result.
    FileCacheReserveStat dummy_stat;
    if (!reserve_stat)
        reserve_stat = &dummy_stat;

    bool reserved = cache->tryReserve(*this, size_to_reserve, *reserve_stat, getKeyMetadata()->user, lock_wait_timeout_milliseconds);

    if (!reserved)
        setDownloadFailedUnlocked(lock());

    return reserved;
}

void FileSegment::setDownloadedUnlocked(const FileSegmentGuard::Lock &)
{
    if (download_state == State::DOWNLOADED)
        return;

    download_state = State::DOWNLOADED;

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }

    chassert(downloaded_size > 0);
    chassert(fs::file_size(getPath()) == downloaded_size);
}

void FileSegment::setDownloadFailed()
{
    auto lk = lock();
    setDownloadFailedUnlocked(lk);
}

void FileSegment::setDownloadFailedUnlocked(const FileSegmentGuard::Lock & lock)
{
    LOG_INFO(log, "Setting download as failed: {}", getInfoForLogUnlocked(lock));

    SCOPE_EXIT({ cv.notify_all(); });

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lock);

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
    }

    remote_file_reader.reset();
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

    LOG_TEST(log, "Complete batch. ({})", getInfoForLogUnlocked(lk));
}

void FileSegment::complete()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentCompleteMicroseconds);

    if (isCompleted())
        return;

    auto locked_key = lockKeyMetadata(false);
    if (!locked_key)
    {
        /// If we failed to lock a key, it must be in detached state.
        if (isDetached())
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot complete file segment: {}", getInfoForLog());
    }

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
        log, "Complete based on current state (is_last_holder: {}, {})",
        is_last_holder, getInfoForLogUnlocked(segment_lock));

    if (is_downloader)
    {
        if (download_state == State::DOWNLOADING)
            resetDownloadingStateUnlocked(segment_lock);
        resetDownloaderUnlocked(segment_lock);
    }

    if (segment_kind == FileSegmentKind::Temporary && is_last_holder)
    {
        LOG_TEST(log, "Removing temporary file segment: {}", getInfoForLogUnlocked(segment_lock));
        locked_key->removeFileSegment(offset(), segment_lock);
        return;
    }

    switch (download_state)
    {
        case State::DOWNLOADED:
        {
            chassert(current_downloaded_size == range().size());
            chassert(current_downloaded_size == fs::file_size(getPath()));
            chassert(!cache_writer);
            chassert(!remote_file_reader);
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

            if (is_last_holder)
            {
                bool added_to_download_queue = false;
                if (background_download_enabled && remote_file_reader)
                {
                    added_to_download_queue = locked_key->addToDownloadQueue(offset(), segment_lock); /// Finish download in background.
                }

                if (!added_to_download_queue)
                {
                    locked_key->shrinkFileSegmentToDownloadedSize(offset(), segment_lock);
                    setDetachedState(segment_lock); /// See comment below.
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
                    LOG_TEST(log, "Resize file segment {} to downloaded: {}", range().toString(), current_downloaded_size);

                    /**
                    * Only last holder of current file segment can resize the file segment,
                    * because there is an invariant that file segments returned to users
                    * in FileSegmentsHolder represent a contiguous range, so we can resize
                    * it only when nobody needs it.
                    */

                    /// Resize this file segment by creating a copy file segment with DOWNLOADED state,
                    /// but current file segment should remain PARRTIALLY_DOWNLOADED_NO_CONTINUATION and with detached state,
                    /// because otherwise an invariant that getOrSet() returns a contiguous range of file segments will be broken
                    /// (this will be crucial for other file segment holder, not for current one).
                    locked_key->shrinkFileSegmentToDownloadedSize(offset(), segment_lock);

                    /// We mark current file segment with state DETACHED, even though the data is still in cache
                    /// (but a separate file segment) because is_last_holder is satisfied, so it does not matter.
                    setDetachedState(segment_lock);
                }
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state while completing file segment");
    }

    LOG_TEST(log, "Completed file segment: {}", getInfoForLogUnlocked(segment_lock));
}

String FileSegment::getInfoForLog() const
{
    auto lk = lock();
    return getInfoForLogUnlocked(lk);
}

String FileSegment::getInfoForLogUnlocked(const FileSegmentGuard::Lock &) const
{
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
    info << "unbound: " << is_unbound;

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

        const auto & entry = it->getEntry();
        if (download_state != State::DOWNLOADING && entry->size != reserved_size)
            throw_logical(fmt::format("Expected entry.size == reserved_size ({} == {})", entry->size, reserved_size));

        chassert(entry->key == key());
        chassert(entry->offset == offset());
    };

    const auto file_path = getPath();
    if (segment_kind != FileSegmentKind::Temporary)
    {
        std::lock_guard lk(write_mutex);
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

    if (download_state == State::DOWNLOADED)
    {
        chassert(downloader_id.empty());
        chassert(downloaded_size == reserved_size);
        chassert(downloaded_size == range().size());
        chassert(downloaded_size > 0);
        chassert(std::filesystem::file_size(getPath()) > 0);
        check_iterator(queue_iterator);
    }
    else
    {
        if (download_state == State::DOWNLOADING)
        {
            chassert(!downloader_id.empty());
        }
        else if (download_state == State::PARTIALLY_DOWNLOADED
                 || download_state == State::EMPTY)
        {
            chassert(downloader_id.empty());
        }

        chassert(reserved_size >= downloaded_size);
        check_iterator(queue_iterator);
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
        .cache_hits = file_segment->hits_count,
        .references = static_cast<uint64_t>(file_segment.use_count()),
        .is_unbound = file_segment->is_unbound,
        .queue_entry_type = file_segment->queue_iterator ? file_segment->queue_iterator->getType() : QueueEntryType::None,
        .user_id = key_metadata->user.user_id,
        .user_weight = key_metadata->user.weight.value(),
    };
}

bool FileSegment::isDetached() const
{
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
    try
    {
        cache_writer.reset();
        remote_file_reader.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void FileSegment::detach(const FileSegmentGuard::Lock & lock, const LockedKey &)
{
    if (download_state == State::DETACHED)
        return;

    if (!downloader_id.empty())
        resetDownloaderUnlocked(lock);
    setDetachedState(lock);
}

void FileSegment::increasePriority()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentUseMicroseconds);

    if (!cache)
    {
        chassert(isDetached());
        return;
    }

    auto it = getQueueIterator();
    if (it)
    {
        if (auto cache_lock = cache->tryLockCache())
            hits_count = it->increasePriority(cache_lock);
        else
            ProfileEvents::increment(ProfileEvents::FileSegmentFailToIncreasePriority);
    }
}

FileSegmentsHolder::FileSegmentsHolder(FileSegments && file_segments_)
    : file_segments(std::move(file_segments_))
{
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments, file_segments.size());
    ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments, file_segments.size());
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentHolderCompleteMicroseconds);

    ProfileEvents::increment(ProfileEvents::FilesystemCacheUnusedHoldFileSegments, file_segments.size());
    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
        file_segment_it = completeAndPopFrontImpl();
}

FileSegments::iterator FileSegmentsHolder::completeAndPopFrontImpl()
{
    front().complete();
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheHoldFileSegments);
    return file_segments.erase(file_segments.begin());
}

FileSegment & FileSegmentsHolder::add(FileSegmentPtr && file_segment)
{
    file_segments.push_back(file_segment);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments);
    return *file_segments.back();
}

String FileSegmentsHolder::toString()
{
    String ranges;
    for (const auto & file_segment : file_segments)
    {
        if (!ranges.empty())
            ranges += ", ";
        ranges += file_segment->range().toString();
        if (file_segment->isUnbound())
            ranges += "(unbound)";
    }
    return ranges;
}

}
