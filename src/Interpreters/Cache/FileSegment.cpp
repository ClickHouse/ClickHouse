#include "FileSegment.h"

#include <base/getThreadId.h>
#include <Common/scope_guard_safe.h>
#include <Common/hex.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <filesystem>

#include <magic_enum.hpp>

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
        size_t offset_,
        size_t size_,
        const Key & key_,
        std::weak_ptr<KeyMetadata> key_metadata_,
        FileCache * cache_,
        State download_state_,
        const CreateFileSegmentSettings & settings)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , key_metadata(std::move(key_metadata_))
    , file_key(key_)
    , file_path(cache_->getPathInLocalCache(key(), offset(), settings.kind))
    , cache(cache_)
#ifndef NDEBUG
    , log(&Poco::Logger::get(fmt::format("FileSegment({}) : {}", key_.toString(), range().toString())))
#else
    , log(&Poco::Logger::get("FileSegment"))
#endif
    , segment_kind(settings.kind)
    , is_unbound(settings.unbounded)
{
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (download_state)
    {
        /// EMPTY is used when file segment is not in cache and
        /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
        case (State::EMPTY):
        {
            break;
        }
        /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
        /// or on reduceSizeToDownloaded() -- when file segment object is updated.
        case (State::DOWNLOADED):
        {
            reserved_size = downloaded_size = size_;
            is_completed = true;
            chassert(std::filesystem::file_size(file_path) == size_);
            break;
        }
        case (State::DETACHED):
        {
            is_completed = true;
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can only create cell with either EMPTY, DOWNLOADED or DETACHED state");
        }
    }
}

FileSegment::State FileSegment::state() const
{
    auto lock = segment_guard.lock();
    return download_state;
}

void FileSegment::setDownloadState(State state, const FileSegmentGuard::Lock &)
{
    LOG_TEST(log, "Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getReservedSize() const
{
    auto lock = segment_guard.lock();
    return reserved_size;
}

size_t FileSegment::getFirstNonDownloadedOffset(bool sync) const
{
    return range().left + getDownloadedSize(sync);
}

size_t FileSegment::getCurrentWriteOffset(bool sync) const
{
    return getFirstNonDownloadedOffset(sync);
}

size_t FileSegment::getDownloadedSize(bool sync) const
{
    if (sync)
    {
        std::lock_guard lock(download_mutex);
        return downloaded_size;
    }
    return downloaded_size;
}

void FileSegment::setDownloadedSize(size_t delta)
{
    auto lock = segment_guard.lock();
    setDownloadedSizeUnlocked(delta, lock);
}

void FileSegment::setDownloadedSizeUnlocked(size_t delta, const FileSegmentGuard::Lock &)
{
    downloaded_size += delta;
    assert(downloaded_size == std::filesystem::file_size(getPathInLocalCache()));
}

bool FileSegment::isDownloaded() const
{
    auto lock = segment_guard.lock();
    return download_state == State::DOWNLOADED;
}

String FileSegment::getCallerId()
{
    if (!CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().empty())
        return "None:" + toString(getThreadId());

    return std::string(CurrentThread::getQueryId()) + ":" + toString(getThreadId());
}

String FileSegment::getDownloader() const
{
    auto lock = segment_guard.lock();
    return getDownloaderUnlocked(lock);
}

String FileSegment::getDownloaderUnlocked(const FileSegmentGuard::Lock &) const
{
    return downloader_id;
}

String FileSegment::getOrSetDownloader()
{
    auto lock = segment_guard.lock();

    assertNotDetachedUnlocked(lock);

    auto current_downloader = getDownloaderUnlocked(lock);

    if (current_downloader.empty())
    {
        const auto caller_id = getCallerId();
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED || !caller_id.starts_with("None");
        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(download_state);

        current_downloader = downloader_id = caller_id;
        setDownloadState(State::DOWNLOADING, lock);
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked([[maybe_unused]] const FileSegmentGuard::Lock & lock)
{
    assert(isDownloaderUnlocked(lock));
    assert(download_state == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSize(true);
    /// range().size() can equal 0 in case of write-though cache.
    if (current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(lock);
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);
}

void FileSegment::resetDownloader()
{
    auto lock = segment_guard.lock();

    assertNotDetachedUnlocked(lock);
    assertIsDownloaderUnlocked("resetDownloader", lock);

    resetDownloadingStateUnlocked(lock);
    resetDownloaderUnlocked(lock);
}

void FileSegment::resetDownloaderUnlocked(const FileSegmentGuard::Lock &)
{
    LOG_TEST(log, "Resetting downloader from {}", downloader_id);
    downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock & lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(lock);
    LOG_TEST(log, "Downloader id: {}, caller id: {}", current_downloader, caller);

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
    auto lock = segment_guard.lock();
    return isDownloaderUnlocked(lock);
}

bool FileSegment::isDownloaderUnlocked(const FileSegmentGuard::Lock & lock) const
{
    return getCallerId() == getDownloaderUnlocked(lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    auto lock = segment_guard.lock();
    assertIsDownloaderUnlocked("getRemoteFileReader", lock);
    return remote_file_reader;
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    auto locked_key = createLockedKey(false);
    if (!locked_key)
    {
        assert(isDetached());
        return std::move(remote_file_reader);
    }

    auto segment_lock = segment_guard.lock();

    assert(download_state != State::DETACHED);

    bool is_last_holder = locked_key->isLastHolder(offset());
    if (!downloader_id.empty() || !is_last_holder)
        return nullptr;

    return std::move(remote_file_reader);
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    auto lock = segment_guard.lock();
    assertIsDownloaderUnlocked("setRemoteFileReader", lock);

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::resetRemoteFileReader()
{
    auto lock = segment_guard.lock();
    assertIsDownloaderUnlocked("resetRemoteFileReader", lock);

    if (!remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader does not exist");

    remote_file_reader.reset();
}

void FileSegment::write(const char * from, size_t size, size_t offset)
{
    if (!size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing zero size is not allowed");

    {
        auto lock = segment_guard.lock();

        assertIsDownloaderUnlocked("write", lock);
        assertNotDetachedUnlocked(lock);

        if (download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        size_t first_non_downloaded_offset = getFirstNonDownloadedOffset(false);
        if (offset != first_non_downloaded_offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to write {} bytes to offset: {}, but current write offset is {}",
                size, offset, first_non_downloaded_offset);

        size_t current_downloaded_size = getDownloadedSize(false);
        chassert(reserved_size >= current_downloaded_size);
        size_t free_reserved_size = reserved_size - current_downloaded_size;

        if (free_reserved_size < size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (current_downloaded_size == range().size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is already fully downloaded");

        if (!cache_writer)
        {
            if (current_downloaded_size > 0)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cache writer was finalized (downloaded size: {}, state: {})",
                    current_downloaded_size, stateToString(download_state));

            cache_writer = std::make_unique<WriteBufferFromFile>(file_path);
        }
    }

    try
    {
        cache_writer->write(from, size);

        std::lock_guard lock(download_mutex);

        cache_writer->next();

        downloaded_size += size;

        chassert(std::filesystem::file_size(file_path) == downloaded_size);
    }
    catch (Exception & e)
    {
        auto lock = segment_guard.lock();

        wrapWithCacheInfo(e, "while writing into cache", lock);

        setDownloadFailedUnlocked(lock);

        cv.notify_all();

        throw;
    }

    chassert(getFirstNonDownloadedOffset(false) == offset + size);
}

FileSegment::State FileSegment::wait()
{
    auto lock = segment_guard.lock();

    if (downloader_id.empty())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);

        chassert(!getDownloaderUnlocked(lock).empty());
        chassert(!isDownloaderUnlocked(lock));

        cv.wait_for(lock, std::chrono::seconds(60));
    }

    return download_state;
}

LockedKeyPtr FileSegment::createLockedKey(bool assert_exists) const
{
    if (key_metadata.expired())
    {
        if (assert_exists)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot lock key");
        else
            return nullptr;
    }
    return cache->createLockedKey(key(), key_metadata.lock());
}

bool FileSegment::reserve(size_t size_to_reserve)
{
    if (!size_to_reserve)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Zero space reservation is not allowed");

    size_t expected_downloaded_size;

    bool is_file_segment_size_exceeded;
    {
        auto lock = segment_guard.lock();

        LOG_TRACE(log, "Try reserve for {}", getInfoForLogUnlocked(lock));

        assertNotDetachedUnlocked(lock);
        assertIsDownloaderUnlocked("reserve", lock);

        expected_downloaded_size = getDownloadedSize(false);

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

    bool reserved = already_reserved_size >= size_to_reserve;
    if (!reserved)
    {
        size_to_reserve = size_to_reserve - already_reserved_size;

        /// This (resizable file segments) is allowed only for single threaded use of file segment.
        /// Currently it is used only for temporary files through cache.
        if (is_unbound && is_file_segment_size_exceeded)
            segment_range.right = range().left + expected_downloaded_size + size_to_reserve;

        reserved = cache->tryReserve(key(), offset(), size_to_reserve, key_metadata.lock());
        if (reserved)
        {
            /// No lock is required because reserved size is always
            /// mananaged (read/modified) by the downloader only
            /// or in isLastHolder() case.
            /// It is made atomic because of getInfoForLog.
            reserved_size += size_to_reserve;
        }
    }

    return reserved;
}

void FileSegment::setDownloadedUnlocked([[maybe_unused]] const FileSegmentGuard::Lock & lock)
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

    chassert(getDownloadedSize(false) > 0);
    chassert(std::filesystem::file_size(file_path) > 0);
}

void FileSegment::setDownloadFailedUnlocked(const FileSegmentGuard::Lock & lock)
{
    LOG_INFO(log, "Settings download as failed: {}", getInfoForLogUnlocked(lock));

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lock);

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::completePartAndResetDownloader()
{
    auto lock = segment_guard.lock();
    completePartAndResetDownloaderUnlocked(lock);
}

void FileSegment::completePartAndResetDownloaderUnlocked(const FileSegmentGuard::Lock & lock)
{
    assertNotDetachedUnlocked(lock);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", lock);

    resetDownloadingStateUnlocked(lock);
    resetDownloaderUnlocked(lock);

    LOG_TEST(log, "Complete batch. ({})", getInfoForLogUnlocked(lock));
    cv.notify_all();
}

void FileSegment::setBroken()
{
    auto lock = segment_guard.lock();
    assertNotDetachedUnlocked(lock);
    assertIsDownloaderUnlocked("setBroken", lock);
    resetDownloadingStateUnlocked(lock);
    resetDownloaderUnlocked(lock);
}

void FileSegment::complete()
{
    auto cache_lock = cache->cacheLock();
    auto locked_key = createLockedKey();
    return completeUnlocked(*locked_key, cache_lock);
}

void FileSegment::completeUnlocked(LockedKey & locked_key, const CacheGuard::Lock & cache_lock)
{
    auto segment_lock = segment_guard.lock();

    if (download_state == State::DETACHED)
    {
        assertDetachedStatus(segment_lock);
        return;
    }

    if (is_completed)
        return;

    const bool is_downloader = isDownloaderUnlocked(segment_lock);
    const bool is_last_holder = locked_key.isLastHolder(offset());
    const size_t current_downloaded_size = getDownloadedSize(true);

    SCOPE_EXIT({
        if (is_downloader)
        {
            cv.notify_all();
        }
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

    if (cache_writer && (is_downloader || is_last_holder))
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }

    if (segment_kind == FileSegmentKind::Temporary && is_last_holder)
    {
        LOG_TEST(log, "Removing temporary file segment: {}", getInfoForLogUnlocked(segment_lock));
        detach(segment_lock, locked_key);
        setDownloadState(State::DETACHED, segment_lock);
        locked_key.remove(offset(), segment_lock, cache_lock);
        return;
    }

    switch (download_state)
    {
        case State::DOWNLOADED:
        {
            chassert(current_downloaded_size == range().size());
            chassert(current_downloaded_size == std::filesystem::file_size(file_path));
            chassert(!cache_writer);

            is_completed = true;
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!is_last_holder);
            break;
        }
        case State::EMPTY:
        case State::PARTIALLY_DOWNLOADED:
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            if (is_last_holder)
            {
                if (current_downloaded_size == 0)
                {
                    LOG_TEST(log, "Remove cell {} (nothing downloaded)", range().toString());

                    setDownloadState(State::DETACHED, segment_lock);
                    locked_key.remove(offset(), segment_lock, cache_lock);
                }
                else
                {
                    LOG_TEST(log, "Resize cell {} to downloaded: {}", range().toString(), current_downloaded_size);

                    /**
                    * Only last holder of current file segment can resize the cell,
                    * because there is an invariant that file segments returned to users
                    * in FileSegmentsHolder represent a contiguous range, so we can resize
                    * it only when nobody needs it.
                    */
                    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, segment_lock);

                    /// Resize this file segment by creating a copy file segment with DOWNLOADED state,
                    /// but current file segment should remain PARRTIALLY_DOWNLOADED_NO_CONTINUATION and with detached state,
                    /// because otherwise an invariant that getOrSet() returns a contiguous range of file segments will be broken
                    /// (this will be crucial for other file segment holder, not for current one).
                    locked_key.reduceSizeToDownloaded(offset(), segment_lock, cache_lock);
                }

                detachAssumeStateFinalized(segment_lock);
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
    auto lock = segment_guard.lock();
    return getInfoForLogUnlocked(lock);
}

String FileSegment::getInfoForLogUnlocked(const FileSegmentGuard::Lock &) const
{
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "key: " << key().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSize(false) << ", ";
    info << "reserved size: " << reserved_size.load() << ", ";
    info << "downloader id: " << (downloader_id.empty() ? "None" : downloader_id) << ", ";
    info << "current write offset: " << getCurrentWriteOffset(false) << ", ";
    info << "first non-downloaded offset: " << getFirstNonDownloadedOffset(false) << ", ";
    info << "caller id: " << getCallerId() << ", ";
    info << "kind: " << toString(segment_kind);

    return info.str();
}

void FileSegment::wrapWithCacheInfo(Exception & e, const String & message, const FileSegmentGuard::Lock & lock) const
{
    e.addMessage(fmt::format("{}, current cache state: {}", message, getInfoForLogUnlocked(lock)));
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
    UNREACHABLE();
}

void FileSegment::assertCorrectness() const
{
    auto lock = segment_guard.lock();
    assertCorrectnessUnlocked(lock);
}

void FileSegment::assertCorrectnessUnlocked(const FileSegmentGuard::Lock & lock) const
{
    auto current_downloader = getDownloaderUnlocked(lock);
    chassert(current_downloader.empty() == (download_state != FileSegment::State::DOWNLOADING));
    chassert(!current_downloader.empty() == (download_state == FileSegment::State::DOWNLOADING));
    chassert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(file_path) > 0);
}

void FileSegment::throwIfDetachedUnlocked(const FileSegmentGuard::Lock & lock) const
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cache file segment is in detached state, operation not allowed. "
        "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
        "Please, retry. File segment info: {}", getInfoForLogUnlocked(lock));
}

void FileSegment::assertNotDetached() const
{
    auto lock = segment_guard.lock();
    assertNotDetachedUnlocked(lock);
}

void FileSegment::assertNotDetachedUnlocked(const FileSegmentGuard::Lock & lock) const
{
    if (download_state == State::DETACHED)
        throwIfDetachedUnlocked(lock);
}

void FileSegment::assertDetachedStatus(const FileSegmentGuard::Lock & lock) const
{
    /// Detached file segment is allowed to have only a certain subset of states.
    /// It should be either EMPTY or one of the finalized states.

    if (download_state != State::EMPTY
        && download_state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        && !hasFinalizedStateUnlocked(lock))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Detached file segment has incorrect state: {}",
            getInfoForLogUnlocked(lock));
    }
}

FileSegmentPtr FileSegment::getSnapshot(const FileSegmentPtr & file_segment)
{
    auto lock = file_segment->segment_guard.lock();

    auto snapshot = std::make_shared<FileSegment>(
        file_segment->offset(),
        file_segment->range().size(),
        file_segment->key(),
        std::weak_ptr<KeyMetadata>(),
        file_segment->cache,
        State::DETACHED,
        CreateFileSegmentSettings(file_segment->getKind()));

    snapshot->hits_count = file_segment->getHitsCount();
    snapshot->downloaded_size = file_segment->getDownloadedSize(false);
    snapshot->download_state = file_segment->download_state;

    snapshot->ref_count = file_segment.use_count();
    snapshot->is_completed = true;

    return snapshot;
}

bool FileSegment::hasFinalizedStateUnlocked(const FileSegmentGuard::Lock &) const
{
    return download_state == State::DOWNLOADED
        || download_state == State::DETACHED;
}

bool FileSegment::isDetached() const
{
    auto lock = segment_guard.lock();
    return download_state == State::DETACHED;
}

void FileSegment::detach(const FileSegmentGuard::Lock & lock, const LockedKey &)
{
    if (download_state == State::DETACHED)
        return;

    setDownloadState(State::DETACHED, lock);
    resetDownloaderUnlocked(lock);

    detachAssumeStateFinalized(lock);
}

void FileSegment::detachAssumeStateFinalized(const FileSegmentGuard::Lock & lock)
{
    is_completed = true;
    key_metadata.reset();
    LOG_TEST(log, "Detached file segment: {}", getInfoForLogUnlocked(lock));
}

FileSegments::iterator FileSegmentsHolder::completeAndPopFrontImpl()
{
    auto & file_segment = front();
    if (file_segment.isDetached())
    {
        return file_segments.erase(file_segments.begin());
    }

    auto cache_lock = file_segment.cache->cacheLock();

    /// File segment pointer must be reset right after calling complete() and
    /// under the same mutex, because complete() checks for segment pointers.
    auto locked_key = file_segment.createLockedKey(/* assert_exists */false);
    if (locked_key)
    {
        auto queue_iter = locked_key->getKeyMetadata()->tryGetByOffset(file_segment.offset())->queue_iterator;
        if (queue_iter)
            LockedCachePriorityIterator(cache_lock, queue_iter).use();

        if (!file_segment.isCompleted())
        {
            file_segment.completeUnlocked(*locked_key, cache_lock);
        }
    }

    return file_segments.erase(file_segments.begin());
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
        file_segment_it = completeAndPopFrontImpl();
}

String FileSegmentsHolder::toString()
{
    String ranges;
    for (const auto & file_segment : file_segments)
    {
        if (!ranges.empty())
            ranges += ", ";
        ranges += file_segment->range().toString();
    }
    return ranges;
}

}
