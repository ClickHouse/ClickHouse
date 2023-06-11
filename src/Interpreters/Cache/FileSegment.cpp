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

namespace CurrentMetrics
{
extern const Metric CacheDetachedFileSegments;
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
        size_t offset_,
        size_t size_,
        const Key & key_,
        FileCache * cache_,
        State download_state_,
        const CreateFileSegmentSettings & settings)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
#ifndef NDEBUG
    , log(&Poco::Logger::get(fmt::format("FileSegment({}) : {}", getHexUIntLowercase(key_), range().toString())))
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
            is_downloaded = true;
            chassert(std::filesystem::file_size(getPathInLocalCache()) == size_);
            break;
        }
        case (State::SKIP_CACHE):
        {
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can only create cell with either EMPTY, DOWNLOADED or SKIP_CACHE state");
        }
    }
}

String FileSegment::getPathInLocalCache() const
{
    chassert(cache);
    return cache->getPathInLocalCache(key(), offset(), segment_kind);
}

FileSegment::State FileSegment::state() const
{
    std::unique_lock segment_lock(mutex);
    return download_state;
}

void FileSegment::setDownloadState(State state)
{
    LOG_TEST(log, "Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getFirstNonDownloadedOffset() const
{
    std::unique_lock segment_lock(mutex);
    return getFirstNonDownloadedOffsetUnlocked(segment_lock);
}

size_t FileSegment::getFirstNonDownloadedOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    return range().left + getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getCurrentWriteOffset() const
{
    std::unique_lock segment_lock(mutex);
    return getCurrentWriteOffsetUnlocked(segment_lock);
}

size_t FileSegment::getCurrentWriteOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    return getFirstNonDownloadedOffsetUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSize() const
{
    std::unique_lock segment_lock(mutex);
    return getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSizeUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    if (download_state == State::DOWNLOADED)
        return downloaded_size;

    std::unique_lock download_lock(download_mutex);
    return downloaded_size;
}

void FileSegment::setDownloadedSize(size_t delta)
{
    std::unique_lock download_lock(download_mutex);
    setDownloadedSizeUnlocked(download_lock, delta);
}

void FileSegment::setDownloadedSizeUnlocked(std::unique_lock<std::mutex> & /* download_lock */, size_t delta)
{
    downloaded_size += delta;
    assert(downloaded_size == std::filesystem::file_size(getPathInLocalCache()));
}

bool FileSegment::isDownloaded() const
{
    std::lock_guard segment_lock(mutex);
    return is_downloaded;
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
    std::unique_lock segment_lock(mutex);
    return getDownloaderUnlocked(segment_lock);
}

String FileSegment::getDownloaderUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    return downloader_id;
}

String FileSegment::getOrSetDownloader()
{
    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);

    auto current_downloader = getDownloaderUnlocked(segment_lock);

    if (current_downloader.empty())
    {
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED;
        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(download_state);

        current_downloader = downloader_id = getCallerId();
        setDownloadState(State::DOWNLOADING);
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked([[maybe_unused]] std::unique_lock<std::mutex> & segment_lock)
{
    assert(isDownloaderUnlocked(segment_lock));
    assert(download_state == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
    /// range().size() can equal 0 in case of write-though cache.
    if (current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(segment_lock);
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED);
}

void FileSegment::resetDownloader()
{
    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked("resetDownloader", segment_lock);

    resetDownloadingStateUnlocked(segment_lock);
    resetDownloaderUnlocked(segment_lock);
}

void FileSegment::resetDownloaderUnlocked(std::unique_lock<std::mutex> & /* segment_lock */)
{
    LOG_TEST(log, "Resetting downloader from {}", downloader_id);
    downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, std::unique_lock<std::mutex> & segment_lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(segment_lock);
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
    std::unique_lock segment_lock(mutex);
    return isDownloaderUnlocked(segment_lock);
}

bool FileSegment::isDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    return getCallerId() == getDownloaderUnlocked(segment_lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("getRemoteFileReader", segment_lock);
    return remote_file_reader;
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    std::lock_guard cache_lock(cache->mutex);
    std::unique_lock segment_lock(mutex);

    if (!is_detached)
    {
        bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
        if (!downloader_id.empty() || !is_last_holder)
            return nullptr;
    }

    return std::move(remote_file_reader);
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("setRemoteFileReader", segment_lock);

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::resetRemoteFileReader()
{
    std::unique_lock segment_lock(mutex);
    assertIsDownloaderUnlocked("resetRemoteFileReader", segment_lock);

    if (!remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader does not exist");

    remote_file_reader.reset();
}

std::unique_ptr<WriteBufferFromFile> FileSegment::detachWriter()
{
    std::unique_lock segment_lock(mutex);

    if (!cache_writer)
    {
        if (detached_writer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Writer is already detached");

        auto download_path = getPathInLocalCache();
        cache_writer = std::make_unique<WriteBufferFromFile>(download_path);
    }
    detached_writer = true;
    return std::move(cache_writer);
}

void FileSegment::write(const char * from, size_t size, size_t offset)
{
    if (!size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing zero size is not allowed");

    {
        std::unique_lock segment_lock(mutex);

        assertIsDownloaderUnlocked("write", segment_lock);
        assertNotDetachedUnlocked(segment_lock);

        if (download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        size_t first_non_downloaded_offset = getFirstNonDownloadedOffsetUnlocked(segment_lock);
        if (offset != first_non_downloaded_offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Attempt to write {} bytes to offset: {}, but current write offset is {}",
                size, offset, first_non_downloaded_offset);

        size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
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

            if (detached_writer)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache writer was detached");

            auto download_path = getPathInLocalCache();
            cache_writer = std::make_unique<WriteBufferFromFile>(download_path);
        }
    }

    try
    {
        cache_writer->write(from, size);

        std::unique_lock download_lock(download_mutex);

        cache_writer->next();

        downloaded_size += size;

        chassert(std::filesystem::file_size(getPathInLocalCache()) == downloaded_size);
    }
    catch (Exception & e)
    {
        std::unique_lock segment_lock(mutex);

        wrapWithCacheInfo(e, "while writing into cache", segment_lock);

        setDownloadFailedUnlocked(segment_lock);

        cv.notify_all();

        throw;
    }

    chassert(getFirstNonDownloadedOffset() == offset + size);
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    if (is_detached)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache file segment is in detached state, operation not allowed");

    if (downloader_id.empty())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);

        chassert(!getDownloaderUnlocked(segment_lock).empty());
        chassert(!isDownloaderUnlocked(segment_lock));

        cv.wait_for(segment_lock, std::chrono::seconds(60));
    }

    return download_state;
}

bool FileSegment::reserve(size_t size_to_reserve)
{
    if (!size_to_reserve)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Zero space reservation is not allowed");

    size_t expected_downloaded_size;

    bool is_file_segment_size_exceeded;
    {
        std::unique_lock segment_lock(mutex);


        assertNotDetachedUnlocked(segment_lock);
        assertIsDownloaderUnlocked("reserve", segment_lock);

        expected_downloaded_size = getDownloadedSizeUnlocked(segment_lock);

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
        std::lock_guard cache_lock(cache->mutex);
        std::lock_guard segment_lock(mutex);

        size_to_reserve = size_to_reserve - already_reserved_size;

        if (is_unbound && is_file_segment_size_exceeded)
        {
            segment_range.right = range().left + expected_downloaded_size + size_to_reserve;
        }

        reserved = cache->tryReserve(key(), offset(), size_to_reserve, cache_lock);

        if (reserved)
            reserved_size += size_to_reserve;
    }

    return reserved;
}

void FileSegment::setDownloadedUnlocked([[maybe_unused]] std::unique_lock<std::mutex> & segment_lock)
{
    if (is_downloaded)
        return;

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }

    download_state = State::DOWNLOADED;
    is_downloaded = true;

    assert(getDownloadedSizeUnlocked(segment_lock) > 0);
    assert(std::filesystem::file_size(getPathInLocalCache()) > 0);
}

void FileSegment::setDownloadFailedUnlocked(std::unique_lock<std::mutex> & segment_lock)
{
    LOG_INFO(log, "Settings download as failed: {}", getInfoForLogUnlocked(segment_lock));

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    resetDownloaderUnlocked(segment_lock);

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::completePartAndResetDownloader()
{
    std::unique_lock segment_lock(mutex);
    completePartAndResetDownloaderUnlocked(segment_lock);
}

void FileSegment::completePartAndResetDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock)
{
    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", segment_lock);

    resetDownloadingStateUnlocked(segment_lock);
    resetDownloaderUnlocked(segment_lock);

    LOG_TEST(log, "Complete batch. ({})", getInfoForLogUnlocked(segment_lock));
    cv.notify_all();
}

void FileSegment::completeWithState(State state)
{
    std::lock_guard cache_lock(cache->mutex);
    std::unique_lock segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked("complete", segment_lock);

    if (state != State::DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
    {
        cv.notify_all();
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot complete file segment with state: {}", stateToString(state));
    }

    setDownloadState(state);
    completeBasedOnCurrentState(cache_lock, segment_lock);
}

void FileSegment::completeWithoutState()
{
    std::lock_guard cache_lock(cache->mutex);
    completeWithoutStateUnlocked(cache_lock);
}

void FileSegment::completeWithoutStateUnlocked(std::lock_guard<std::mutex> & cache_lock)
{
    std::unique_lock segment_lock(mutex);
    completeBasedOnCurrentState(cache_lock, segment_lock);
}

void FileSegment::completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::unique_lock<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    bool is_downloader = isDownloaderUnlocked(segment_lock);
    bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
    size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);

    SCOPE_EXIT({
        if (is_downloader)
        {
            cv.notify_all();
        }
    });

    LOG_TEST(
        log,
        "Complete based on current state (is_last_holder: {}, {})",
        is_last_holder, getInfoForLogUnlocked(segment_lock));

    if (is_downloader)
    {
        if (download_state == State::DOWNLOADING) /// != in case of completeWithState
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
        detach(cache_lock, segment_lock);
        setDownloadState(State::SKIP_CACHE);
        cache->remove(key(), offset(), cache_lock, segment_lock);
        return;
    }

    switch (download_state)
    {
        case State::SKIP_CACHE:
        {
            if (is_last_holder)
                cache->remove(key(), offset(), cache_lock, segment_lock);
            break;
        }
        case State::DOWNLOADED:
        {
            chassert(getDownloadedSizeUnlocked(segment_lock) == range().size());
            chassert(getDownloadedSizeUnlocked(segment_lock) == std::filesystem::file_size(getPathInLocalCache()));
            chassert(is_downloaded);
            chassert(!cache_writer);
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

                    setDownloadState(State::SKIP_CACHE);
                    cache->remove(key(), offset(), cache_lock, segment_lock);
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
                    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

                    /// Resize this file segment by creating a copy file segment with DOWNLOADED state,
                    /// but current file segment should remain PARRTIALLY_DOWNLOADED_NO_CONTINUATION and with detached state,
                    /// because otherwise an invariant that getOrSet() returns a contiguous range of file segments will be broken
                    /// (this will be crucial for other file segment holder, not for current one).
                    cache->reduceSizeToDownloaded(key(), offset(), cache_lock, segment_lock);
                }

                detachAssumeStateFinalized(segment_lock);
            }
            break;
        }
    }

    is_completed = true;
    LOG_TEST(log, "Completed file segment: {}", getInfoForLogUnlocked(segment_lock));
}

String FileSegment::getInfoForLog() const
{
    std::unique_lock segment_lock(mutex);
    return getInfoForLogUnlocked(segment_lock);
}

String FileSegment::getInfoForLogUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "key: " << key().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSizeUnlocked(segment_lock) << ", ";
    info << "reserved size: " << reserved_size << ", ";
    info << "downloader id: " << (downloader_id.empty() ? "None" : downloader_id) << ", ";
    info << "current write offset: " << getCurrentWriteOffsetUnlocked(segment_lock) << ", ";
    info << "first non-downloaded offset: " << getFirstNonDownloadedOffsetUnlocked(segment_lock) << ", ";
    info << "caller id: " << getCallerId() << ", ";
    info << "detached: " << is_detached << ", ";
    info << "kind: " << toString(segment_kind);

    return info.str();
}

void FileSegment::wrapWithCacheInfo(Exception & e, const String & message, std::unique_lock<std::mutex> & segment_lock) const
{
    e.addMessage(fmt::format("{}, current cache state: {}", message, getInfoForLogUnlocked(segment_lock)));
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
        case FileSegment::State::SKIP_CACHE:
            return "SKIP_CACHE";
    }
    UNREACHABLE();
}

void FileSegment::assertCorrectness() const
{
    std::unique_lock segment_lock(mutex);
    assertCorrectnessUnlocked(segment_lock);
}

void FileSegment::assertCorrectnessUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    auto current_downloader = getDownloaderUnlocked(segment_lock);
    chassert(current_downloader.empty() == (download_state != FileSegment::State::DOWNLOADING));
    chassert(!current_downloader.empty() == (download_state == FileSegment::State::DOWNLOADING));
    chassert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(getPathInLocalCache()) > 0);
}

void FileSegment::throwIfDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cache file segment is in detached state, operation not allowed. "
        "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
        "Please, retry. File segment info: {}", getInfoForLogUnlocked(segment_lock));
}

void FileSegment::assertNotDetached() const
{
    std::unique_lock segment_lock(mutex);
    assertNotDetachedUnlocked(segment_lock);
}

void FileSegment::assertNotDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const
{
    if (is_detached)
        throwIfDetachedUnlocked(segment_lock);
}

void FileSegment::assertDetachedStatus(std::unique_lock<std::mutex> & segment_lock) const
{
    /// Detached file segment is allowed to have only a certain subset of states.
    /// It should be either EMPTY or one of the finalized states.

    if (download_state != State::EMPTY && !hasFinalizedStateUnlocked(segment_lock))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Detached file segment has incorrect state: {}",
            getInfoForLogUnlocked(segment_lock));
    }
}

FileSegmentPtr FileSegment::getSnapshot(const FileSegmentPtr & file_segment, std::lock_guard<std::mutex> & /* cache_lock */)
{
    std::unique_lock segment_lock(file_segment->mutex);

    auto snapshot = std::make_shared<FileSegment>(
        file_segment->offset(),
        file_segment->range().size(),
        file_segment->key(),
        nullptr,
        State::EMPTY,
        CreateFileSegmentSettings{});

    snapshot->hits_count = file_segment->getHitsCount();
    snapshot->ref_count = file_segment.use_count();
    snapshot->downloaded_size = file_segment->getDownloadedSizeUnlocked(segment_lock);
    snapshot->download_state = file_segment->download_state;
    snapshot->segment_kind = file_segment->getKind();

    return snapshot;
}

bool FileSegment::hasFinalizedStateUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const
{
    return download_state == State::DOWNLOADED
        || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        || download_state == State::SKIP_CACHE;
}

bool FileSegment::isDetached() const
{
    std::unique_lock segment_lock(mutex);
    return is_detached;
}

bool FileSegment::isCompleted() const
{
    std::unique_lock segment_lock(mutex);
    return is_completed;
}

void FileSegment::detach(std::lock_guard<std::mutex> & /* cache_lock */, std::unique_lock<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    if (download_state == State::DOWNLOADING)
        resetDownloadingStateUnlocked(segment_lock);
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

    resetDownloaderUnlocked(segment_lock);
    detachAssumeStateFinalized(segment_lock);
}

void FileSegment::detachAssumeStateFinalized(std::unique_lock<std::mutex> & segment_lock)
{
    is_detached = true;
    CurrentMetrics::add(CurrentMetrics::CacheDetachedFileSegments);
    LOG_TEST(log, "Detached file segment: {}", getInfoForLogUnlocked(segment_lock));
}

FileSegment::~FileSegment()
{
    std::unique_lock segment_lock(mutex);
    if (is_detached)
        CurrentMetrics::sub(CurrentMetrics::CacheDetachedFileSegments);
}

void FileSegmentsHolder::reset()
{
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    SCOPE_EXIT({
        file_segments.clear();
    });

    FileCache * cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        auto current_file_segment_it = file_segment_it;
        auto & file_segment = *current_file_segment_it;

        if (!cache)
            cache = file_segment->cache;

        assert(cache == file_segment->cache); /// all segments should belong to the same cache

        try
        {
            bool is_detached = false;

            {
                std::unique_lock segment_lock(file_segment->mutex);
                is_detached = file_segment->isDetached(segment_lock);
                if (is_detached)
                    file_segment->assertDetachedStatus(segment_lock);
            }

            if (is_detached)
            {
                /// This file segment is not owned by cache, so it will be destructed
                /// at this point, therefore no completion required.
                file_segment_it = file_segments.erase(current_file_segment_it);
                continue;
            }

            /// File segment pointer must be reset right after calling complete() and
            /// under the same mutex, because complete() checks for segment pointers.
            std::lock_guard cache_lock(cache->mutex);

            file_segment->completeWithoutStateUnlocked(cache_lock);

            file_segment_it = file_segments.erase(current_file_segment_it);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    reset();
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
