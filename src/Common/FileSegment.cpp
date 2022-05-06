#include "FileSegment.h"
#include <base/getThreadId.h>
#include <Common/FileCache.h>
#include <Common/hex.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <filesystem>

namespace CurrentMetrics
{
extern const Metric CacheDetachedFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CACHE_FILE_SEGMENT_IS_DETACHED;
}

FileSegment::FileSegment(
        size_t offset_,
        size_t size_,
        const Key & key_,
        IFileCache * cache_,
        State download_state_)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
#ifndef NDEBUG
    , log(&Poco::Logger::get(fmt::format("FileSegment({}) : {}", getHexUIntLowercase(key_), range().toString())))
#else
    , log(&Poco::Logger::get("FileSegment"))
#endif
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
            break;
        }
        /// DOWNLOADING is used only for write-through caching (e.g. getOrSetDownloader() is not
        /// needed, downloader is set on file segment creation).
        case (State::DOWNLOADING):
        {
            /// On write-through cache we do not check downloader id.
            is_write_through_cache = true;
            downloader_id = getCallerId();
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state");
        }
    }
}

FileSegment::State FileSegment::state() const
{
    std::lock_guard segment_lock(mutex);
    return download_state;
}

size_t FileSegment::getDownloadOffset() const
{
    std::lock_guard segment_lock(mutex);
    return range().left + getDownloadedSize(segment_lock);
}

size_t FileSegment::getDownloadedSize() const
{
    std::lock_guard segment_lock(mutex);
    return getDownloadedSize(segment_lock);
}

size_t FileSegment::getAvailableSize() const
{
    std::lock_guard segment_lock(mutex);
    return range().size() - downloaded_size;
}

size_t FileSegment::getDownloadedSize(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    if (download_state == State::DOWNLOADED)
        return downloaded_size;

    std::lock_guard download_lock(download_mutex);
    return downloaded_size;
}

String FileSegment::getCallerId()
{
    return getCallerIdImpl();
}

String FileSegment::getCallerIdImpl()
{
    if (!CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().size == 0)
        return "None:" + toString(getThreadId());

    return CurrentThread::getQueryId().toString() + ":" + toString(getThreadId());
}

String FileSegment::getOrSetDownloader()
{
    std::lock_guard segment_lock(mutex);

    assertNotDetached(segment_lock);

    if (downloader_id.empty())
    {
        assert(download_state != State::DOWNLOADING);

        if (download_state != State::EMPTY
            && download_state != State::PARTIALLY_DOWNLOADED)
            return "None";

        downloader_id = getCallerId();
        download_state = State::DOWNLOADING;
    }
    else if (downloader_id == getCallerId())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Attempt to set the same downloader for segment {} for the second time", range().toString());

    return downloader_id;
}

void FileSegment::resetDownloader()
{
    std::lock_guard segment_lock(mutex);

    assertNotDetached(segment_lock);

    if (downloader_id.empty())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "There is no downloader");

    if (getCallerId() != downloader_id)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Downloader can be reset only by downloader");

    resetDownloaderImpl(segment_lock);
}

void FileSegment::resetDownloaderImpl(std::lock_guard<std::mutex> & segment_lock)
{
    if (downloaded_size == range().size())
        setDownloaded(segment_lock);
    else
        download_state = State::PARTIALLY_DOWNLOADED;

    downloader_id.clear();
}

String FileSegment::getDownloader() const
{
    std::lock_guard segment_lock(mutex);
    return downloader_id;
}

bool FileSegment::isDownloader() const
{
    std::lock_guard segment_lock(mutex);

    return getCallerId() == downloader_id;
}

bool FileSegment::isDownloaderImpl(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    return getCallerId() == downloader_id;
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    if (!isDownloader())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Only downloader can use remote filesystem file reader");

    return remote_file_reader;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    if (!isDownloader())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Only downloader can use remote filesystem file reader");

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::resetRemoteFileReader()
{
    if (!isDownloader())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Only downloader can use remote filesystem file reader");

    if (!remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader does not exist");

    remote_file_reader.reset();
}

void FileSegment::write(const char * from, size_t size, size_t offset_)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    if (availableSize() < size)
        throw Exception(
            ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", availableSize(), size);

    if (!isDownloader() && !is_write_through_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Only downloader can do the downloading. (CallerId: {}, DownloaderId: {})",
                        getCallerId(), downloader_id);

    if (downloaded_size == range().size())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Attempt to write {} bytes to offset: {}, but current file segment is already fully downloaded",
                        size, offset_);

    auto download_offset = range().left + downloaded_size;
    if (offset_ != download_offset)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Attempt to write {} bytes to offset: {}, but current download offset is {}",
                        size, offset_, download_offset);

    std::lock_guard detach_lock(detach_mutex);

    {
        std::lock_guard segment_lock(mutex);
        assertNotDetached(segment_lock);
    }

    if (!cache_writer)
    {
        if (downloaded_size > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cache writer was finalized (downloaded size: {}, state: {})",
                            downloaded_size, stateToString(download_state));

        auto download_path = cache->getPathInLocalCache(key(), offset());
        cache_writer = std::make_unique<WriteBufferFromFile>(download_path);
    }

    try
    {
        cache_writer->write(from, size);

        std::lock_guard download_lock(download_mutex);

        cache_writer->next();

        downloaded_size += size;
    }
    catch (Exception & e)
    {
        std::lock_guard segment_lock(mutex);

        wrapWithCacheInfo(e, "while writing into cache", segment_lock);

        setDownloadFailed(segment_lock);

        cv.notify_all();

        throw;
    }

    assert(getDownloadOffset() == offset_ + size);
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    if (is_detached)
        throwDetachedUnlocked(segment_lock);

    if (downloader_id.empty())
        return download_state;

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);

        assert(!downloader_id.empty());
        assert(downloader_id != getCallerId());

        cv.wait_for(segment_lock, std::chrono::seconds(60));
    }

    return download_state;
}

bool FileSegment::reserve(size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Zero space reservation is not allowed");

    std::lock_guard cache_lock(cache->mutex);
    std::lock_guard detach_lock(detach_mutex);

    {
        std::lock_guard segment_lock(mutex);
        assertNotDetached(segment_lock);

        auto caller_id = getCallerId();
        bool is_downloader = caller_id == downloader_id;
        if (!is_downloader && !is_write_through_cache)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Space can be reserved only by downloader (current: {}, expected: {})", caller_id, downloader_id);

        if (downloaded_size + size > range().size())
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                            "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                            size, range().toString(), downloaded_size);

        assert(reserved_size >= downloaded_size);
    }

    /**
     * It is possible to have downloaded_size < reserved_size when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */
    size_t free_space = reserved_size - downloaded_size;
    size_t size_to_reserve = size - free_space;

    bool reserved = cache->tryReserve(key(), offset(), size_to_reserve, cache_lock);

    if (reserved)
        reserved_size += size;

    return reserved;
}

void FileSegment::setDownloaded(std::lock_guard<std::mutex> & /* segment_lock */)
{
    if (is_downloaded)
        return;

    download_state = State::DOWNLOADED;
    is_downloaded = true;
    downloader_id.clear();

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::setDownloadFailed(std::lock_guard<std::mutex> & /* segment_lock */)
{
    download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
    downloader_id.clear();

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::completeBatchAndResetDownloader()
{
    std::lock_guard segment_lock(mutex);

    assertNotDetached(segment_lock);

    if (!isDownloaderImpl(segment_lock))
    {
        cv.notify_all();
        throw Exception(
            ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "File segment can be completed only by downloader ({} != {})",
            downloader_id, getCallerId());
    }

    resetDownloaderImpl(segment_lock);

    LOG_TEST(log, "Complete batch. Current downloaded size: {}", downloaded_size);

    cv.notify_all();
}

void FileSegment::complete(State state)
{
    std::lock_guard cache_lock(cache->mutex);
    std::lock_guard segment_lock(mutex);

    assertNotDetached(segment_lock);

    bool is_downloader = isDownloaderImpl(segment_lock);
    if (!is_downloader && !is_write_through_cache)
    {
        cv.notify_all();
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "File segment can be completed only by downloader or downloader's FileSegmentsHodler");
    }

    if (state != State::DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
    {
        cv.notify_all();
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Cannot complete file segment with state: {}", stateToString(state));
    }

    if (state == State::DOWNLOADED)
        setDownloaded(segment_lock);

    download_state = state;

    try
    {
        completeImpl(cache_lock, segment_lock);
    }
    catch (...)
    {
        if (!downloader_id.empty() && is_downloader)
            downloader_id.clear();

        cv.notify_all();
        throw;
    }

    cv.notify_all();
}

void FileSegment::complete(std::lock_guard<std::mutex> & cache_lock)
{
    std::lock_guard segment_lock(mutex);

    assertNotDetached(segment_lock);

    completeUnlocked(cache_lock, segment_lock);
}

void FileSegment::completeUnlocked(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock)
{
    if (download_state == State::SKIP_CACHE || is_detached)
        return;

    if (isDownloaderImpl(segment_lock)
        && download_state != State::DOWNLOADED
        && getDownloadedSize(segment_lock) == range().size())
    {
        setDownloaded(segment_lock);
    }

    assertNotDetached(segment_lock);

    if (download_state == State::DOWNLOADING || download_state == State::EMPTY)
    {
        /// Segment state can be changed from DOWNLOADING or EMPTY only if the caller is the
        /// downloader or the only owner of the segment.

        bool can_update_segment_state = isDownloaderImpl(segment_lock)
            || cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);

        if (can_update_segment_state)
            download_state = State::PARTIALLY_DOWNLOADED;
    }

    try
    {
        completeImpl(cache_lock, segment_lock);
    }
    catch (...)
    {
        if (!downloader_id.empty() && isDownloaderImpl(segment_lock))
            downloader_id.clear();

        cv.notify_all();
        throw;
    }

    cv.notify_all();
}

void FileSegment::completeImpl(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock)
{
    bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);

    if (is_last_holder
        && (download_state == State::PARTIALLY_DOWNLOADED || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION))
    {
        size_t current_downloaded_size = getDownloadedSize(segment_lock);
        if (current_downloaded_size == 0)
        {
            download_state = State::SKIP_CACHE;
            LOG_TEST(log, "Remove cell {} (nothing downloaded)", range().toString());
            cache->remove(key(), offset(), cache_lock, segment_lock);
        }
        else
        {
            /**
            * Only last holder of current file segment can resize the cell,
            * because there is an invariant that file segments returned to users
            * in FileSegmentsHolder represent a contiguous range, so we can resize
            * it only when nobody needs it.
            */
            download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
            LOG_TEST(log, "Resize cell {} to downloaded: {}", range().toString(), current_downloaded_size);
            cache->reduceSizeToDownloaded(key(), offset(), cache_lock, segment_lock);
        }

        markAsDetached(segment_lock);

        if (cache_writer)
        {
            cache_writer->finalize();
            cache_writer.reset();
            remote_file_reader.reset();
        }
    }

    if (!downloader_id.empty() && (isDownloaderImpl(segment_lock) || is_last_holder))
    {
        LOG_TEST(log, "Clearing downloader id: {}, current state: {}", downloader_id, stateToString(download_state));
        downloader_id.clear();
    }

    LOG_TEST(log, "Completed file segment: {}", getInfoForLogImpl(segment_lock));
    assertCorrectnessImpl(segment_lock);
}

String FileSegment::getInfoForLog() const
{
    std::lock_guard segment_lock(mutex);
    return getInfoForLogImpl(segment_lock);
}

String FileSegment::getInfoForLogImpl(std::lock_guard<std::mutex> & segment_lock) const
{
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSize(segment_lock) << ", ";
    info << "downloader id: " << downloader_id << ", ";
    info << "caller id: " << getCallerId();

    return info.str();
}

void FileSegment::wrapWithCacheInfo(Exception & e, const String & message, std::lock_guard<std::mutex> & segment_lock) const
{
    e.addMessage(fmt::format("{}, current cache state: {}", message, getInfoForLogImpl(segment_lock)));
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
    __builtin_unreachable();
}

void FileSegment::assertCorrectness() const
{
    std::lock_guard segment_lock(mutex);
    assertCorrectnessImpl(segment_lock);
}

void FileSegment::assertCorrectnessImpl(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    assert(downloader_id.empty() == (download_state != FileSegment::State::DOWNLOADING));
    assert(!downloader_id.empty() == (download_state == FileSegment::State::DOWNLOADING));
    assert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(cache->getPathInLocalCache(key(), offset())) > 0);
}

void FileSegment::throwDetached() const
{
    std::lock_guard segment_lock(mutex);
    throwDetachedUnlocked(segment_lock);
}

void FileSegment::throwDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    throw Exception(
        is_forcefully_detached ? ErrorCodes::CACHE_FILE_SEGMENT_IS_DETACHED : ErrorCodes::LOGICAL_ERROR,
        "Cache file segment is in detached state, operation not allowed. "
        "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
        "Please, retry. File segment info: {}", getInfoForLogImpl(segment_lock));
}


void FileSegment::assertNotDetached(std::lock_guard<std::mutex> & segment_lock) const
{
    if (is_detached)
        throwDetachedUnlocked(segment_lock);
}

void FileSegment::assertDetachedStatus(std::lock_guard<std::mutex> & segment_lock) const
{
    if (download_state != State::EMPTY && !hasFinalizedState())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cache get into inconsistent state {}", getInfoForLogImpl(segment_lock));
    }
}

FileSegmentPtr FileSegment::getSnapshot(const FileSegmentPtr & file_segment, std::lock_guard<std::mutex> & /* cache_lock */)
{
    auto snapshot = std::make_shared<FileSegment>(
        file_segment->offset(),
        file_segment->range().size(),
        file_segment->key(),
        nullptr,
        State::EMPTY);

    snapshot->hits_count = file_segment->getHitsCount();
    snapshot->ref_count = file_segment.use_count();
    snapshot->downloaded_size = file_segment->getDownloadedSize();
    snapshot->download_state = file_segment->state();

    return snapshot;
}

bool FileSegment::hasFinalizedState() const
{
    return download_state == State::DOWNLOADED
        || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        || download_state == State::SKIP_CACHE;
}

bool FileSegment::isForcefullyDetached() const
{
    std::lock_guard detach_lock(detach_mutex);
    return is_forcefully_detached;
}

void FileSegment::detach(
    bool forced_detach,
    std::lock_guard<std::mutex> & /* cache_lock */,
    std::lock_guard<std::mutex> & /* detach_lock */,
    std::lock_guard<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    markAsDetached(segment_lock);
    is_forcefully_detached = forced_detach;
    download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
    downloader_id.clear();

    LOG_TEST(log, "Detached file segment: {}", getInfoForLogImpl(segment_lock));
}

void FileSegment::markAsDetached(std::lock_guard<std::mutex> & /* segment_lock */)
{
    is_detached = true;
    CurrentMetrics::add(CurrentMetrics::CacheDetachedFileSegments);
}

FileSegment::~FileSegment()
{
    std::lock_guard segment_lock(mutex);
    if (is_detached)
        CurrentMetrics::sub(CurrentMetrics::CacheDetachedFileSegments);
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    IFileCache * cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        auto current_file_segment_it = file_segment_it;
        auto & file_segment = *current_file_segment_it;

        if (!cache)
            cache = file_segment->cache;

        try
        {
            bool is_detached = false;

            {
                std::lock_guard segment_lock(file_segment->mutex);
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

            file_segment->complete(cache_lock);

            file_segment_it = file_segments.erase(current_file_segment_it);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
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


FileSegmentRangeWriter::FileSegmentRangeWriter(
    IFileCache * cache_,
    const FileSegment::Key & key_,
    size_t max_file_segment_size_)
    : cache(cache_)
    , key(key_)
    , max_file_segment_size(max_file_segment_size_)
{
}

void FileSegmentRangeWriter::allocateFileSegment(size_t offset)
{
    std::lock_guard cache_lock(cache->mutex);

    auto file_segment = cache->setDownloading(key, offset, max_file_segment_size, cache_lock);
    current_file_segment = file_segment;

    file_segments_holder.add(std::move(file_segment));
}

bool FileSegmentRangeWriter::write(char * data, size_t size, size_t offset)
{
    /**
     * We want to write eventually some size, which is not known until the very end.
     * Therefore we allocate file segments lazily. Each file segment is assigned capacity
     * of max_file_segment_size, but reserved_size remains 0, until call to tryReserve().
     * Once current file segment is full (reached max_file_segment_size), we allocate a
     * new file segment. All allocated file segments resize in file segments holder.
     * If at the end of all writes, the last file segment is not full, then it is resized.
     */

    std::lock_guard lock(mutex);

    if (finalized)
        return false;

    if (current_file_segment.expired())
        allocateFileSegment(current_file_segment_start_offset);

    auto file_segment = current_file_segment.lock();
    if (file_segment->getAvailableSize() == 0)
    {
        file_segment->complete(FileSegment::State::DOWNLOADED);

        allocateFileSegment(current_file_segment_start_offset);
        file_segment = current_file_segment.lock();
    }

    bool reserved = file_segment->reserve(size);
    if (!reserved)
        return false;

    file_segment->write(data, size, offset);
    current_file_segment_start_offset += size;

    return true;
}

void FileSegmentRangeWriter::finalize()
{
    std::lock_guard lock(mutex);

    if (finalized)
        return;

    if (file_segments_holder.file_segments.empty() || current_file_segment.expired())
        return;

    auto file_segment = current_file_segment.lock();

    std::lock_guard cache_lock(cache->mutex);
    file_segment->complete(cache_lock);

    finalized = true;
}

FileSegmentRangeWriter::~FileSegmentRangeWriter()
{
    try
    {
        if (!finalized)
            finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void FileSegmentRangeWriter::clearDownloaded()
{
    std::lock_guard lock(mutex);

    current_file_segment.reset();

    auto & file_segments = file_segments_holder.file_segments;
    if (file_segments.empty())
        return;

    std::lock_guard cache_lock(cache->mutex);

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        auto file_segment = *file_segment_it;

        std::lock_guard segment_lock(file_segment->mutex);
        cache->remove(key, file_segment->offset(), cache_lock, segment_lock);
    }

    file_segments.clear();
}

}
