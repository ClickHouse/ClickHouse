#include "FileSegment.h"
#include <base/getThreadId.h>
#include <Common/FileCache.h>
#include <Common/hex.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
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
    if (download_state == State::DOWNLOADED)
        reserved_size = downloaded_size = size_;
    else if (download_state != State::EMPTY)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Can create cell with either DOWNLOADED or EMPTY state");
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

size_t FileSegment::getDownloadedSize(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    if (download_state == State::DOWNLOADED)
        return downloaded_size;

    std::lock_guard download_lock(download_mutex);
    return downloaded_size;
}

String FileSegment::getCallerId()
{
    return getCallerIdImpl(false);
}

String FileSegment::getCallerIdImpl(bool allow_non_strict_checking)
{
    if (IFileCache::shouldBypassCache())
    {
        /// getCallerId() can be called from completeImpl(), which can be called from complete().
        /// complete() is called from destructor of CachedReadBufferFromRemoteFS when there is no query id anymore.
        /// Allow non strict checking in this case. This works correctly as if getCallerIdImpl() is called from destructor,
        /// then we know that caller is not a downloader, because downloader is reset each nextImpl() call either
        /// manually or via SCOPE_EXIT.

        if (allow_non_strict_checking)
            return "None";

        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cannot use cache without query id");
    }

    return CurrentThread::getQueryId().toString() + ":" + toString(getThreadId());
}

String FileSegment::getOrSetDownloader()
{
    std::lock_guard segment_lock(mutex);

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
    LOG_TEST(log, "Checking for current downloader. Caller: {}, downloader: {}, current state: {}", getCallerId(), downloader_id, stateToString(download_state));
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

void FileSegment::write(const char * from, size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    if (availableSize() < size)
        throw Exception(
            ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", availableSize(), size);

    if (!isDownloader())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Only downloader can do the downloading. (CallerId: {}, DownloaderId: {})",
                        getCallerId(), downloader_id);

    if (!cache_writer)
    {
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
    catch (...)
    {
        std::lock_guard segment_lock(mutex);

        LOG_ERROR(log, "Failed to write to cache. File segment info: {}", getInfoForLog());

        download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;

        cache_writer->finalize();
        cache_writer.reset();

        throw;
    }
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

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

    {
        std::lock_guard segment_lock(mutex);

        auto caller_id = getCallerId();
        if (downloader_id != caller_id)
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Space can be reserved only by downloader (current: {}, expected: {})", caller_id, downloader_id);

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

    std::lock_guard cache_lock(cache->mutex);
    bool reserved = cache->tryReserve(key(), offset(), size_to_reserve, cache_lock);

    if (reserved)
        reserved_size += size;

    return reserved;
}

void FileSegment::setDownloaded(std::lock_guard<std::mutex> & /* segment_lock */)
{
    download_state = State::DOWNLOADED;
    is_downloaded = true;

    assert(cache_writer);
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

    bool is_downloader = downloader_id == getCallerId();
    if (!is_downloader)
    {
        cv.notify_all();
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "File segment can be completed only by downloader");
    }

    resetDownloaderImpl(segment_lock);

    LOG_TEST(log, "Complete batch. Current downloaded size: {}", downloaded_size);

    cv.notify_all();
}

void FileSegment::complete(State state)
{
    {
        std::lock_guard segment_lock(mutex);

        bool is_downloader = downloader_id == getCallerId();
        if (!is_downloader)
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

        download_state = state;
    }

    completeImpl();
    cv.notify_all();
}

void FileSegment::complete()
{
    {
        std::lock_guard segment_lock(mutex);

        if (download_state == State::SKIP_CACHE || detached)
            return;

        if (download_state != State::DOWNLOADED && getDownloadedSize(segment_lock) == range().size())
            setDownloaded(segment_lock);

        if (download_state == State::DOWNLOADING || download_state == State::EMPTY)
            download_state = State::PARTIALLY_DOWNLOADED;
    }

    completeImpl(true);
    cv.notify_all();
}

void FileSegment::completeImpl(bool allow_non_strict_checking)
{
    /// cache lock is always taken before segment lock.
    std::lock_guard cache_lock(cache->mutex);
    std::lock_guard segment_lock(mutex);

    bool download_can_continue = false;

    if (download_state == State::PARTIALLY_DOWNLOADED
                || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
    {
        bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
        download_can_continue = !is_last_holder && download_state == State::PARTIALLY_DOWNLOADED;

        if (!download_can_continue)
        {
            size_t current_downloaded_size = getDownloadedSize(segment_lock);
            if (current_downloaded_size == 0)
            {
                download_state = State::SKIP_CACHE;
                LOG_TEST(log, "Remove cell {} (nothing downloaded)", range().toString());
                cache->remove(key(), offset(), cache_lock, segment_lock);

                detached = true;
            }
            else if (is_last_holder)
            {
                /**
                * Only last holder of current file segment can resize the cell,
                * because there is an invariant that file segments returned to users
                * in FileSegmentsHolder represent a contiguous range, so we can resize
                * it only when nobody needs it.
                */
                LOG_TEST(log, "Resize cell {} to downloaded: {}", range().toString(), current_downloaded_size);
                cache->reduceSizeToDownloaded(key(), offset(), cache_lock, segment_lock);

                detached = true;
            }
        }
    }

    if (!downloader_id.empty() && downloader_id == getCallerIdImpl(allow_non_strict_checking))
    {
        LOG_TEST(log, "Clearing downloader id: {}, current state: {}", downloader_id, stateToString(download_state));
        downloader_id.clear();
    }

    if (!download_can_continue && cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }

    assert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(cache->getPathInLocalCache(key(), offset())) > 0);
}

String FileSegment::getInfoForLog() const
{
    std::lock_guard segment_lock(mutex);

    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSize(segment_lock) << ", ";
    info << "downloader id: " << downloader_id << ", ";
    info << "caller id: " << getCallerId();

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
        case FileSegment::State::SKIP_CACHE:
            return "SKIP_CACHE";
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


}
