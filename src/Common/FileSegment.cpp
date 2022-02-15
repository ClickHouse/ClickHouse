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
    extern const int FILE_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
}

FileSegment::FileSegment(
        size_t offset_,
        size_t size_,
        const Key & key_,
        FileCache * cache_,
        State download_state_)
    : segment_range(offset_, offset_ + size_ - 1)
    , download_state(download_state_)
    , file_key(key_)
    , cache(cache_)
    , log(&Poco::Logger::get(fmt::format("FileSegment({}) : {}", getHexUIntLowercase(key_), range().toString())))
{
    if (download_state == State::DOWNLOADED)
        reserved_size = downloaded_size = size_;
    else if (download_state != State::EMPTY)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Can create cell with either DOWNLOADED or EMPTY state");
}

FileSegment::State FileSegment::state() const
{
    std::lock_guard segment_lock(mutex);
    return download_state;
}

size_t FileSegment::downloadOffset() const
{
    std::lock_guard segment_lock(mutex);
    return range().left + downloaded_size;
}

String FileSegment::getCallerId()
{
    if (!CurrentThread::isInitialized() || CurrentThread::getQueryId().size == 0)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Cannot use cache without query id");

    return CurrentThread::getQueryId().toString() + ":" + toString(getThreadId());
}

String FileSegment::getOrSetDownloader()
{
    std::lock_guard segment_lock(mutex);

    if (downloader_id.empty())
    {
        if (download_state != State::EMPTY
            && download_state != State::PARTIALLY_DOWNLOADED)
            throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                            "Can set downloader only for file segment with state EMPTY or PARTIALLY_DOWNLOADED, but got: {}",
                            download_state);

        downloader_id = getCallerId();
        LOG_TEST(log, "Set downloader: {}, prev state: {}", downloader_id, stateToString(download_state));
        download_state = State::DOWNLOADING;
    }
    else if (downloader_id == getCallerId())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set the same downloader for segment {} for the second time", range().toString());

    LOG_TEST(log, "Returning with downloader: {} and state: {}", downloader_id, stateToString(download_state));
    return downloader_id;
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
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Only downloader can use remote filesystem file reader");

    return remote_file_reader;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    if (!isDownloader())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Only downloader can use remote filesystem file reader");

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::write(const char * from, size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Writing zero size is not allowed");

    if (availableSize() < size)
        throw Exception(
            ErrorCodes::FILE_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", availableSize(), size);

    if (!isDownloader())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                        "Only downloader can do the downloading. (CallerId: {}, DownloaderId: {})",
                        getCallerId(), downloader_id);

    if (!cache_writer)
    {
        auto download_path = cache->path(key(), offset());
        cache_writer = std::make_unique<WriteBufferFromFile>(download_path);
    }

    try
    {
        cache_writer->write(from, size);
        cache_writer->next();
    }
    catch (...)
    {
        download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
        throw;
    }

    downloaded_size += size;
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    if (download_state == State::EMPTY)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(log, "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloader_id);

        assert(!downloader_id.empty());
        assert(downloader_id != getCallerId());

#ifndef NDEBUG
        {
            std::lock_guard cache_lock(cache->mutex);
            assert(!cache->isLastFileSegmentHolder(key(), offset(), cache_lock));
        }
#endif

        cv.wait_for(segment_lock, std::chrono::seconds(60)); /// TODO: make configurable by setting
    }

    return download_state;
}

bool FileSegment::reserve(size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Zero space reservation is not allowed");

    std::lock_guard segment_lock(mutex);

    if (downloaded_size + size > range().size())
        throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                        "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                        size, range().toString(), downloaded_size);

    auto caller_id = getCallerId();
    if (downloader_id != caller_id)
        throw Exception(ErrorCodes::FILE_CACHE_ERROR, "Space can be reserved only by downloader (current: {}, expected: {})", caller_id, downloader_id);

    assert(reserved_size >= downloaded_size);

    std::lock_guard cache_lock(cache->mutex);

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

void FileSegment::completeBatchAndResetDownloader()
{
    {
        std::lock_guard segment_lock(mutex);

        bool is_downloader = downloader_id == getCallerId();
        if (!is_downloader)
        {
            cv.notify_all();
            throw Exception(ErrorCodes::FILE_CACHE_ERROR, "File segment can be completed only by downloader");
        }

        if (downloaded_size == range().size())
        {
            download_state = State::DOWNLOADED;
            cache_writer->sync();
            cache_writer.reset();
        }
        else
            download_state = State::PARTIALLY_DOWNLOADED;

        downloader_id.clear();
        LOG_TEST(log, "Complete batch. Current downloaded size: {}", downloaded_size);
    }

    cv.notify_all();
}

void FileSegment::complete(State state, bool complete_because_of_error)
{
    {
        std::lock_guard segment_lock(mutex);

        bool is_downloader = downloader_id == getCallerId();
        if (!is_downloader)
        {
            cv.notify_all();
            throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                            "File segment can be completed only by downloader or downloader's FileSegmentsHodler");
        }

        if (state != State::DOWNLOADED
            && state != State::PARTIALLY_DOWNLOADED
            && state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
        {
            cv.notify_all();
            throw Exception(ErrorCodes::FILE_CACHE_ERROR,
                            "Cannot complete file segment with state: {}", stateToString(state));
        }

        if (complete_because_of_error)
        {
            /// Let's use a new buffer on the next attempt in this case.
            remote_file_reader.reset();
        }

        download_state = state;
        completeImpl(segment_lock);
    }

    cv.notify_all();
}

void FileSegment::complete()
{
    {
        std::lock_guard segment_lock(mutex);

        if (download_state == State::SKIP_CACHE || detached)
            return;

        if (downloaded_size == range().size() && download_state != State::DOWNLOADED)
        {
            download_state = State::DOWNLOADED;
            cache_writer->sync();
            cache_writer.reset();
        }

        if (download_state == State::DOWNLOADING || download_state == State::EMPTY)
            download_state = State::PARTIALLY_DOWNLOADED;

        completeImpl(segment_lock);
    }

    cv.notify_all();
}

void FileSegment::completeImpl(std::lock_guard<std::mutex> & /* segment_lock */)
{
    std::lock_guard cache_lock(cache->mutex);

    bool download_can_continue = false;

    if (download_state == State::PARTIALLY_DOWNLOADED
                || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
    {
        bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock);
        download_can_continue = !is_last_holder && download_state == State::PARTIALLY_DOWNLOADED;

        if (!download_can_continue)
        {
            if (!downloaded_size)
            {
                download_state = State::SKIP_CACHE;
                LOG_TEST(log, "Remove cell {} (downloaded: {})", range().toString(), downloaded_size);
                cache->remove(key(), offset(), cache_lock);

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
                LOG_TEST(log, "Resize cell {} to downloaded: {}", range().toString(), downloaded_size);
                cache->reduceSizeToDownloaded(key(), offset(), cache_lock);

                detached = true;
            }
        }
    }

    if (downloader_id == getCallerId())
    {
        LOG_TEST(log, "Clearing downloader id: {}, current state: {}", downloader_id, stateToString(download_state));
        downloader_id.clear();
    }

    if (!download_can_continue && cache_writer)
    {
        cache_writer->sync();
        cache_writer.reset();
    }

    assert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(cache->path(key(), offset())) > 0);
}

String FileSegment::getInfoForLog() const
{
    std::lock_guard segment_lock(mutex);

    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << downloaded_size << ", ";
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
    std::lock_guard lock(mutex);

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
