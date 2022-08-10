#include "FileSegment.h"
#include <base/getThreadId.h>
#include <Common/hex.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <filesystem>


namespace CurrentMetrics
{
extern const Metric CacheDetachedFileSegments;
}

namespace DB
{

thread_local std::string background_caller_id;

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
    , is_persistent(settings.is_persistent) /// Not really used for now, see PR 36171
    , is_async_download(settings.is_async_download)
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
            break;
        }
        /// DOWNLOADING is used only for write-through caching (e.g. getOrSetDownloader() is not
        /// needed, downloader is set on file segment creation).
        case (State::DOWNLOADING):
        {
            downloader_id = getCallerId();
            break;
        }
        case (State::SKIP_CACHE):
        {
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state");
        }
    }

    if (is_async_download)
        async_write_state.emplace();
}

FileSegment::State FileSegment::state() const
{
    std::lock_guard segment_lock(mutex);
    return download_state;
}

String FileSegment::getPathInLocalCache() const
{
    return cache->getPathInLocalCache(key(), offset(), isPersistent());
}

size_t FileSegment::getDownloadOffset() const
{
    std::lock_guard segment_lock(mutex);
    return getDownloadOffsetUnlocked(segment_lock);
}

size_t FileSegment::getDownloadOffsetUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    return range().left + getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSize() const
{
    std::lock_guard segment_lock(mutex);
    return getDownloadedSizeUnlocked(segment_lock);
}

size_t FileSegment::getDownloadedSizeUnlocked(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    if (download_state == State::DOWNLOADED)
        return downloaded_size;

    std::lock_guard download_lock(download_mutex);
    return downloaded_size;
}

String FileSegment::getCallerId()
{
    if (!background_caller_id.empty())
        return background_caller_id;

    if (!CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().empty())
        return "None:" + toString(getThreadId());

    return std::string(CurrentThread::getQueryId()) + ":" + toString(getThreadId());
}

String FileSegment::getDownloader() const
{
    std::lock_guard segment_lock(mutex);
    return getDownloaderUnlocked(false, segment_lock);
}

String FileSegment::getDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & /* segment_lock */) const
{
    return is_internal ? background_downloader_id : downloader_id;
}

String FileSegment::getOrSetDownloader()
{
    std::lock_guard segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);

    auto current_downloader = getDownloaderUnlocked(false, segment_lock);
    auto caller_id = getCallerId();

    if (current_downloader.empty())
    {
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED;
        if (!allow_new_downloader)
            return "None";

        assert(download_state != State::DOWNLOADING);

        current_downloader = downloader_id = caller_id;
        download_state = State::DOWNLOADING;
    }

    return current_downloader;
}

void FileSegment::resetDownloader()
{
    std::lock_guard segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked(false, "resetDownloader", segment_lock);

    if (downloaded_size == range().size())
        setDownloadedUnlocked(segment_lock);

    resetDownloaderUnlocked(false, segment_lock);
}

void FileSegment::resetDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & /* segment_lock */)
{
    if (is_internal)
        background_downloader_id.clear();
    else
        downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(bool is_internal, const std::string & operation, std::lock_guard<std::mutex> & segment_lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(is_internal, segment_lock);
    LOG_TEST(log, "Downloader id: {}, caller id: {}", current_downloader, caller);

    if (caller != current_downloader)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Operation `{}` can be done only by downloader. "
            "(CallerId: {}, downloader id: {}, internal downloader id: {}, is_internal: {})",
            operation, caller, downloader_id, background_downloader_id, is_internal);
    }
}

bool FileSegment::isDownloader() const
{
    std::lock_guard segment_lock(mutex);
    return isDownloaderUnlocked(/* is_internal */false, segment_lock);
}

bool FileSegment::isDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & segment_lock) const
{
    return getCallerId() == getDownloaderUnlocked(is_internal, segment_lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    std::lock_guard segment_lock(mutex);
    assertIsDownloaderUnlocked(false, "getRemoteFileReader", segment_lock);
    return remote_file_reader;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    std::lock_guard segment_lock(mutex);
    assertIsDownloaderUnlocked(false, "setRemoteFileReader", segment_lock);

    if (remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader already exists");

    remote_file_reader = remote_file_reader_;
}

void FileSegment::resetRemoteFileReader()
{
    std::lock_guard segment_lock(mutex);
    assertIsDownloaderUnlocked(false, "resetRemoteFileReader", segment_lock);

    if (!remote_file_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remote file reader does not exist");

    remote_file_reader.reset();
}

void FileSegment::write(const char * from, size_t size, size_t offset_)
{
    if (is_async_download)
        asynchronousWrite(from, size, offset_);
    else
        synchronousWrite(from, size, offset_, false);
}

void FileSegment::asynchronousWrite(const char * from, size_t size, size_t offset)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    {
        std::lock_guard segment_lock(mutex);

        assertIsDownloaderUnlocked(false, "write", segment_lock);
        assertNotDetachedUnlocked(segment_lock);

        if (getDownloadedSizeUnlocked(segment_lock) == range().size())
        {
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Attempt to write {} bytes to offset: {}, but current file segment is already fully downloaded",
                size, offset);
        }
    }

    /// When we need to pass some data buffer, which need to be written to cache
    /// in the background, we create a threadpool task. But we need to note that
    /// each such buffer need to be written in direct order one after another within
    /// the same file segment, e.g. we can only write into file segments sequentially.
    /// Therefore the first task to set background_downloader becomes the executor and other
    /// tasks will become noop in case executor is set. On the other hand, we might
    /// add new buffers for write after previous executor reached the end of state.buffers,
    /// so instead of being noop, some of such tasks becomes a new executor.

    /// Adding new buffers, changing background_downloader_id and pulling buffers from
    /// state.buffers (by executor) is done under the same mutex, which allows to have an invariant:
    /// as long as !state.buffers.empty() (&& !state.exception) there should be a task
    /// which will set background_downloader_id and write those buffers to cache.

    {
        WriteState::Buffer buffer(size, offset);
        memcpy(buffer.data(), from, size);

        auto & state = *async_write_state;

        std::lock_guard state_lock(state.mutex);

        /// If there was an exception on a previous attempt to write data - rethrow it.
        if (state.exception)
            std::rethrow_exception(state.exception);

        LOG_TEST(log, "Current async write state has {} buffers to be written", state.buffers.size());

        if (!state.buffers.empty())
        {
            if (static_cast<int64_t>(offset) <= state.last_added_offset)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to add buffer with offset {}, but last added offset was {}",
                    offset, state.last_added_offset);
            }

            size_t expected_offset = state.buffers.back().offset + state.buffers.back().size();
            if (offset != expected_offset)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to add buffer with offset {}, but expected {} (previous offset: {}, previous size: {})",
                    offset, expected_offset, state.buffers.back().offset, state.buffers.back().size());
            }
        }

        state.buffers.emplace(std::move(buffer));
        state.last_added_offset = offset;
    }

    ThreadGroupStatusPtr running_group = CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup()
            ? CurrentThread::get().getThreadGroup()
            : MainThreadStatus::getInstance().getThreadGroup();

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    auto task = std::make_shared<std::packaged_task<void()>>(
        [this,
         executor_id = getCallerId() + "_async", /// Id for background downloader
         file_segment = shared_from_this(), /// Hold shared pointer to the file segment to mark is as used
         running_group, query_context]()
    {
        ThreadStatus thread_status;
        if (running_group)
            thread_status.attachQuery(running_group);
        if (query_context)
            thread_status.attachQueryContext(query_context);

        SCOPE_EXIT({
            thread_status.detachQuery(/* if_not_detached */true);
        });

        auto & state = *async_write_state;

        {
            std::lock_guard state_lock(state.mutex);

            /// If there was an exception on writing previous block of data, do not attempt
            /// to write later block. Once state.exception is set, each next attempt to add
            /// one more block into state.buffers will fail with that exception.
            if (state.exception)
                return;

            /// If !background_downloader.empty(), state.buffers is currently used by another
            /// execution task, so we do not need to execute anything as this
            /// executing task will continue with all state.buffers.
            if (!background_downloader_id.empty())
                return;

            background_downloader_id = background_caller_id = executor_id;
            LOG_TEST(log, "Assigned background downloader: {}", executor_id);
        }

        try
        {
            while (true)
            {
                WriteState::Buffer buffer;
                {
                    std::lock_guard state_lock(state.mutex);

                    assert(!state.exception);

                    if (state.buffers.empty())
                    {
                        LOG_TEST(log, "No buffers left, will reset downloader {}", background_downloader_id);

                        /// Resetting background_downloader must be done under state lock
                        /// along with the check state.buffers.empty()

                        completePartAndResetDownloaderImpl(true);
                        background_downloader_id.clear();

                        break;
                    }

                    buffer = std::move(state.buffers.front());
                    state.buffers.pop();
                }

                synchronousWrite(buffer.data(), buffer.size(), buffer.offset, true);

                {
                    std::lock_guard cache_lock(cache->mutex);
                    cache->background_download_max_memory_usage += buffer.size();
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);

            completePartAndResetDownloaderImpl(true);

            std::lock_guard state_lock(state.mutex);

            background_downloader_id.clear();

            if (!state.exception)
            {
                state.exception = std::current_exception();
                state.buffers = {}; /// Clear all hold memory.
            }
        }

        /// Notify that some part was written.
        /// This is needed to let other threads fall back into
        /// "can_start_from_cache" case:
        ///                      segment{k}
        /// cache:           [______|___________
        ///                         ^
        ///                         download_offset
        /// requested_range:    [__________]
        ///                     ^
        ///                     wait_offset
        cv.notify_all();
    });

    cache->getThreadPoolForAsyncWrite().scheduleOrThrow([task]{ (*task)(); });
}

void FileSegment::synchronousWrite(const char * from, size_t size, size_t offset, bool is_internal)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    {
        std::lock_guard segment_lock(mutex);
        assertIsDownloaderUnlocked(is_internal, "write", segment_lock);
        assertNotDetachedUnlocked(segment_lock);

        if (downloaded_size == range().size())
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Attempt to write {} bytes to offset: {}, but current file segment is already fully downloaded",
                size, offset);

        if (!is_internal && download_state != State::DOWNLOADING)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected DOWNLOADING state, got {}", stateToString(download_state));

        size_t download_offset = getDownloadOffsetUnlocked(segment_lock);
        if (offset != download_offset)
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Attempt to write {} bytes to offset: {}, but current download offset is {}",
                size, offset, download_offset);

        size_t available_size = availableSizeUnlocked(segment_lock);
        if (available_size < size)
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Not enough space is reserved. Available: {}, expected: {}", available_size, size);
    }

    if (!cache_writer)
    {
        if (downloaded_size > 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cache writer was finalized (downloaded size: {}, state: {})",
                downloaded_size, stateToString(download_state));

        auto download_path = getPathInLocalCache();
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

        setDownloadFailedUnlocked(segment_lock);

        cv.notify_all();

        throw;
    }

    assert(getDownloadOffset() == offset + size);
}

void FileSegment::writeInMemory(const char * from, size_t size)
{
    if (!size)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Attempt to write zero size cache file");

    std::lock_guard segment_lock(mutex);

    size_t available_size = availableSizeUnlocked(segment_lock);
    if (available_size < size)
        throw Exception(
            ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "Not enough space is reserved. Available: {}, expected: {}", available_size, size);

    assertNotDetachedUnlocked(segment_lock);

    if (cache_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache writer already initialized");

    auto download_path = getPathInLocalCache();
    cache_writer = std::make_unique<WriteBufferFromFile>(download_path, size + 1);

    try
    {
        cache_writer->write(from, size);
    }
    catch (Exception & e)
    {
        wrapWithCacheInfo(e, "while writing into cache", segment_lock);

        setDownloadFailedUnlocked(segment_lock);

        cv.notify_all();

        throw;
    }
}

size_t FileSegment::finalizeWrite()
{
    std::lock_guard segment_lock(mutex);

    if (!cache_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache writer not initialized");

    size_t size = cache_writer->offset();

    if (size == 0)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Writing zero size is not allowed");

    assertNotDetachedUnlocked(segment_lock);

    try
    {
        cache_writer->next();
    }
    catch (Exception & e)
    {
        wrapWithCacheInfo(e, "while writing into cache", segment_lock);

        setDownloadFailedUnlocked(segment_lock);

        cv.notify_all();

        throw;
    }

    downloaded_size += size;

    if (downloaded_size != range().size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected downloaded size to equal file segment size ({} == {})",
            downloaded_size, range().size());

    setDownloadedUnlocked(segment_lock);
    resetDownloaderUnlocked(false, segment_lock);

    return size;
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
        std::lock_guard cache_lock(cache->mutex);
        if (size + cache->current_background_download_memory_usage > cache->background_download_max_memory_usage)
            return false;

        cache->background_download_max_memory_usage += size;
    }

    {
        std::lock_guard segment_lock(mutex);

        assertNotDetachedUnlocked(segment_lock);
        assertIsDownloaderUnlocked(/* is_internal */false, "reserve", segment_lock);

        if (downloaded_size + size > range().size())
            throw Exception(
                ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
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
    {
        std::lock_guard segment_lock(mutex);
        reserved_size += size;
    }

    return reserved;
}

void FileSegment::setDownloadedUnlocked(std::lock_guard<std::mutex> & /* segment_lock */)
{
    if (is_downloaded)
        return;

    download_state = State::DOWNLOADED;
    is_downloaded = true;

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::setDownloadFailedUnlocked(std::lock_guard<std::mutex> & segment_lock)
{
    download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;

    resetDownloaderUnlocked(true, segment_lock);
    resetDownloaderUnlocked(false, segment_lock);

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
        remote_file_reader.reset();
    }
}

void FileSegment::completePartAndResetDownloader()
{
    completePartAndResetDownloaderImpl(false);
}

void FileSegment::completePartAndResetDownloaderImpl(bool is_internal)
{
    std::lock_guard segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked(is_internal, "completePartAndResetDownloader", segment_lock);

    if (background_downloader_id.empty() || is_internal)
    {
        if (getDownloadOffsetUnlocked(segment_lock) == range().size())
        {
            setDownloadedUnlocked(segment_lock);
        }
        else
            download_state = State::PARTIALLY_DOWNLOADED;
    }

    resetDownloaderUnlocked(is_internal, segment_lock);

    LOG_TEST(log, "Complete batch. (is_internal: {}, file segment info: {})", is_internal, getInfoForLogUnlocked(segment_lock));
    cv.notify_all();
}

void FileSegment::completeWithState(State state)
{
    std::lock_guard cache_lock(cache->mutex);
    std::lock_guard segment_lock(mutex);

    assertNotDetachedUnlocked(segment_lock);
    assertIsDownloaderUnlocked(false, "complete", segment_lock);

    if (state != State::DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED
        && state != State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
    {
        cv.notify_all();
        throw Exception(
            ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "Cannot complete file segment with state: {}", stateToString(state));
    }

    download_state = state;
    completeBasedOnCurrentState(cache_lock, segment_lock);
}

void FileSegment::completeWithoutState(std::lock_guard<std::mutex> & cache_lock)
{
    std::lock_guard segment_lock(mutex);
    completeBasedOnCurrentState(cache_lock, segment_lock);
}

void FileSegment::completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock)
{
    if (is_detached)
        return;

    SCOPE_EXIT({
        cv.notify_one();
    });

    bool is_downloader = isDownloaderUnlocked(false, segment_lock);
    bool is_last_holder = cache->isLastFileSegmentHolder(key(), offset(), cache_lock, segment_lock);
    bool can_update_segment_state = is_downloader || is_last_holder;

    LOG_TEST(log, "Complete without state (is_last_holder: {}). File segment info: {}", is_last_holder, getInfoForLogUnlocked(segment_lock));

    if (can_update_segment_state)
    {
        if (getDownloadedSizeUnlocked(segment_lock) == range().size())
            setDownloadedUnlocked(segment_lock);
        else
            download_state = State::PARTIALLY_DOWNLOADED;

        resetDownloaderUnlocked(false, segment_lock);

        if (cache_writer)
        {
            cache_writer->finalize();
            cache_writer.reset();
            remote_file_reader.reset();
        }
    }

    switch (download_state)
    {
        case State::SKIP_CACHE:
        {
            if (is_last_holder)
                cache->remove(key(), offset(), cache_lock, segment_lock);

            return;
        }
        case State::DOWNLOADED:
        {
            assert(is_downloaded);
            break;
        }
        case State::DOWNLOADING:
        case State::EMPTY:
        {
            assert(!is_last_holder);
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            if (is_last_holder)
            {
                size_t current_downloaded_size = getDownloadedSizeUnlocked(segment_lock);
                if (current_downloaded_size == 0)
                {
                    LOG_TEST(log, "Remove cell {} (nothing downloaded)", range().toString());

                    download_state = State::SKIP_CACHE;
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
                    download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
                    cache->reduceSizeToDownloaded(key(), offset(), cache_lock, segment_lock);
                }

                markAsDetached(segment_lock);
            }
            break;
        }
    }

    LOG_TEST(log, "Completed file segment: {}", getInfoForLogUnlocked(segment_lock));
    assertCorrectnessUnlocked(segment_lock);
}

String FileSegment::getInfoForLog() const
{
    std::lock_guard segment_lock(mutex);
    return getInfoForLogUnlocked(segment_lock);
}

String FileSegment::getInfoForLogUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    WriteBufferFromOwnString info;
    info << "File segment: " << range().toString() << ", ";
    info << "state: " << download_state << ", ";
    info << "downloaded size: " << getDownloadedSizeUnlocked(segment_lock) << ", ";
    info << "reserved size: " << reserved_size << ", ";
    info << "downloader id: " << downloader_id << ", ";
    info << "background downloader id: " << background_downloader_id << ", ";
    info << "caller id: " << getCallerId();

    return info.str();
}

void FileSegment::wrapWithCacheInfo(Exception & e, const String & message, std::lock_guard<std::mutex> & segment_lock) const
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
    __builtin_unreachable();
}

void FileSegment::assertCorrectnessUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    auto current_downloader = getDownloaderUnlocked(false, segment_lock);
    assert(current_downloader.empty() == (download_state != FileSegment::State::DOWNLOADING));
    assert(!current_downloader.empty() == (download_state == FileSegment::State::DOWNLOADING));
    assert(download_state != FileSegment::State::DOWNLOADED || std::filesystem::file_size(getPathInLocalCache()) > 0);
}

void FileSegment::throwIfDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cache file segment is in detached state, operation not allowed. "
        "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
        "Please, retry. File segment info: {}", getInfoForLogUnlocked(segment_lock));
}

void FileSegment::assertNotDetached() const
{
    std::lock_guard segment_lock(mutex);
    assertNotDetachedUnlocked(segment_lock);
}

void FileSegment::assertNotDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const
{
    if (is_detached)
        throwIfDetachedUnlocked(segment_lock);
}

void FileSegment::assertDetachedStatus(std::lock_guard<std::mutex> & segment_lock) const
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
    auto snapshot = std::make_shared<FileSegment>(
        file_segment->offset(),
        file_segment->range().size(),
        file_segment->key(),
        nullptr,
        State::EMPTY,
        CreateFileSegmentSettings{});

    snapshot->hits_count = file_segment->getHitsCount();
    snapshot->ref_count = file_segment.use_count();
    snapshot->downloaded_size = file_segment->getDownloadedSize();
    snapshot->download_state = file_segment->state();
    snapshot->is_persistent = file_segment->isPersistent();

    return snapshot;
}

bool FileSegment::hasFinalizedStateUnlocked(std::lock_guard<std::mutex> & /* segment_lock */) const
{
    return download_state == State::DOWNLOADED
        || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        || download_state == State::SKIP_CACHE;
}

void FileSegment::detach(std::lock_guard<std::mutex> & /* cache_lock */, std::lock_guard<std::mutex> & segment_lock)
{
    /// Now detached status can be in 2 cases, which do not do any complex logic:
    /// 1. there is only 1 remaining file segment holder
    ///    && it does not need this segment anymore
    ///    && this file segment was in cache and needs to be removed
    /// 2. in read_from_cache_if_exists_otherwise_bypass_cache case
    if (is_detached)
        return;

    markAsDetached(segment_lock);
    /// FIXME: do something with internal
    resetDownloaderUnlocked(false, segment_lock);
    download_state = State::PARTIALLY_DOWNLOADED_NO_CONTINUATION;

    LOG_TEST(log, "Detached file segment: {}", getInfoForLogUnlocked(segment_lock));
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

            file_segment->completeWithoutState(cache_lock);

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

}
