#include <Disks/IO/AsynchronousBoundedReadBuffer.h>

#include <cstring>

#include <Common/CurrentThread.h>

#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadStatus.h>
#include <Common/getRandomASCIIString.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}

namespace ProfileEvents
{
    extern const Event AsynchronousRemoteReadWaitMicroseconds;
    extern const Event SynchronousRemoteReadWaitMicroseconds;
    extern const Event RemoteFSSeeks;
    extern const Event RemoteFSPrefetches;
    extern const Event RemoteFSCancelledPrefetches;
    extern const Event RemoteFSUnusedPrefetches;
    extern const Event RemoteFSPrefetchedReads;
    extern const Event RemoteFSUnprefetchedReads;
    extern const Event RemoteFSPrefetchedBytes;
    extern const Event RemoteFSUnprefetchedBytes;
    extern const Event RemoteFSLazySeeks;
    extern const Event RemoteFSSeeksWithReset;
    extern const Event RemoteFSBuffers;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

AsynchronousBoundedReadBuffer::AsynchronousBoundedReadBuffer(
    ImplPtr impl_,
    IAsynchronousReader & reader_,
    size_t buffer_size_,
    size_t min_bytes_for_seek_,
    Priority priority_,
    size_t page_cache_block_size_,
    bool enable_prefetches_log_,
    AsyncReadCountersPtr async_read_counters_,
    FilesystemReadPrefetchesLogPtr prefetches_log_)
    : ReadBufferFromFileBase(0, nullptr, 0, impl_->getFileSize())
    , impl(std::move(impl_))
    , base_priority(priority_)
    , page_cache_block_size(page_cache_block_size_)
    , enable_prefetches_log(enable_prefetches_log_)
    , buffer_size(buffer_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    /// Avoid calling thread-unsafe impl->getFileName() while prefetch is in progress.
    /// If impl's getFileName() can change on the fly, our getFileName() won't reflect that.
    /// That is ok, it's not used for anything important.
    , file_name(impl->getFileName())
    , reader(reader_)
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().tryGetQueryContext() != nullptr ? CurrentThread::getQueryId() : "")
    , current_reader_id(getRandomASCIIString(8))
    , log(getLogger("AsynchronousBoundedReadBuffer"))
    , async_read_counters(async_read_counters_)
    , prefetches_log(prefetches_log_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);

    const auto * cached_impl = typeid_cast<const CachedInMemoryReadBufferFromFile *>(impl.get());
    if (cached_impl)
    {
        use_page_cache = true;
        buffer_size = page_cache_block_size;
    }
    LOG_TEST(log, "Using buffer size {} while reading {}", buffer_size, file_name);

    if (buffer_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Buffer size cannot be zero");
}

String AsynchronousBoundedReadBuffer::getInfoForLog()
{
    if (prefetch_future.valid())
        prefetch_future.wait();
    return impl->getInfoForLog();
}

bool AsynchronousBoundedReadBuffer::hasPendingDataToRead()
{
    if (read_until_position)
    {
        if (file_offset_of_buffer_end == *read_until_position) /// Everything is already read.
            return false;

        if (file_offset_of_buffer_end > *read_until_position)
        {
            /// Avoid race condition on impl->getInfoForLog().
            if (prefetch_future.valid())
                prefetch_future.wait();
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Read beyond last offset ({} > {}): file size = {}, info: {}",
                file_offset_of_buffer_end, *read_until_position, getFileSize(), impl->getInfoForLog());
        }
    }

    return true;
}

std::future<IAsynchronousReader::Result> AsynchronousBoundedReadBuffer::readAsync(char * data, size_t size, Priority priority)
{
    if (use_page_cache)
        chassert(data == nullptr);
    else
        chassert(data != nullptr);
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(*impl, async_read_counters);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = Priority{base_priority.value + priority.value};
    request.ignore = bytes_to_ignore;
    return reader.submit(request);
}

IAsynchronousReader::Result AsynchronousBoundedReadBuffer::readSync(char * data, size_t size)
{
    if (use_page_cache)
        chassert(data == nullptr);
    else
        chassert(data != nullptr);
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(*impl, async_read_counters);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.ignore = bytes_to_ignore;
    return reader.execute(request);
}

void AsynchronousBoundedReadBuffer::prefetch(Priority priority)
{
    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    last_prefetch_info.submit_time = std::chrono::system_clock::now();
    last_prefetch_info.priority = priority;

    /// Don't allocate any buffers if page cache is in use, the cache has its own buffers (PageCacheCell).
    if (!use_page_cache)
        prefetch_buffer.resize(buffer_size);

    prefetch_future = readAsync(prefetch_buffer.data(), buffer_size, priority);
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}

void AsynchronousBoundedReadBuffer::setReadUntilPosition(size_t position)
{
    if (!read_until_position || position != *read_until_position)
    {
        if (position < file_offset_of_buffer_end)
        {
            /// file has been read beyond new read until position already
            if (available() >= file_offset_of_buffer_end - position)
            {
                /// new read until position is after the current position in the working buffer
                working_buffer.resize(working_buffer.size() - (file_offset_of_buffer_end - position));
                file_offset_of_buffer_end = position;
                pos = std::min(pos, working_buffer.end());
            }
            else
            {
                if (prefetch_future.valid())
                    prefetch_future.wait();
                /// new read until position is before the current position in the working buffer
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to set read until position before already read data ({} < {}, info: {})",
                    position,
                    getPosition(),
                    impl->getInfoForLog());
            }
        }

        read_until_position = position;

        /// We must wait on future and reset the prefetch here, because otherwise there might be
        /// a race between reading the data in the threadpool and impl->setReadUntilPosition()
        /// which reinitializes internal remote read buffer (because if we have a new read range
        /// then we need a new range request) and in case of reading from cache we need to request
        /// and hold more file segment ranges from cache.
        resetPrefetch(FilesystemPrefetchState::CANCELLED_WITH_RANGE_CHANGE);
        impl->setReadUntilPosition(*read_until_position);
    }
}

void AsynchronousBoundedReadBuffer::appendToPrefetchLog(
    FilesystemPrefetchState state,
    int64_t size,
    const std::unique_ptr<Stopwatch> & execution_watch)
{
    FilesystemReadPrefetchesLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .path = impl->getFileName(),
        .offset = file_offset_of_buffer_end,
        .size = size,
        .prefetch_submit_time = last_prefetch_info.submit_time,
        .execution_watch = execution_watch ? std::optional<Stopwatch>(*execution_watch) : std::nullopt,
        .priority = last_prefetch_info.priority,
        .state = state,
        .thread_id = getThreadId(),
        .reader_id = current_reader_id,
    };

    if (prefetches_log)
        prefetches_log->add(std::move(elem));
}


bool AsynchronousBoundedReadBuffer::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    chassert(file_offset_of_buffer_end <= getFileSize());
    size_t old_file_offset_of_buffer_end = file_offset_of_buffer_end;

    IAsynchronousReader::Result result;
    if (prefetch_future.valid())
    {
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AsynchronousRemoteReadWaitMicroseconds);
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

            result = prefetch_future.get();
        }

        prefetch_future = {};
        prefetch_buffer.swap(memory);

        if (enable_prefetches_log)
            appendToPrefetchLog(FilesystemPrefetchState::USED, result.size, result.execution_watch);

        last_prefetch_info = {};

        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, result.size);
    }
    else
    {
        /// Don't allocate any buffers if page cache is in use, the cache has its own buffers (PageCacheCell).
        if (!use_page_cache)
            memory.resize(buffer_size);

        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::SynchronousRemoteReadWaitMicroseconds);
            result = readSync(memory.data(), buffer_size);
        }

        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedBytes, result.size);
    }

    bytes_to_ignore = 0;
    resetWorkingBuffer();

    chassert(use_page_cache || !result.page_cache_cell);

    /// Diagnostic asserts for the bounds-corruption class of bug tracked in
    /// `https://github.com/ClickHouse/ClickHouse/issues/104692`. The
    /// `working_buffer` we hand back to the caller MUST be a sub-range of the
    /// `Memory<>` we own (`memory.data()`, `memory.size()`), or, when the page
    /// cache is in use, a sub-range of the cache cell that backs `result.buf`.
    /// Anything else means an inner buffer (`ReadBufferFromS3`,
    /// `ReadBufferFromRemoteFSGather`) returned a corrupt range.
    chassert(result.offset <= result.size);
    if (!use_page_cache)
    {
        chassert(result.size <= memory.size());
        chassert(result.size == 0 || result.buf == memory.data());
    }

    size_t bytes_read = result.size - result.offset;
    if (bytes_read)
    {
        page_cache_cell = result.page_cache_cell;
        working_buffer = Buffer(result.buf + result.offset, result.buf + result.size);
        pos = working_buffer.begin();
    }

    file_offset_of_buffer_end = result.file_offset_of_buffer_end;

    chassert(file_offset_of_buffer_end <= getFileSize());

    if (read_until_position && (file_offset_of_buffer_end > *read_until_position))
    {
        size_t excessive_bytes_read = file_offset_of_buffer_end - *read_until_position;

        if (excessive_bytes_read > working_buffer.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "File offset moved too far: old_file_offset = {}, new_file_offset = {}, read_until_position = {}, bytes_read = {}",
                            old_file_offset_of_buffer_end, file_offset_of_buffer_end, *read_until_position, bytes_read);

        working_buffer.resize(working_buffer.size() - excessive_bytes_read);
        file_offset_of_buffer_end = *read_until_position;
    }

    return !working_buffer.empty();
}


off_t AsynchronousBoundedReadBuffer::seek(off_t offset, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    size_t new_pos = 0;
    if (whence == SEEK_SET)
    {
        chassert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = static_cast<size_t>(getPosition()) + offset;
    }
    else
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Expected SEEK_SET or SEEK_CUR as whence");
    }

    /// Position is unchanged.
    if (new_pos == static_cast<size_t>(getPosition()))
        return new_pos;

    bool read_from_prefetch = false;
    while (true)
    {
        /// The first condition implies bytes_to_ignore = 0.
        if (!working_buffer.empty() && file_offset_of_buffer_end - working_buffer.size() <= new_pos &&
            new_pos <= file_offset_of_buffer_end)
        {
            /// Position is still inside the buffer.
            /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.
            pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
            chassert(pos >= working_buffer.begin());
            chassert(pos <= working_buffer.end());

            return new_pos;
        }
        if (prefetch_future.valid())
        {
            read_from_prefetch = true;

            /// Read from prefetch buffer and recheck if the new position is valid inside.
            if (nextImpl())
                continue;
        }

        /// Prefetch is cancelled because of seek.
        if (read_from_prefetch)
        {
            ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
            if (enable_prefetches_log)
                appendToPrefetchLog(FilesystemPrefetchState::CANCELLED_WITH_SEEK, -1, nullptr);
        }

        break;
    }

    chassert(!prefetch_future.valid());

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();
    page_cache_cell = nullptr;
    bytes_to_ignore = 0;

    if (read_until_position && new_pos > *read_until_position)
    {
        if (!impl->isSeekCheap())
            ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
        file_offset_of_buffer_end = new_pos = *read_until_position; /// read_until_position is a non-included boundary.
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
        return new_pos;
    }

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */
    if (!impl->isSeekCheap() && file_offset_of_buffer_end && read_until_position && new_pos < *read_until_position
        && new_pos > file_offset_of_buffer_end && new_pos < file_offset_of_buffer_end + min_bytes_for_seek)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = new_pos - file_offset_of_buffer_end;
    }
    else
    {
        if (!impl->isSeekCheap())
            ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
        file_offset_of_buffer_end = new_pos;
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
    }

    return new_pos;
}


void AsynchronousBoundedReadBuffer::finalize()
{
    resetPrefetch(FilesystemPrefetchState::UNNEEDED);
}

AsynchronousBoundedReadBuffer::~AsynchronousBoundedReadBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void AsynchronousBoundedReadBuffer::resetPrefetch(FilesystemPrefetchState state)
{
    if (!prefetch_future.valid())
        return;

    auto result = prefetch_future.get();
    prefetch_future = {};
    last_prefetch_info = {};

    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, result.size);

    switch (state)
    {
        case FilesystemPrefetchState::UNNEEDED:
            ProfileEvents::increment(ProfileEvents::RemoteFSUnusedPrefetches);
            break;
        case FilesystemPrefetchState::CANCELLED_WITH_SEEK:
        case FilesystemPrefetchState::CANCELLED_WITH_RANGE_CHANGE:
            ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of prefetch: {}", magic_enum::enum_name(state));
    }
}

size_t AsynchronousBoundedReadBuffer::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const
{
    if (!impl->supportsReadAt())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method readBigAt() is not implemented for a given implementation");

    /// A small-object initial prefetch may be in flight even though the consumer reads via positioned
    /// reads (e.g. a small Parquet/ORC/Arrow file read through an object storage table function).
    /// readBigAt() and the sequential prefetch must not run against impl concurrently, so consume the
    /// prefetch first: serve the part of the requested range that the prefetch covers straight from the
    /// prefetched buffer, and read only the missing suffix (if any) directly.
    if (prefetch_future.valid())
    {
        IAsynchronousReader::Result result;
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AsynchronousRemoteReadWaitMicroseconds);
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
            result = prefetch_future.get();
        }
        prefetch_future = {};
        last_prefetch_info = {};

        const size_t prefetched_bytes = result.size - result.offset;
        const size_t prefetch_end = result.file_offset_of_buffer_end;
        const size_t prefetch_begin = prefetch_end - prefetched_bytes;

        /// Serve the prefix of the range that the prefetch covers.
        if (prefetched_bytes != 0 && range_begin >= prefetch_begin && range_begin < prefetch_end)
        {
            const size_t from_prefetch = std::min(n, prefetch_end - range_begin);
            memcpy(to, result.buf + result.offset + (range_begin - prefetch_begin), from_prefetch);
            ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);
            ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, from_prefetch);

            if (from_prefetch == n)
            {
                if (progress_callback)
                    progress_callback(n);
                return n;
            }

            /// Read the missing suffix directly. impl->readBigAt reports progress relative to its own
            /// request (starting from 0), but the progress reported for the whole readBigAt must stay
            /// cumulative and monotonic (e.g. ParallelReadBuffer::on_progress ignores non-increasing
            /// values), so shift the suffix progress by the prefix already served.
            std::function<bool(size_t)> suffix_progress;
            if (progress_callback)
                suffix_progress = [&](size_t copied) { return progress_callback(from_prefetch + copied); };

            return from_prefetch
                + impl->readBigAt(to + from_prefetch, n - from_prefetch, range_begin + from_prefetch, suffix_progress);
        }

        /// The prefetched range does not cover the head of the request; drop it and read directly.
        ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
    }

    return impl->readBigAt(to, n, range_begin, progress_callback);
}

std::optional<Field> AsynchronousBoundedReadBuffer::getMetadata(const String & name) const
{
    if (auto * provider = dynamic_cast<IReadBufferMetadataProvider *>(impl.get()))
        return provider->getMetadata(name);
    return std::nullopt;
}

}
