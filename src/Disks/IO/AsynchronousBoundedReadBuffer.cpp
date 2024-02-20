#include "AsynchronousBoundedReadBuffer.h"

#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
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
}

AsynchronousBoundedReadBuffer::AsynchronousBoundedReadBuffer(
    ImplPtr impl_,
    IAsynchronousReader & reader_,
    const ReadSettings & settings_,
    AsyncReadCountersPtr async_read_counters_,
    FilesystemReadPrefetchesLogPtr prefetches_log_)
    : ReadBufferFromFileBase(chooseBufferSizeForRemoteReading(settings_, impl_->getFileSize()), nullptr, 0)
    , impl(std::move(impl_))
    , read_settings(settings_)
    , reader(reader_)
    , prefetch_buffer(chooseBufferSizeForRemoteReading(read_settings, impl->getFileSize()))
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr ? CurrentThread::getQueryId() : "")
    , current_reader_id(getRandomASCIIString(8))
    , log(&Poco::Logger::get("AsynchronousBoundedReadBuffer"))
    , async_read_counters(async_read_counters_)
    , prefetches_log(prefetches_log_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}

bool AsynchronousBoundedReadBuffer::hasPendingDataToRead()
{
    if (read_until_position)
    {
        if (file_offset_of_buffer_end == *read_until_position) /// Everything is already read.
            return false;

        if (file_offset_of_buffer_end > *read_until_position)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Read beyond last offset ({} > {}): file size = {}, info: {}",
                file_offset_of_buffer_end, *read_until_position, impl->getFileSize(), impl->getInfoForLog());
    }

    return true;
}

std::future<IAsynchronousReader::Result>
AsynchronousBoundedReadBuffer::asyncReadInto(char * data, size_t size, Priority priority)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(*impl, async_read_counters);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = Priority{read_settings.priority.value + priority.value};
    request.ignore = bytes_to_ignore;
    return reader.submit(request);
}

void AsynchronousBoundedReadBuffer::prefetch(Priority priority)
{
    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    last_prefetch_info.submit_time = std::chrono::system_clock::now();
    last_prefetch_info.priority = priority;

    chassert(prefetch_buffer.size() == chooseBufferSizeForRemoteReading(read_settings, impl->getFileSize()));
    prefetch_future = asyncReadInto(prefetch_buffer.data(), prefetch_buffer.size(), priority);
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
                file_offset_of_buffer_end = position;
                working_buffer.resize(working_buffer.size() - (file_offset_of_buffer_end - position));
            }
            else
            {
                /// new read until position is before the current position in the working buffer
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to set read until position before already read data ({} > {}, info: {})",
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

    chassert(file_offset_of_buffer_end <= impl->getFileSize());
    size_t old_file_offset_of_buffer_end = file_offset_of_buffer_end;

    size_t size, offset;
    if (prefetch_future.valid())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AsynchronousRemoteReadWaitMicroseconds);
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

        auto result = prefetch_future.get();
        size = result.size;
        offset = result.offset;

        prefetch_future = {};
        prefetch_buffer.swap(memory);

        if (read_settings.enable_filesystem_read_prefetches_log)
        {
            appendToPrefetchLog(FilesystemPrefetchState::USED, size, result.execution_watch);
        }
        last_prefetch_info = {};

        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, size);
    }
    else
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::SynchronousRemoteReadWaitMicroseconds);

        chassert(memory.size() == chooseBufferSizeForRemoteReading(read_settings, impl->getFileSize()));
        std::tie(size, offset) = impl->readInto(memory.data(), memory.size(), file_offset_of_buffer_end, bytes_to_ignore);

        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedBytes, size);
    }

    bytes_to_ignore = 0;
    resetWorkingBuffer();

    chassert(size >= offset);
    size_t bytes_read = size - offset;
    if (bytes_read)
    {
        /// Adjust the working buffer so that it ignores `offset` bytes.
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = Buffer(memory.data() + offset, memory.data() + size);
        pos = working_buffer.begin();
    }

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();

    /// In case of multiple files for the same file in clickhouse (i.e. log family)
    /// file_offset_of_buffer_end will not match getImplementationBufferOffset()
    /// so we use [impl->getImplementationBufferOffset(), impl->getFileSize()]
    chassert(file_offset_of_buffer_end >= impl->getFileOffsetOfBufferEnd());
    chassert(file_offset_of_buffer_end <= impl->getFileSize());

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

    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
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
            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return new_pos;
        }
        else if (prefetch_future.valid())
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
            if (read_settings.enable_filesystem_read_prefetches_log)
            {
                appendToPrefetchLog(FilesystemPrefetchState::CANCELLED_WITH_SEEK, -1, nullptr);
            }
        }

        break;
    }

    assert(!prefetch_future.valid());

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();
    bytes_to_ignore = 0;

    if (read_until_position && new_pos > *read_until_position)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
        file_offset_of_buffer_end = new_pos = *read_until_position; /// read_until_position is a non-included boundary.
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
        return new_pos;
    }

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */
    if (!impl->seekIsCheap() && file_offset_of_buffer_end && read_until_position && new_pos < *read_until_position
        && new_pos > file_offset_of_buffer_end && new_pos < file_offset_of_buffer_end + read_settings.remote_read_min_bytes_for_seek)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = new_pos - file_offset_of_buffer_end;
    }
    else
    {
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
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void AsynchronousBoundedReadBuffer::resetPrefetch(FilesystemPrefetchState state)
{
    if (!prefetch_future.valid())
        return;

    auto [size, offset, _] = prefetch_future.get();
    prefetch_future = {};
    last_prefetch_info = {};

    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, size);

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

}
