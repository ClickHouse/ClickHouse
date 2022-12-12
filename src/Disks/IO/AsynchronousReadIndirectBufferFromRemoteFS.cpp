#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Storages/ElapsedTimeProfileEventIncrement.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Interpreters/Context.cpp>
#include <base/getThreadId.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}

namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
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


AsynchronousReadIndirectBufferFromRemoteFS::AsynchronousReadIndirectBufferFromRemoteFS(
        IAsynchronousReader & reader_,
        const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t min_bytes_for_seek_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , read_settings(settings_)
    , reader(reader_)
    , base_priority(settings_.priority)
    , impl(impl_)
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr
               ? CurrentThread::getQueryId() : "")
    , current_reader_id(getRandomASCIIString(8))
#ifndef NDEBUG
    , log(&Poco::Logger::get("AsynchronousBufferFromRemoteFS"))
#else
    , log(&Poco::Logger::get("AsyncBuffer(" + impl->getFileName() + ")"))
#endif
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}

String AsynchronousReadIndirectBufferFromRemoteFS::getFileName() const
{
    return impl->getFileName();
}


String AsynchronousReadIndirectBufferFromRemoteFS::getInfoForLog()
{
    return impl->getInfoForLog();
}

size_t AsynchronousReadIndirectBufferFromRemoteFS::getFileSize()
{
    return impl->getFileSize();
}

bool AsynchronousReadIndirectBufferFromRemoteFS::hasPendingDataToRead()
{
    /**
     * Note: read_until_position here can be std::nullopt only for non-MergeTree tables.
     * For mergeTree tables it must be guaranteed that setReadUntilPosition() or
     * setReadUntilEnd() is called before any read or prefetch.
     * setReadUntilEnd() always sets read_until_position to file size.
     * setReadUntilPosition(pos) always has pos > 0, because if
     * right_offset_in_compressed_file is 0, then setReadUntilEnd() is used.
     */
    if (read_until_position)
    {
        /// Everything is already read.
        if (file_offset_of_buffer_end == *read_until_position)
            return false;

        if (file_offset_of_buffer_end > *read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {}, info: {})",
                            file_offset_of_buffer_end, *read_until_position, impl->getInfoForLog());
    }

    return true;
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::asyncReadInto(char * data, size_t size, int64_t priority)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(impl);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = base_priority + priority;

    if (bytes_to_ignore)
    {
        request.ignore = bytes_to_ignore;
        bytes_to_ignore = 0;
    }
    return reader.submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch(int64_t priority)
{
    if (prefetch_future.valid())
        return;

    /// Check boundary, which was set in readUntilPosition().
    if (!hasPendingDataToRead())
        return;

    /// Prefetch even in case hasPendingData() == true.
    chassert(prefetch_buffer.size() == read_settings.remote_fs_buffer_size);
    prefetch_future = asyncReadInto(prefetch_buffer.data(), prefetch_buffer.size(), priority);
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
    prefetch_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void AsynchronousReadIndirectBufferFromRemoteFS::resetPrefetch(FilesystemPrefetchState state)
{
    if (!prefetch_future.valid())
        return;

    auto [size, _] = prefetch_future.get();
    prefetch_future = {};

    if (read_settings.enable_filesystem_read_prefetches_log)
    {
        appendToPrefetchLog(state, size);
    }

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

void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilPosition(size_t position)
{
    /// Do not reinitialize internal state in case the new end of range is already included.
    /// Actually it is likely that we will anyway reinitialize it as seek method is called after
    /// changing end position, but seek avoiding feature might help to avoid reinitialization,
    /// so this check is useful to save the prefetch for the time when we try to avoid seek by
    /// reading and ignoring some data.
    if (!read_until_position || position > *read_until_position)
    {
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


void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilEnd()
{
    read_until_position = impl->getFileSize();
    impl->setReadUntilPosition(*read_until_position);
}


void AsynchronousReadIndirectBufferFromRemoteFS::appendToPrefetchLog(FilesystemPrefetchState state, int64_t size)
{
    auto * object = impl->getCurrentObject();
    chassert(object != nullptr);
    if (!object)
        return;

    FilesystemReadPrefetchesLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .path = object->getConnectedPath(),
        .offset = file_offset_of_buffer_end,
        .size = size,
        .prefetch_start_time = prefetch_start_time,
        .state = state,
        .thread_id = getThreadId(),
        .reader_id = current_reader_id,
    };

    if (auto log = Context::getGlobalContextInstance()->getFilesystemReadPrefetchesLog())
        log->add(elem);
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    Stopwatch watch;
    size_t size, offset;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

    if (prefetch_future.valid())
    {
        ElapsedUSProfileEventIncrement measure_time(ProfileEvents::AsynchronousReadWaitMicroseconds);

        std::tie(size, offset) = prefetch_future.get();
        prefetch_future = {};
        prefetch_buffer.swap(memory);

        if (read_settings.enable_filesystem_read_prefetches_log)
        {
            appendToPrefetchLog(FilesystemPrefetchState::USED, size);
        }

        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, size);
    }
    else
    {
        ElapsedUSProfileEventIncrement measure_time(ProfileEvents::AsynchronousReadWaitMicroseconds);

        chassert(memory.size() == read_settings.remote_fs_buffer_size);
        std::tie(size, offset) = asyncReadInto(memory.data(), memory.size(), 0).get();

        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedBytes, size);
    }

    chassert(size >= offset);
    LOG_TEST(log, "Current size: {}, offset: {}", size, offset);

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
    assert(file_offset_of_buffer_end >= impl->getImplementationBufferOffset());
    assert(file_offset_of_buffer_end <= impl->getFileSize());

    // if (bytes_read)
    // {
    //     /// Prefetch next data. Will do nothing if the requested range
    //     /// (set with setReadUntilPosition()) is already fulfilled.
    //     prefetch();
    // }

    return bytes_read;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset, int whence)
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
        new_pos = file_offset_of_buffer_end - (working_buffer.end() - pos) + offset;
    }
    else
    {
        throw Exception("ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
        return new_pos;

    bool read_from_prefetch = false;
    while (true)
    {
        if (file_offset_of_buffer_end - working_buffer.size() <= new_pos && new_pos <= file_offset_of_buffer_end)
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
                appendToPrefetchLog(FilesystemPrefetchState::CANCELLED_WITH_SEEK, -1);
            }
        }

        break;
    }

    assert(!prefetch_future.valid());

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */
    if (impl->initialized()
        && read_until_position && new_pos < *read_until_position
        && new_pos > file_offset_of_buffer_end
        && new_pos < file_offset_of_buffer_end + min_bytes_for_seek)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = new_pos - file_offset_of_buffer_end;
    }
    else
    {
        if (impl->initialized())
        {
            ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
            impl->reset();
        }

        file_offset_of_buffer_end = new_pos;
    }

    return new_pos;
}


void AsynchronousReadIndirectBufferFromRemoteFS::finalize()
{
    resetPrefetch(FilesystemPrefetchState::UNNEEDED);
}


AsynchronousReadIndirectBufferFromRemoteFS::~AsynchronousReadIndirectBufferFromRemoteFS()
{
    finalize();
}

}
