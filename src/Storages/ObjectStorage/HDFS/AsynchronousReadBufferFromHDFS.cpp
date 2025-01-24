#include "AsynchronousReadBufferFromHDFS.h"

#if USE_HDFS
#include "ReadBufferFromHDFS.h"
#include <mutex>
#include <Common/logger_useful.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/AsynchronousReader.h>


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
    extern const Event RemoteFSBuffers;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

AsynchronousReadBufferFromHDFS::AsynchronousReadBufferFromHDFS(
    IAsynchronousReader & reader_, const ReadSettings & settings_, std::shared_ptr<ReadBufferFromHDFS> impl_)
    : BufferWithOwnMemory<SeekableReadBuffer>(settings_.remote_fs_buffer_size)
    , reader(reader_)
    , base_priority(settings_.priority)
    , impl(std::move(impl_))
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    , read_until_position(impl->getFileSize())
    , use_prefetch(settings_.remote_fs_prefetch)
    , log(getLogger("AsynchronousReadBufferFromHDFS"))
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}

bool AsynchronousReadBufferFromHDFS::hasPendingDataToRead()
{
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

std::future<IAsynchronousReader::Result> AsynchronousReadBufferFromHDFS::asyncReadInto(char * data, size_t size, Priority priority)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(*impl, nullptr);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = Priority{base_priority.value + priority.value};
    request.ignore = 0;
    return reader.submit(request);
}

void AsynchronousReadBufferFromHDFS::prefetch(Priority priority)
{
    interval_watch.restart();

    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    prefetch_future = asyncReadInto(prefetch_buffer.data(), prefetch_buffer.size(), priority);
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}


std::optional<size_t> AsynchronousReadBufferFromHDFS::tryGetFileSize()
{
    return impl->tryGetFileSize();
}

String AsynchronousReadBufferFromHDFS::getFileName() const
{
    return impl->getFileName();
}


bool AsynchronousReadBufferFromHDFS::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    ++next_times;
    sum_interval += interval_watch.elapsedMicroseconds();

    Stopwatch next_watch;
    Int64 wait = -1;
    size_t size = 0;
    size_t bytes_read = 0;

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);

        size_t offset = 0;
        {
            Stopwatch watch;
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
            auto result = prefetch_future.get();
            size = result.size;
            offset = result.offset;
            LOG_TEST(log, "Current size: {}, offset: {}", size, offset);

            /// If prefetch_future is valid, size should always be greater than zero.
            assert(offset <= size);
            bytes_read = size - offset;

            wait = watch.elapsedMicroseconds();
            ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, wait);
        }

        prefetch_buffer.swap(memory);

        /// Adjust the working buffer so that it ignores `offset` bytes.
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = Buffer(memory.data() + offset, memory.data() + size);
        pos = working_buffer.begin();
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);

        auto result = asyncReadInto(memory.data(), memory.size(), DEFAULT_PREFETCH_PRIORITY).get();
        size = result.size;
        auto offset = result.offset;

        LOG_TEST(log, "Current size: {}, offset: {}", size, offset);
        assert(offset <= size);
        bytes_read = size - offset;

        if (bytes_read)
        {
            /// Adjust the working buffer so that it ignores `offset` bytes.
            internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
            working_buffer = Buffer(memory.data() + offset, memory.data() + size);
            pos = working_buffer.begin();
        }
    }

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();
    prefetch_future = {};

    if (use_prefetch && bytes_read)
        prefetch(DEFAULT_PREFETCH_PRIORITY);

    sum_duration += next_watch.elapsedMicroseconds();
    sum_wait += wait;
    return bytes_read;
}

off_t AsynchronousReadBufferFromHDFS::seek(off_t offset, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset);

    size_t new_pos = offset;

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
        if (prefetch_future.valid())
        {
            /// Read from prefetch buffer and recheck if the new position is valid inside.
            /// TODO we can judge quickly without waiting for prefetch
            if (nextImpl())
            {
                read_from_prefetch = true;
                continue;
            }
        }

        /// Prefetch is cancelled because of seek.
        if (read_from_prefetch)
            ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
        break;
    }

    assert(!prefetch_future.valid());

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();

    ++seek_times;
    impl->seek(new_pos, SEEK_SET);
    file_offset_of_buffer_end = new_pos;

    return new_pos;
}

void AsynchronousReadBufferFromHDFS::finalize()
{
    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnusedPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }
}

AsynchronousReadBufferFromHDFS::~AsynchronousReadBufferFromHDFS()
{
    LOG_TEST(
        log,
        "object:{} next_times:{} seek_times:{} interval:{}|{} duration:{}|{} wait:{}|{}",
        reinterpret_cast<std::uintptr_t>(this),
        next_times,
        seek_times,
        sum_interval,
        double(sum_interval)/next_times,
        sum_duration,
        double(sum_duration)/next_times,
        sum_wait,
        double(sum_wait)/next_times);

    finalize();
}

off_t AsynchronousReadBufferFromHDFS::getPosition()
{
    return file_offset_of_buffer_end - available();
}

size_t AsynchronousReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}

}

#endif
