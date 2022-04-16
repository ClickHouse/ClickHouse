#include "AsynchronousReadBufferFromHDFS.h"
#include "base/logger_useful.h"

#if USE_HDFS
#include <mutex>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

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
    extern const Event RemoteFSLazySeeks;
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
    AsynchronousReaderPtr reader_, const ReadSettings & settings_, std::shared_ptr<ReadBufferFromHDFS> impl_, size_t min_bytes_for_seek_)
    : BufferWithOwnMemory<SeekableReadBufferWithSize>(settings_.remote_fs_buffer_size)
    , reader(reader_)
    , priority(settings_.priority)
    , impl(impl_)
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , read_until_position(impl->getTotalSize())
    , log(&Poco::Logger::get("AsynchronousReadBufferFromHDFS"))
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

std::future<IAsynchronousReader::Result> AsynchronousReadBufferFromHDFS::readInto(char * data, size_t size)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor<ReadBufferFromHDFS>>(impl);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = priority;
    request.ignore = 0;

    if (bytes_to_ignore)
    {
        request.ignore = bytes_to_ignore;
        bytes_to_ignore = 0;
    }
    return reader->submit(request);
}

void AsynchronousReadBufferFromHDFS::prefetch()
{
    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}


std::optional<size_t> AsynchronousReadBufferFromHDFS::getTotalSize()
{
    return impl->getTotalSize();
}


bool AsynchronousReadBufferFromHDFS::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    size_t size = 0;
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
            assert(offset < size);
            ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
        }

        prefetch_buffer.swap(memory);
        /// Adjust the working buffer so that it ignores `offset` bytes.
        setWithBytesToIgnore(memory.data(), size, offset);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);

        auto result = readInto(memory.data(), memory.size()).get();
        size = result.size;
        auto offset = result.offset;

        LOG_TEST(log, "Current size: {}, offset: {}", size, offset);
        assert(offset < size);

        if (size)
        {
            /// Adjust the working buffer so that it ignores `offset` bytes.
            setWithBytesToIgnore(memory.data(), size, offset);
        }
    }

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();
    prefetch_future = {};
    return size;
}

off_t AsynchronousReadBufferFromHDFS::seek(off_t offset, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

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
        else if (prefetch_future.valid())
        {
            /// Read from prefetch buffer and recheck if the new position is valid inside.
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

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */

    if (read_until_position && new_pos < *read_until_position && new_pos > file_offset_of_buffer_end
        && new_pos < file_offset_of_buffer_end + min_bytes_for_seek)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = new_pos - file_offset_of_buffer_end;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);
        impl->seek(new_pos, SEEK_SET);
        file_offset_of_buffer_end = new_pos;
    }

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


/*
    static AsynchronousReaderPtr getThreadPoolReader()
    {
        constexpr size_t pool_size = 50;
        constexpr size_t queue_size = 1000000;
        static AsynchronousReaderPtr reader = std::make_shared<ThreadPoolRemoteFSReader<ReadBufferFromHDFS>>(pool_size, queue_size);
        return reader;
    }
*/
}

#endif
