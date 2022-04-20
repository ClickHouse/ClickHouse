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
    AsynchronousReaderPtr reader_, const ReadSettings & settings_, std::shared_ptr<ReadBufferFromHDFS> impl_)
    // size_t min_bytes_for_seek_)
    : BufferWithOwnMemory<SeekableReadBufferWithSize>(settings_.remote_fs_buffer_size)
    // : BufferWithOwnMemory<SeekableReadBuffer>(settings_.remote_fs_buffer_size)
    , reader(reader_)
    , priority(settings_.priority)
    , impl(std::move(impl_))
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    // , min_bytes_for_seek(min_bytes_for_seek_)
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
    /*
    if (bytes_to_ignore)
    {
        request.ignore = bytes_to_ignore;
        bytes_to_ignore = 0;
    }
    */
    // std::cout << "obj:" << getId() << ",size:" << request.size << ",offset:" << request.offset << ",ignore:" << request.ignore << std::endl;
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
    // std::cout << getStatus(__FUNCTION__) << std::endl;
    return impl->getTotalSize();
}


bool AsynchronousReadBufferFromHDFS::nextImpl()
{
    StatusGuard guard{this, __FUNCTION__};
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
        // std::cout << "obj:" << getId() << ",size:" << size << ",offset:" << offset << std::endl;

        LOG_TEST(log, "Current size: {}, offset: {}", size, offset);
        assert(offset < size);

        if (size)
        {
            /// Adjust the working buffer so that it ignores `offset` bytes.
            setWithBytesToIgnore(memory.data(), size, offset);
            // std::cout << "memory:" << std::string(memory.data(), size) << std::endl;
        }
    }

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();
    prefetch_future = {};
    // std::cout << "stacktrace:" << StackTrace{}.toString() << std::endl;
    return size;
}

off_t AsynchronousReadBufferFromHDFS::seek(off_t offset, int whence)
{
    std::cout << "AsynchronousReadBufferFromHDFS seek" << std::endl;
    std::cout << "obj:" << getId() << ",seek_offset:" << offset << std::endl;
    StatusGuard guard{this, __FUNCTION__};
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

    impl->seek(new_pos, SEEK_SET);
    file_offset_of_buffer_end = new_pos;
    return new_pos;
}

void AsynchronousReadBufferFromHDFS::finalize()
{
    // StatusGuard guard{this, __FUNCTION__};
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
    // std::cout << getStatus(__FUNCTION__) << std::endl;
    return file_offset_of_buffer_end - available();
}

size_t AsynchronousReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}

String AsynchronousReadBufferFromHDFS::getStatus(const String & action)
{
    String res;
    res += "action:" + action;
    res += ",";
    res += "obj:" + getId();
    res += ",";
    /*
    res += "impl_position:" + std::to_string(impl->getPosition());
    res += ",";
    res += "impl_file_offset_of_buffer_end:" + std::to_string(impl->getFileOffsetOfBufferEnd());
    res += ",";
    */
    res += "file_offset_of_buffer_end:" + std::to_string(file_offset_of_buffer_end);
    res += ",";
    res += "read_until_position:" + std::to_string(*read_until_position);
    res += ",";
    res += "prefetch_future_valid:" + std::to_string(prefetch_future.valid());
    res += ",";
    res += "working_buffer_size:" + std::to_string(working_buffer.size());
    res += ",";
    res += "working_buffer_pos:" + std::to_string(offset());
    return res;
}


}

#endif
