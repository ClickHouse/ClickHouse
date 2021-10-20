#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <Common/Stopwatch.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <base/logger_useful.h>


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
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


AsynchronousReadIndirectBufferFromRemoteFS::AsynchronousReadIndirectBufferFromRemoteFS(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t buf_size_,
        size_t min_bytes_for_seek_)
    : ReadBufferFromFileBase(buf_size_, nullptr, 0)
    , reader(reader_)
    , priority(priority_)
    , impl(impl_)
    , prefetch_buffer(buf_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}


String AsynchronousReadIndirectBufferFromRemoteFS::getFileName() const
{
    return impl->getFileName();
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::readInto(char * data, size_t size)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>(impl);
    request.buf = data;
    request.size = size;
    request.offset = absolute_position;
    request.priority = priority;

    if (bytes_to_ignore)
    {
        request.ignore = bytes_to_ignore;
        bytes_to_ignore = 0;
    }
    return reader->submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch()
{
    if (prefetch_future.valid())
        return;

    /// Everything is already read.
    if (absolute_position == last_offset)
        return;

    if (absolute_position > last_offset)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {})",
                        absolute_position, last_offset);
    }

    /// Prefetch even in case hasPendingData() == true.
    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}


void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilPosition(size_t position)
{
    if (prefetch_future.valid())
    {
        /// TODO: Planning to put logical error here after more testing,
        // because seems like future is never supposed to be valid at this point.
        std::terminate();
    }

    last_offset = position;
    impl->setReadUntilPosition(position);
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    /// Everything is already read.
    if (absolute_position == last_offset)
        return false;

    if (absolute_position > last_offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {})",
                        absolute_position, last_offset);

    size_t size = 0;

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        {
            size = prefetch_future.get();
            if (size)
            {
                memory.swap(prefetch_buffer);
                set(memory.data(), memory.size());
                working_buffer.resize(size);
                absolute_position += size;
            }
        }

        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        size = readInto(memory.data(), memory.size()).get();

        if (size)
        {
            set(memory.data(), memory.size());
            working_buffer.resize(size);
            absolute_position += size;
        }
    }

    prefetch_future = {};
    return size;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && static_cast<size_t>(getPosition() + offset_) < absolute_position)
        {
            pos += offset_;
            return getPosition();
        }
        else
        {
            absolute_position += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position is within current working buffer - shift pos.
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= absolute_position - working_buffer.size()
            && size_t(offset_) < absolute_position)
        {
            pos = working_buffer.end() - (absolute_position - offset_);

            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return getPosition();
        }
        else
        {
            absolute_position = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }

    pos = working_buffer.end();

    /// Note: we read in range [absolute_position, last_offset).
    if (absolute_position < last_offset
        && static_cast<off_t>(absolute_position) >= getPosition()
        && static_cast<off_t>(absolute_position) < getPosition() + static_cast<off_t>(min_bytes_for_seek))
    {
       /**
        * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
        */
        bytes_to_ignore = absolute_position - getPosition();
    }
    else
    {
        impl->seek(absolute_position); /// SEEK_SET.
    }

    return absolute_position;
}


void AsynchronousReadIndirectBufferFromRemoteFS::finalize()
{
    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnusedPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }
}


AsynchronousReadIndirectBufferFromRemoteFS::~AsynchronousReadIndirectBufferFromRemoteFS()
{
    finalize();
}

}
