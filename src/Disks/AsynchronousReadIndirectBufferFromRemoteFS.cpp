#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <IO/ReadBufferFromS3.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Disks/ReadIndirectBufferFromWebServer.h>
#include <Common/Stopwatch.h>
#include <IO/ThreadPoolRemoteFSReader.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}
namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


AsynchronousReadIndirectBufferFromRemoteFS::AsynchronousReadIndirectBufferFromRemoteFS(
    AsynchronousReaderPtr reader_, Int32 priority_, std::shared_ptr<ReadBufferFromRemoteFS> impl_)
    : reader(reader_), priority(priority_), impl(impl_)
{
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::readNext()
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>(impl);
    request.offset = absolute_position;
    request.priority = priority;
    return reader->submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch()
{
    if (prefetch_future.valid())
        return;

    prefetch_future = readNext();
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    size_t size = 0;

    if (prefetch_future.valid())
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        size = prefetch_future.get();
        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
    }
    else
    {
        size = readNext().get();
    }

    if (size)
    {
        swap(*impl);
        absolute_position += size;
        impl->reset();
    }

    prefetch_future = {};
    return size;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
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
        std::cerr << "Ignoring prefetched data" << "\n";
        prefetch_future.wait();
        prefetch_future = {};
    }

    pos = working_buffer.end();
    impl->reset(true);

    return absolute_position;
}


void AsynchronousReadIndirectBufferFromRemoteFS::finalize()
{
    if (prefetch_future.valid())
    {
        prefetch_future.wait();
        prefetch_future = {};
    }
}


AsynchronousReadIndirectBufferFromRemoteFS::~AsynchronousReadIndirectBufferFromRemoteFS()
{
    finalize();
}

}
