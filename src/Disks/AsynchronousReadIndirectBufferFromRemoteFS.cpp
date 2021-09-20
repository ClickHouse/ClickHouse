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
    AsynchronousReaderPtr reader_, Int32 priority_, ReadBufferFromRemoteFSImpl impl_)
    : reader(reader_), priority(priority_), impl(impl_)
{
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::read()
{
    IAsynchronousReader::Request request;

    auto remote_fd = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>();
    remote_fd->impl = impl;
    swap(*impl);
    request.descriptor = std::move(remote_fd);
    request.offset = absolute_position;
    request.priority = priority;

    return reader->submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch()
{
    if (prefetch_future.valid())
    {
        std::cerr << "Prefetch, but not needed." << "\n";
        return;
    }

    std::cerr << fmt::format("Prefetch. Internal buffer size: {}, "
                             "prefetch buffer size: {}, "
                             "impl interanl buffer size: unknown",
                             internal_buffer.size());

    prefetch_future = read();
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    size_t size = 0;
    std::cerr << fmt::format("NextImpl. Offset: {}, absolute_pos: {}", offset(), absolute_position) << std::endl;

    if (prefetch_future.valid())
    {
        std::cerr << "Future is VALID!\n";

        Stopwatch watch;
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

        size = prefetch_future.get();

        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());

        swap(*impl);
        prefetch_future = {};
    }
    else
    {
        std::cerr << "Future is NOT VALID!\n";

        size = read().get();
        swap(*impl);
        prefetch_future = {};
    }

    if (size)
    {
        absolute_position += size;
        return true;
    }

    return false;
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

    std::cerr << "Seek with new absolute_position: " << absolute_position << std::endl;
    impl->seek(absolute_position, SEEK_SET);
    pos = working_buffer.end();

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
