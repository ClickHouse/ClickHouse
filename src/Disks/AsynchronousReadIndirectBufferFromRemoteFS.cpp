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


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::readNext()
{
    IAsynchronousReader::Request request;

    auto remote_fd = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>();
    remote_fd->impl = impl;
    impl->position() = position();
    assert(!impl->hasPendingData());

    request.descriptor = std::move(remote_fd);
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
        std::cerr << "Having prefetched data\n";
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        size = prefetch_future.get();
        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
    }
    else
    {
        std::cerr << "No prefetched data\n";
        size = readNext().get();
    }

    if (size)
    {
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
        prefetch_future = {};
    }

    return size;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && static_cast<size_t>(getPosition() + offset_) < impl->absolute_position)
        {
            pos += offset_;
            return getPosition();
        }
        else
        {
            impl->absolute_position += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position is within current working buffer - shift pos.
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= impl->absolute_position - working_buffer.size()
            && size_t(offset_) < impl->absolute_position)
        {
            pos = working_buffer.end() - (impl->absolute_position - offset_);

            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return getPosition();
        }
        else
        {
            impl->absolute_position = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (prefetch_future.valid())
    {
        std::cerr << "Ignoring prefetched data" << "\n";
        prefetch_future.wait();
        impl->reset(); /// Clean the buffer, we do no need it.
        prefetch_future = {};
    }

    impl->seek(impl->absolute_position, SEEK_SET);
    pos = working_buffer.end();

    return impl->absolute_position;
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
