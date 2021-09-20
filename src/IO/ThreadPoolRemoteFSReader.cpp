#include <IO/ThreadPoolRemoteFSReader.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>

#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromRemoteFS.h>

#include <future>
#include <iostream>


namespace ProfileEvents
{
    extern const Event RemoteFSReadMicroseconds;
    extern const Event RemoteFSReadBytes;
}

namespace CurrentMetrics
{
    extern const Metric Read;
}

namespace DB
{


ThreadPoolRemoteFSReader::ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_)
    : pool(pool_size, pool_size, queue_size_)
{
}


std::future<IAsynchronousReader::Result> ThreadPoolRemoteFSReader::submit(Request request)
{
    auto task = std::make_shared<std::packaged_task<Result()>>([request]
    {
        setThreadName("ThreadPoolRead");
        CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());
        auto * remote_fs_buf = dynamic_cast<ReadBufferFromRemoteFS *>(remote_fs_fd->impl.get());

        Stopwatch watch(CLOCK_MONOTONIC);
        size_t bytes_read = 0;
        auto result = remote_fs_buf->readNext();
        if (result)
            bytes_read = remote_fs_buf->buffer().size();
        watch.stop();

        ProfileEvents::increment(ProfileEvents::RemoteFSReadMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::RemoteFSReadBytes, bytes_read);

        return bytes_read;
    });

    auto future = task->get_future();

    /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
    pool.scheduleOrThrow([task]{ (*task)(); }, -request.priority);
    return future;
}
}
