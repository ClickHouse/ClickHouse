#include "ThreadPoolRemoteFSReader.h"

#include "config.h"
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <IO/SeekableReadBuffer.h>

#include <future>


namespace ProfileEvents
{
    extern const Event ThreadpoolReaderTaskMicroseconds;
    extern const Event ThreadpoolReaderReadBytes;
}

namespace CurrentMetrics
{
    extern const Metric Read;
}

namespace DB
{
IAsynchronousReader::Result RemoteFSFileDescriptor::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    return reader->readInto(data, size, offset, ignore);
}


ThreadPoolRemoteFSReader::ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_)
    : pool(pool_size, pool_size, queue_size_)
{
}


std::future<IAsynchronousReader::Result> ThreadPoolRemoteFSReader::submit(Request request)
{
    auto schedule = threadPoolCallbackRunner<Result>(pool, "VFSRead");

    return schedule([request]() -> Result
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());

        Stopwatch watch(CLOCK_MONOTONIC);

        Result result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, result.offset ? result.size - result.offset : result.size);

        return Result{ .size = result.size, .offset = result.offset };
    }, request.priority);
}

}
