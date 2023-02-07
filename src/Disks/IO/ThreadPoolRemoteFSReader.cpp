#include "ThreadPoolRemoteFSReader.h"

#include "config.h"
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <IO/SeekableReadBuffer.h>

#include <future>


namespace ProfileEvents
{
    extern const Event ThreadpoolReaderTaskMicroseconds;
    extern const Event ThreadpoolReaderReadBytes;
    extern const Event ThreadpoolReaderSubmit;
}

namespace CurrentMetrics
{
    extern const Metric RemoteRead;
}

namespace DB
{
IAsynchronousReader::Result RemoteFSFileDescriptor::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    return reader.readInto(data, size, offset, ignore);
}


ThreadPoolRemoteFSReader::ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_)
    : pool(pool_size, pool_size, queue_size_)
{
}


std::future<IAsynchronousReader::Result> ThreadPoolRemoteFSReader::submit(Request request)
{
    ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderSubmit);
    return scheduleFromThreadPool<Result>([request]() -> Result
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::RemoteRead};
        Stopwatch watch(CLOCK_MONOTONIC);

        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());

        Result result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, result.size);

        return Result{ .size = result.size, .offset = result.offset };
    }, pool, "VFSRead", request.priority);
}

}
