#include "ThreadPoolRemoteFSReader.h"

#include "config.h"
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/AsyncReadCounters.h>
#include <Interpreters/Context.h>

#include <future>


namespace ProfileEvents
{
    extern const Event ThreadpoolReaderTaskMicroseconds;
    extern const Event ThreadpoolReaderReadBytes;
}

namespace CurrentMetrics
{
    extern const Metric RemoteFSRead;
}

namespace DB
{

namespace
{
    struct AsyncReadIncrement : boost::noncopyable
    {
        explicit AsyncReadIncrement(AsyncReadCounters & counters_)
            : counters(counters_)
        {
            size_t current = counters.current_parallel_read_tasks.fetch_add(1) + 1;
            size_t current_max = counters.max_parallel_read_tasks;
            while (current > current_max &&
                        !counters.max_parallel_read_tasks.compare_exchange_weak(current_max, current)) {}

        }

        ~AsyncReadIncrement()
        {
            --counters.current_parallel_read_tasks;
        }

        AsyncReadCounters & counters;
    };
}

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
    return scheduleFromThreadPool<Result>([request]() -> Result
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::RemoteFSRead};

        std::optional<AsyncReadIncrement> increment;
        if (CurrentThread::isInitialized())
        {
            auto query_context = CurrentThread::get().getQueryContext();
            if (query_context)
                increment.emplace(query_context->getAsyncReadCounters());
        }

        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());

        Stopwatch watch(CLOCK_MONOTONIC);
        Result result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, result.size);

        return Result{ .size = result.size, .offset = result.offset };
    }, pool, "VFSRead", request.priority);
}

}
