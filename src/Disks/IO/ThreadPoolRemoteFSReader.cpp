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
#include <IO/AsyncReadCounters.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>

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

namespace
{
    struct AsyncReadIncrement : boost::noncopyable
    {
        explicit AsyncReadIncrement(std::shared_ptr<AsyncReadCounters> counters_)
            : counters(counters_)
        {
            std::lock_guard lock(counters->mutex);
            if (++counters->current_parallel_read_tasks > counters->max_parallel_read_tasks)
                counters->max_parallel_read_tasks = counters->current_parallel_read_tasks;
        }

        ~AsyncReadIncrement()
        {
            std::lock_guard lock(counters->mutex);
            --counters->current_parallel_read_tasks;
        }

        std::shared_ptr<AsyncReadCounters> counters;
    };
}

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

        std::optional<AsyncReadIncrement> increment;
        if (CurrentThread::isInitialized())
        {
            auto query_context = CurrentThread::get().getQueryContext();
            if (query_context)
                increment.emplace(query_context->getAsyncReadCounters());
        }

        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());

        auto watch = std::make_unique<Stopwatch>(CLOCK_MONOTONIC);
        Result result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);
        watch->stop();

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch->elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, result.size);

        return Result{ .size = result.size, .offset = result.offset, .execution_watch = std::move(watch) };
    }, pool, "VFSRead", request.priority);
}

}
