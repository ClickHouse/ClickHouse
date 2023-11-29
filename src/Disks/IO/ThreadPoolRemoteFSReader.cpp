#include "ThreadPoolRemoteFSReader.h"

#include "config.h"
#include <Common/ThreadPool_fwd.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/AsyncReadCounters.h>
#include <base/getThreadId.h>

#include <future>
#include <memory>


namespace ProfileEvents
{
    extern const Event ThreadpoolReaderTaskMicroseconds;
    extern const Event ThreadpoolReaderReadBytes;
    extern const Event ThreadpoolReaderSubmit;
}

namespace CurrentMetrics
{
    extern const Metric RemoteRead;
    extern const Metric ThreadPoolRemoteFSReaderThreads;
    extern const Metric ThreadPoolRemoteFSReaderThreadsActive;
    extern const Metric ThreadPoolRemoteFSReaderThreadsScheduled;
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

ThreadPoolRemoteFSReader::ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_)
    : pool(std::make_unique<ThreadPool>(CurrentMetrics::ThreadPoolRemoteFSReaderThreads,
                                        CurrentMetrics::ThreadPoolRemoteFSReaderThreadsActive,
                                        CurrentMetrics::ThreadPoolRemoteFSReaderThreadsScheduled,
                                        pool_size, pool_size, queue_size_))
{
}


std::future<IAsynchronousReader::Result> ThreadPoolRemoteFSReader::submit(Request request)
{
    ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderSubmit);
    return scheduleFromThreadPool<Result>([request, this]() -> Result { return execute(request); },
                                          *pool,
                                          "VFSRead",
                                          request.priority);
}

IAsynchronousReader::Result ThreadPoolRemoteFSReader::execute(Request request)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::RemoteRead};

    auto * fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());
    auto & reader = fd->getReader();

    auto read_counters = fd->getReadCounters();
    std::optional<AsyncReadIncrement> increment = read_counters ? std::optional<AsyncReadIncrement>(read_counters) : std::nullopt;

    auto watch = std::make_unique<Stopwatch>(CLOCK_REALTIME);

    reader.set(request.buf, request.size);
    reader.seek(request.offset, SEEK_SET);
    if (request.ignore)
        reader.ignore(request.ignore);

    bool result = reader.available();
    if (!result)
        result = reader.next();

    watch->stop();
    ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch->elapsedMicroseconds());

    IAsynchronousReader::Result read_result;
    if (result)
    {
        read_result.size = reader.buffer().size();
        read_result.offset = reader.offset();
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, read_result.size);
    }

    read_result.execution_watch = std::move(watch);
    return read_result;
}

void ThreadPoolRemoteFSReader::wait()
{
    pool->wait();
}

}
