#include "ThreadPoolRemoteFSReader.h"

#include <IO/AsyncReadCounters.h>
#include <IO/SeekableReadBuffer.h>
#include <base/getThreadId.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/assert_cast.h>
#include "config.h"

#include <future>
#include <memory>


namespace ProfileEvents
{
    extern const Event ThreadpoolReaderTaskMicroseconds;
    extern const Event ThreadpoolReaderPrepareMicroseconds;
    extern const Event ThreadpoolReaderReadBytes;
    extern const Event ThreadpoolReaderSubmit;
    extern const Event ThreadpoolReaderSubmitReadSynchronously;
    extern const Event ThreadpoolReaderSubmitReadSynchronouslyBytes;
    extern const Event ThreadpoolReaderSubmitReadSynchronouslyMicroseconds;
    extern const Event ThreadpoolReaderSubmitLookupInCacheMicroseconds;
    extern const Event AsynchronousReaderIgnoredBytes;
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
    auto * fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());
    auto & reader = fd->getReader();

    {
        ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderPrepareMicroseconds);
        /// `seek` have to be done before checking `isContentCached`, and `set` have to be done prior to `seek`
        reader.set(request.buf, request.size);
        reader.seek(request.offset, SEEK_SET);
    }

    bool is_content_cached = false;
    {
        ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderSubmitLookupInCacheMicroseconds);
        is_content_cached = reader.isContentCached(request.offset, request.size);
    }

    if (is_content_cached)
    {
        std::promise<Result> promise;
        std::future<Result> future = promise.get_future();
        auto && res = execute(request, /*seek_performed=*/true);

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderSubmitReadSynchronously);
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderSubmitReadSynchronouslyBytes, res.size);
        if (res.execution_watch)
            ProfileEvents::increment(ProfileEvents::ThreadpoolReaderSubmitReadSynchronouslyMicroseconds, res.execution_watch->elapsedMicroseconds());

        promise.set_value(std::move(res));
        return future;
    }

    ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderSubmit);
    return scheduleFromThreadPoolUnsafe<Result>(
        [request, this]() -> Result { return execute(request, /*seek_performed=*/true); }, *pool, "VFSRead", request.priority);
}

IAsynchronousReader::Result ThreadPoolRemoteFSReader::execute(Request request)
{
    return execute(request, /*seek_performed=*/false);
}

IAsynchronousReader::Result ThreadPoolRemoteFSReader::execute(Request request, bool seek_performed)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::RemoteRead};

    auto * fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());
    auto & reader = fd->getReader();

    auto read_counters = fd->getReadCounters();
    std::optional<AsyncReadIncrement> increment = read_counters ? std::optional<AsyncReadIncrement>(read_counters) : std::nullopt;

    {
        ProfileEventTimeIncrement<Microseconds> elapsed(ProfileEvents::ThreadpoolReaderPrepareMicroseconds);
        if (!seek_performed)
        {
            reader.set(request.buf, request.size);
            reader.seek(request.offset, SEEK_SET);
        }

        if (request.ignore)
        {
            ProfileEvents::increment(ProfileEvents::AsynchronousReaderIgnoredBytes, request.ignore);
            reader.ignore(request.ignore);
        }
    }

    auto watch = std::make_unique<Stopwatch>(CLOCK_REALTIME);

    bool result = reader.available();
    if (!result)
        result = reader.next();

    watch->stop();
    ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch->elapsedMicroseconds());

    IAsynchronousReader::Result read_result;
    if (result)
    {
        chassert(reader.buffer().begin() == request.buf);
        chassert(reader.buffer().end() <= request.buf + request.size);
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
