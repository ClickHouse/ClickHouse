#include "ThreadPoolRemoteFSReader.h"

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>
#include <Common/config.h>
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
    ThreadGroupStatusPtr running_group = CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup()
            ? CurrentThread::get().getThreadGroup()
            : MainThreadStatus::getInstance().getThreadGroup();

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    auto task = std::make_shared<std::packaged_task<Result()>>([request, running_group, query_context]
    {
        ThreadStatus thread_status;

        /// To be able to pass ProfileEvents.
        if (running_group)
            thread_status.attachQuery(running_group);

        /// Save query context if any, because cache implementation needs it.
        if (query_context)
            thread_status.attachQueryContext(query_context);

        setThreadName("VFSRead");

        CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
        auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());

        Stopwatch watch(CLOCK_MONOTONIC);

        Result result;
        try
        {
            result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);
        }
        catch (...)
        {
            if (running_group)
                CurrentThread::detachQuery();
            throw;
        }

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderTaskMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::ThreadpoolReaderReadBytes, result.offset ? result.size - result.offset : result.size);

        thread_status.detachQuery(/* if_not_detached */true);

        return Result{ .size = result.size, .offset = result.offset };
    });

    auto future = task->get_future();

    /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
    pool.scheduleOrThrow([task]{ (*task)(); }, -request.priority);

    return future;
}

}
