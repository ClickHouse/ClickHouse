#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

void CurrentThread::detachFromGroupIfNotDetached()
{
}

void CurrentThread::attachToGroup(const ThreadGroupPtr &)
{
}

void ThreadStatus::initGlobalProfiler(UInt64 /*global_profiler_real_time_period*/, UInt64 /*global_profiler_cpu_time_period*/)
{
}

}
