#include <Common/ThreadWithThreadStatus.h>

#include <Common/ThreadStatus.h>


void ThreadWithThreadStatus::run(std::function<void()> func, UInt64 real_time_period_ns, UInt64 cpu_time_period_ns)
{
    DB::ThreadStatus thread_status;
    if (unlikely(real_time_period_ns != 0 || cpu_time_period_ns != 0))
        thread_status.initGlobalProfiler(real_time_period_ns, cpu_time_period_ns);

    func();
}
