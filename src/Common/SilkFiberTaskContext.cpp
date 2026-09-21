#include <Common/SilkFiberTaskContext.h>

#if USE_SILK

#include <Common/CurrentThread.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <silk/fibers/future.h>

#include <utility>

namespace Silk
{

FiberTaskContext FiberTaskContext::capture(std::string operation_name_)
{
    return FiberTaskContext
    {
        .thread_group = DB::CurrentThread::getGroup(),
        .trace_context = DB::OpenTelemetry::CurrentContext(),
        .operation_name = std::move(operation_name_),
    };
}

int spawn(std::function<int()> task, silk::FiberFuture & future, FiberTaskContext context)
{
    return spawn(
        [fiber_task = std::move(task), fiber_context = std::move(context)]() -> int
        {
            /// The fiber already owns a `ThreadStatus` that is not bound to an OS thread
            /// (see `FiberContext::main` in `SilkFiberScheduler.cpp`); here it joins the group.
            if (fiber_context.thread_group)
            {
                LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
                DB::CurrentThread::attachToGroup(fiber_context.thread_group);
            }
            SCOPE_EXIT_SAFE({
                if (fiber_context.thread_group)
                {
                    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
                    DB::CurrentThread::detachFromGroupIfNotDetached();
                }
            });

            /// A no-op when the captured context carries no trace.
            DB::OpenTelemetry::TracingContextHolder tracing_context_holder(fiber_context.operation_name, fiber_context.trace_context);

            return fiber_task();
        },
        future);
}

int runBlocking(std::function<int()> task, FiberTaskContext context)
{
    silk::FiberFuture future;
    int res = spawn(std::move(task), future, std::move(context));
    return res ? res : future.wait();
}

}

#endif
