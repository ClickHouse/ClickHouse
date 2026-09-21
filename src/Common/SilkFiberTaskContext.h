#pragma once

#include "config.h"

#if USE_SILK

#include <Common/OpenTelemetryTraceContext.h>

#include <functional>
#include <memory>
#include <string>

namespace silk
{
class FiberFuture;
}

namespace DB
{
class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;
}

namespace Silk
{

/// What a fiber inherits from the code that spawns it, the same way a `ThreadPool` job inherits it
/// from the thread that schedules it: the thread group (memory tracking, profile events, query
/// context, logs) and the OpenTelemetry tracing context.
///
/// Without it a fiber runs detached: its allocations are charged to the global memory tracker only,
/// bypassing the query's `max_memory_usage`, its profile events are lost for the query, and its
/// spans belong to no trace.
struct FiberTaskContext
{
    DB::ThreadGroupPtr thread_group;
    DB::OpenTelemetry::TracingContextOnThread trace_context;
    /// Name of the span that covers the whole fiber when tracing is enabled.
    std::string operation_name;

    /// Captures the thread group and the tracing context of the calling thread or fiber.
    static FiberTaskContext capture(std::string operation_name);
};

/// Runs the task on a fiber attached to `context`: the fiber's `ThreadStatus` joins the thread group
/// for the duration of the task, and the task runs under a span that is a child of the captured
/// tracing context. Result codes are those of `spawn` in `SilkFiberScheduler.h`.
[[nodiscard]] int spawn(std::function<int()> task, silk::FiberFuture & future, FiberTaskContext context);

/// Like `spawn` above, but waits for the result: blocks a plain thread, suspends cooperatively when
/// called from a fiber.
[[nodiscard]] int runBlocking(std::function<int()> task, FiberTaskContext context);

}

#endif
