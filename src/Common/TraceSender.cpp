#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <Common/CPUID.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ProfileTracesBlocker.h>
#include <Common/StackTrace.h>
#include <Common/TraceSender.h>
#include <Common/setThreadName.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <string_view>
#include <chrono>
#include <cstring>
#include <poll.h>

namespace
{
    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    ///
    /// And it cannot be large, since otherwise it will not fit into PIPE_BUF.
    /// The performance test query ids can be surprisingly long like
    /// `aggregating_merge_tree_simple_aggregate_function_string.query100.profile100`,
    /// so make some allowance for them as well.
    /// Leave room for the fixed fields and stack frames within a portable 512-byte atomic pipe write.
    constexpr size_t QUERY_ID_MAX_LEN = 93;
    static_assert(QUERY_ID_MAX_LEN <= std::numeric_limits<uint8_t>::max());
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

namespace FailPoints
{
    extern const char profile_traces_flush_write_timeout[];
}

LazyPipeFDs TraceSender::pipe;
std::atomic<bool> TraceSender::shutdown{true};
std::atomic<int> TraceSender::in_flight{0};

static thread_local bool inside_send = false;
void TraceSender::send(TraceType trace_type, const StackTrace & stack_trace, Extras extras) noexcept
{
    /** The method shouldn't be called recursively or throw exceptions.
      * There are several reasons:
      * - avoid infinite recursion when some of subsequent functions invoke tracing;
      * - avoid inconsistent writes if the method was interrupted by a signal handler in the middle of writing,
      *   and then another tracing is invoked (e.g., from query profiler).
      */
    if (unlikely(inside_send))
        return;
    inside_send = true;
    SCOPE_EXIT({ inside_send = false; });
    DENY_ALLOCATIONS_IN_SCOPE;

    /// Drain bookkeeping for `~TraceCollector`: bump the in-flight counter
    /// before checking `shutdown`, so the closer either sees us in-flight
    /// (and waits) or we observe `shutdown` and bail out.
    in_flight.fetch_add(1);
    SCOPE_EXIT(in_flight.fetch_sub(1));
    if (shutdown.load())
        return;

    constexpr size_t buf_size = sizeof(char) /// TraceCollector stop flag
        + sizeof(UInt64)                     /// Profile trace subscription ID
        + sizeof(UInt8)                      /// String size
        + QUERY_ID_MAX_LEN                   /// Maximum query_id length
        + sizeof(UInt8)                      /// Number of stack frames
        + sizeof(FramePointers)              /// Collected stack trace, maximum capacity
        + sizeof(TraceType)                  /// trace type
        + sizeof(Int32)                      /// cpu_id (the signed result of get_cpuid)
        + sizeof(UInt64)                     /// thread_id
        + sizeof(ThreadName)                 /// thread name enum
        + sizeof(Int64)                      /// size
        + sizeof(void *)                     /// ptr
        + sizeof(UInt8)                      /// memory_context
        + sizeof(UInt8)                      /// memory_blocked_context
        + sizeof(ProfileEvents::Event)       /// event
        + sizeof(ProfileEvents::Count);      /// increment

    /// Write should be atomic to avoid overlaps
    /// (since recursive collect() is possible)
    static_assert(PIPE_BUF >= 512);
    static_assert(buf_size <= 512, "Trace records must fit in the portable 512-byte atomic pipe write limit");

    char buffer[buf_size];
    WriteBufferFromFileDescriptorDiscardOnFailure out(pipe.fds_rw[1], buf_size, buffer);

    std::string_view query_id;
    Int32 cpu_id = CPU::get_cpuid();
    UInt64 thread_id = 0;

    if (CurrentThread::isInitialized())
    {
        query_id = CurrentThread::getQueryId();
        if (query_id.size() > QUERY_ID_MAX_LEN)
            query_id.remove_suffix(query_id.size() - QUERY_ID_MAX_LEN);

        thread_id = CurrentThread::get().thread_id;
    }
    else if (const auto * main_thread = MainThreadStatus::get())
    {
        thread_id = main_thread->thread_id;
    }

    writeChar(false, out);  /// true if requested to stop the collecting thread.
    writePODBinary(
        CurrentThread::isInitialized() && !ProfileTracesBlocker::isBlocked() ? CurrentThread::get().getProfileTracesId() : UInt64{0}, out);

    writeBinary(static_cast<uint8_t>(query_id.size()), out);
    out.write(query_id.data(), query_id.size());

    size_t stack_trace_size = stack_trace.getSize();
    size_t stack_trace_offset = stack_trace.getOffset();
    writeIntBinary(static_cast<UInt8>(stack_trace_size - stack_trace_offset), out);
    for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
        writePODBinary(stack_trace.getFramePointers()[i], out);

    writePODBinary(trace_type, out);
    writePODBinary(cpu_id, out);
    writePODBinary(thread_id, out);
    writePODBinary(UInt8(getThreadName()), out);

    writePODBinary(extras.size, out);
    writePODBinary(UInt64(extras.ptr), out);
    if (extras.memory_context.has_value())
        writePODBinary(static_cast<Int8>(extras.memory_context.value()), out);
    else
        writePODBinary(static_cast<Int8>(MEMORY_CONTEXT_UNKNOWN), out);
    if (extras.memory_blocked_context.has_value())
        writePODBinary(static_cast<Int8>(extras.memory_blocked_context.value()), out);
    else
        writePODBinary(static_cast<Int8>(MEMORY_CONTEXT_UNKNOWN), out);
    writePODBinary(extras.event, out);
    writePODBinary(extras.increment, out);

    out.next();
    out.finalize();

    /// Multiple threads are calling this function concurrently, so writes to pipe should be atomic (single flush).
    chassert(out.getFlushCount() == 1);
}

TraceSender::ProfileTracesFlushResult TraceSender::flushProfileTraces(
    UInt64 subscription_id, std::chrono::steady_clock::time_point deadline)
{
    in_flight.fetch_add(1);
    SCOPE_EXIT(in_flight.fetch_sub(1));
    if (shutdown.load())
        return ProfileTracesFlushResult::NotRunning;

    /// Keep the marker in one atomic write, just like a sampled stack. The ordinary sender
    /// drops samples on a full pipe, but a final drain needs a delivered ordering marker.
    char buffer[1 + sizeof(subscription_id)];
    buffer[0] = 2;
    memcpy(buffer + 1, &subscription_id, sizeof(subscription_id));
    fiu_do_on(FailPoints::profile_traces_flush_write_timeout, { deadline = std::chrono::steady_clock::now(); });
    while (true)
    {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0)
            return ProfileTracesFlushResult::TimedOut;

        auto written = ::write(pipe.fds_rw[1], buffer, sizeof(buffer));
        if (written == static_cast<ssize_t>(sizeof(buffer)))
            return ProfileTracesFlushResult::MarkerSent;
        if (written < 0 && errno != EINTR && errno != EAGAIN)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot flush profile traces");
        if (written >= 0)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Incomplete profile trace flush marker");

        pollfd descriptor{pipe.fds_rw[1], POLLOUT, 0};
        int result = ::poll(&descriptor, 1, static_cast<int>(remaining));
        if (result < 0 && errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot wait to flush profile traces");
    }
}

}
