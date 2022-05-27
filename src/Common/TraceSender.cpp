#include <Common/TraceSender.h>

#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <Common/StackTrace.h>
#include <Common/CurrentThread.h>

namespace
{
    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    ///
    /// And it cannot be large, since otherwise it will not fit into PIPE_BUF.
    /// The performance test query ids can be surprisingly long like
    /// `aggregating_merge_tree_simple_aggregate_function_string.query100.profile100`,
    /// so make some allowance for them as well.
    constexpr size_t QUERY_ID_MAX_LEN = 128;
    static_assert(QUERY_ID_MAX_LEN <= std::numeric_limits<uint8_t>::max());
}

namespace DB
{

LazyPipeFDs TraceSender::pipe;

void TraceSender::send(TraceType trace_type, const StackTrace & stack_trace, Int64 size)
{
    constexpr size_t buf_size = sizeof(char) /// TraceCollector stop flag
        + sizeof(UInt8)                      /// String size
        + QUERY_ID_MAX_LEN                   /// Maximum query_id length
        + sizeof(UInt8)                      /// Number of stack frames
        + sizeof(StackTrace::FramePointers)  /// Collected stack trace, maximum capacity
        + sizeof(TraceType)                  /// trace type
        + sizeof(UInt64)                     /// thread_id
        + sizeof(Int64);                     /// size

    /// Write should be atomic to avoid overlaps
    /// (since recursive collect() is possible)
    static_assert(PIPE_BUF >= 512);
    static_assert(buf_size <= 512, "Only write of PIPE_BUF to pipe is atomic and the minimal known PIPE_BUF across supported platforms is 512");

    char buffer[buf_size];
    WriteBufferFromFileDescriptorDiscardOnFailure out(pipe.fds_rw[1], buf_size, buffer);

    StringRef query_id;
    UInt64 thread_id;

    if (CurrentThread::isInitialized())
    {
        query_id = CurrentThread::getQueryId();
        query_id.size = std::min(query_id.size, QUERY_ID_MAX_LEN);

        thread_id = CurrentThread::get().thread_id;
    }
    else
    {
        thread_id = MainThreadStatus::get()->thread_id;
    }

    writeChar(false, out);  /// true if requested to stop the collecting thread.

    writeBinary(static_cast<uint8_t>(query_id.size), out);
    out.write(query_id.data, query_id.size);

    size_t stack_trace_size = stack_trace.getSize();
    size_t stack_trace_offset = stack_trace.getOffset();
    writeIntBinary(static_cast<UInt8>(stack_trace_size - stack_trace_offset), out);
    for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
        writePODBinary(stack_trace.getFramePointers()[i], out);

    writePODBinary(trace_type, out);
    writePODBinary(thread_id, out);
    writePODBinary(size, out);

    out.next();
}

}
