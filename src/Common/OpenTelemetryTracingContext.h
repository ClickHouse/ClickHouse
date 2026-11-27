#pragma once

#include <Core/UUID.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

namespace OpenTelemetry
{

/// See https://www.w3.org/TR/trace-context/ for trace_flags definition
enum TraceFlags : UInt8
{
    TRACE_FLAG_NONE = 0,
    TRACE_FLAG_SAMPLED = 1,
    TRACE_FLAG_KEEPER_SPANS = 2,
};

/// The runtime info we need to create new OpenTelemetry spans.
struct TracingContext
{
    UUID trace_id;
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;
    UInt8 trace_flags = TRACE_FLAG_NONE;

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(std::string_view traceparent, String & error);
    String composeTraceparentHeader() const;

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }

    /// Generate a new random non-zero trace ID
    static UUID generateTraceId();

    /// Generate a new random span ID
    static UInt64 generateSpanId();

    void deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
};

}

}
