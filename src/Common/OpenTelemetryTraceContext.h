#pragma once

namespace DB
{

// The runtime info we need to create new OpenTelemetry spans.
struct OpenTelemetryTraceContext
{
    UUID trace_id{};
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;
    UInt8 trace_flags = 0;

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(const std::string & traceparent, std::string & error);
    std::string composeTraceparentHeader() const;
};

}
