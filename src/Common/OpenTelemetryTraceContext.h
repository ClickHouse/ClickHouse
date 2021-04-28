#pragma once

namespace DB
{

// The runtime info we need to create new OpenTelemetry spans.
struct OpenTelemetryTraceContext
{
    __uint128_t trace_id = 0;
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;

#if defined(OS_SUNOS)
    uint8_t trace_flags = 0;
#else
    __uint8_t trace_flags = 0;
#endif

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(const std::string & traceparent, std::string & error);
    std::string composeTraceparentHeader() const;
};

}
