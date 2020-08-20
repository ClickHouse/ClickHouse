#pragma once

#include <Interpreters/SystemLog.h>

namespace DB
{

/*
struct OpenTelemetrySpanContext
{
    UInt128 trace_id;
    UInt64 span_id;
    UInt8 trace_flags;
    String trace_state;
};
*/

// TODO figure out precisely which part of this is run time, and which part we
// must log.
struct OpenTelemetrySpan
{
    UInt128 trace_id;
    UInt64 span_id;
    UInt64 parent_span_id;
    std::string operation_name;
    time_t start_time{};
    time_t finish_time{};
    Array attribute_names;
    Array attribute_values;
    // I don't understand how Links work, namely, which direction should they
    // point to, and how they are related with parent_span_id, so no Links for
    // now.

    // The following fields look like something that is runtime only and doesn't
    // require logging.
    UInt8 trace_flags;
    // Vendor-specific info, key-value pairs. Keep it as a string as described
    // here: https://w3c.github.io/trace-context/#tracestate-header
    String trace_state;
};

struct OpenTelemetrySpanLogElement : public OpenTelemetrySpan
{
    static std::string name() { return "OpenTelemetrySpanLog"; }
    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};

// OpenTelemetry standartizes some Log data as well, so it's not just
// OpenTelemetryLog to avoid confusion.
class OpenTelemetrySpanLog : public SystemLog<OpenTelemetrySpanLogElement>
{
public:
    using SystemLog<OpenTelemetrySpanLogElement>::SystemLog;
};

}
