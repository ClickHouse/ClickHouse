#pragma once

#include <Interpreters/SystemLog.h>

namespace DB
{

struct OpenTelemetrySpan
{
    __uint128_t trace_id;
    UInt64 span_id;
    UInt64 parent_span_id;
    std::string operation_name;
    UInt64 start_time_us;
    UInt64 finish_time_us;
    Array attribute_names;
    Array attribute_values;
    // I don't understand how Links work, namely, which direction should they
    // point to, and how they are related with parent_span_id, so no Links for
    // now.
};

struct OpenTelemetrySpanLogElement : public OpenTelemetrySpan
{
    OpenTelemetrySpanLogElement() = default;
    OpenTelemetrySpanLogElement(const OpenTelemetrySpan & span)
        : OpenTelemetrySpan(span) {}

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

struct OpenTelemetrySpanHolder : public OpenTelemetrySpan
{
    OpenTelemetrySpanHolder(const std::string & _operation_name);
    ~OpenTelemetrySpanHolder();
};

}
