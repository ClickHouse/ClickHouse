#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/OpenTelemetryTraceContext.h>

namespace DB
{

struct OpenTelemetrySpanLogElement : public OpenTelemetrySpan
{
    OpenTelemetrySpanLogElement() = default;
    OpenTelemetrySpanLogElement(const OpenTelemetrySpan & span)
        : OpenTelemetrySpan(span) {}

    static std::string name() { return "OpenTelemetrySpanLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

// OpenTelemetry standartizes some Log data as well, so it's not just
// OpenTelemetryLog to avoid confusion.
class OpenTelemetrySpanLog : public SystemLog<OpenTelemetrySpanLogElement>
{
public:
    using SystemLog<OpenTelemetrySpanLogElement>::SystemLog;
};

typedef std::shared_ptr<OpenTelemetrySpanLog> OpenTelemetrySpanLogPtr;

struct OpenTelemetrySpanHolder : public OpenTelemetrySpan
{
    OpenTelemetrySpanHolder(const OpenTelemetryTraceContext& _trace_context, OpenTelemetrySpanLogPtr _span_log, const std::string & _operation_name);
    explicit OpenTelemetrySpanHolder(const std::string & _operation_name);
    void addAttribute(const std::string& name, UInt64 value);
    void addAttribute(const std::string& name, const std::string& value);
    void addAttribute(const Exception & e);
    void addAttribute(std::exception_ptr e);

    void finish();
    ~OpenTelemetrySpanHolder();
};

}
