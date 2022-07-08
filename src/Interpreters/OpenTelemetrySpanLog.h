#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>

namespace DB
{

struct OpenTelemetrySpan
{
    UUID trace_id;
    UInt64 span_id;
    UInt64 parent_span_id;
    std::string operation_name;
    UInt64 start_time_us;
    UInt64 finish_time_us;
    Array attribute_names;
    Array attribute_values;
    // I don't understand how Links work, namely, which direction should they
    // point to, and how they are related with parent_span_id, so no Links for now.
};

struct OpenTelemetrySpanLogElement : public OpenTelemetrySpan
{
    OpenTelemetrySpanLogElement() = default;
    explicit OpenTelemetrySpanLogElement(const OpenTelemetrySpan & span)
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

struct OpenTelemetrySpanHolder : public OpenTelemetrySpan
{
    explicit OpenTelemetrySpanHolder(const std::string & _operation_name);
    void addAttribute(const std::string& name, UInt64 value);
    void addAttribute(const std::string& name, const std::string& value);
    void addAttribute(const Exception & e);
    void addAttribute(std::exception_ptr e);

    ~OpenTelemetrySpanHolder();
};

}
