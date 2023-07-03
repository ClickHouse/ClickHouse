#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>

namespace DB
{

struct OpenTelemetrySpanLogElement : public OpenTelemetry::Span
{
    OpenTelemetrySpanLogElement() = default;
    OpenTelemetrySpanLogElement(const OpenTelemetry::Span & span)
        : OpenTelemetry::Span(span) {}

    static std::string name() { return "OpenTelemetrySpanLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

// OpenTelemetry standardizes some Log data as well, so it's not just
// OpenTelemetryLog to avoid confusion.
class OpenTelemetrySpanLog : public SystemLog<OpenTelemetrySpanLogElement>
{
public:
    using SystemLog<OpenTelemetrySpanLogElement>::SystemLog;
};

}
