#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct OpenTelemetrySpanLogElement : public OpenTelemetry::Span
{
    OpenTelemetrySpanLogElement() = default;
    explicit OpenTelemetrySpanLogElement(const OpenTelemetry::Span & span)
        : OpenTelemetry::Span(span) {}

    static std::string name() { return "OpenTelemetrySpanLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

// OpenTelemetry standardizes some Log data as well, so it's not just
// OpenTelemetryLog to avoid confusion.
class OpenTelemetrySpanLog : public SystemLog<OpenTelemetrySpanLogElement>
{
public:
    using SystemLog<OpenTelemetrySpanLogElement>::SystemLog;
};

}
