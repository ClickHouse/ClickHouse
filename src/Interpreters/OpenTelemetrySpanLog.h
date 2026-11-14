#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <string_view>
#include <utility>

namespace DB
{

struct OpenTelemetrySpanLogElement
{
    OpenTelemetry::Span span;

    static std::string name() { return "OpenTelemetrySpanLog"; }

    /// {former name, current name} of the columns renamed with the former name kept as an `ALIAS`.
    static constexpr std::pair<std::string_view, std::string_view> renamed_columns[] = {{"finish_date", "event_date"}};

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

    static const char * getDefaultOrderBy() { return "event_date, finish_time_us"; }
};

}
