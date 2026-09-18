#pragma once

#include <Core/UUID.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

/// traceView(trace_id [, timeline_width [, cluster]]) - renders the spans of one trace from
/// `system.opentelemetry_span_log` as a call tree with a timeline: one row per span, the tree
/// on the left (`span`), and a fixed-width bar (`timeline`) whose position is the span's start
/// offset within the trace and whose length is proportional to its duration. Made for debugging
/// traced queries: an over-long or ERROR phase is visible at a glance.
class TableFunctionTraceView : public ITableFunction
{
public:
    static constexpr auto name = "traceView";
    static constexpr UInt64 default_timeline_width = 40;
    static constexpr UInt64 max_timeline_width = 1024;

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// No underlying storage engine
        return "";
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    /// The table the spans are read from: the local span log, or the span log of every replica of `cluster`.
    String spanLogSource() const;

    /// The trace to render: `trace_id`, or the most recent trace of `query_id` looked up in `source`.
    UUID resolveTraceId(const String & source, ContextPtr context) const;

    /// Exactly one of trace_id and query_id is set; a query_id is resolved to the
    /// most recent trace of that query when the function executes.
    UUID trace_id{};
    String query_id;
    UInt64 timeline_width = default_timeline_width;
    String cluster;
};

}
