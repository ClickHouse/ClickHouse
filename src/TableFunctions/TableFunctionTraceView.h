#pragma once

#include <Core/UUID.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

/// traceView(trace_id [, timeline_width [, cluster]] [, since = date] [, until = date]) 
/// renders the spans of one trace from `system.opentelemetry_span_log` as a call tree with a timeline
/// Made for debugging traced queries: an over-long or ERROR phase is visible at a glance.
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

    /// The result columns are fixed, so `CREATE TABLE ... AS traceView(...)` needs no lazy proxy storage.
    bool hasStaticStructure() const override { return true; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    /// The table the spans are read from: the local span log, or the span log of every replica of `cluster`.
    String spanLogSource() const;

    /// The `finish_date` window of `since` and `until` as an ` AND ...` condition on the span log, or empty.
    String spanLogTimeFilter() const;

    /// The trace to render: `trace_id`, or the most recent trace of `query_id` looked up in `source`.
    UUID resolveTraceId(const String & source, const String & time_filter, ContextPtr context) const;

    /// Exactly one of trace_id and query_id is set; a query_id is resolved to the
    /// most recent trace of that query when the function executes.
    UUID trace_id{};
    String query_id;
    UInt64 timeline_width = default_timeline_width;
    String cluster;
    /// `YYYY-MM-DD` bounds on `finish_date`, inclusive; empty means unbounded.
    String since;
    String until;
};

}
