#pragma once

#include <Core/UUID.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

/// The parsed arguments of traceView. The table function validates them; the table it returns
/// renders the trace from them when it is read.
struct TraceViewArguments
{
    static constexpr UInt64 default_timeline_width = 40;
    static constexpr UInt64 max_timeline_width = 1024;

    /// Exactly one of trace_id and query_id is set; a query_id is resolved to the
    /// most recent trace of that query when the table is read.
    UUID trace_id{};
    String query_id;
    UInt64 timeline_width = default_timeline_width;
    String cluster;
    /// `YYYY-MM-DD` bounds on `finish_date`, inclusive; empty means unbounded.
    String since;
    String until;
};

/// traceView(trace_id [, timeline_width [, cluster]] [, since = date] [, until = date])
/// renders the spans of one trace from `system.opentelemetry_span_log` as a call tree with a timeline
/// Made for debugging traced queries: an over-long or ERROR phase is visible at a glance.
class TableFunctionTraceView : public ITableFunction
{
public:
    static constexpr auto name = "traceView";

    std::string getName() const override { return name; }

private:
    /// Returns a table that reads the span log and renders the trace when it is read, not here:
    /// `CREATE TABLE ... AS traceView(...)` re-executes the function whenever the table is loaded,
    /// on a server start or when a `Replicated` database recovers a replica, and those run on
    /// threads that cannot execute a query and at a time when the spans may be gone.
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// No underlying storage engine
        return "";
    }

    /// The result columns are fixed, so `CREATE TABLE ... AS traceView(...)` needs no proxy storage to find them.
    bool hasStaticStructure() const override { return true; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    TraceViewArguments arguments;
};

}
