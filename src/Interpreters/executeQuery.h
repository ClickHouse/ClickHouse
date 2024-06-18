#pragma once

#include <Core/QueryProcessingStage.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryLog.h>
#include <QueryPipeline/BlockIO.h>

#include <memory>
#include <optional>

namespace DB
{

class IInterpreter;
class ReadBuffer;
class WriteBuffer;
class IOutputFormat;
struct QueryStatusInfo;

struct QueryResultDetails
{
    String query_id;
    std::optional<String> content_type = {};
    std::optional<String> format = {};
    std::optional<String> timezone = {};
};

using SetResultDetailsFunc = std::function<void(const QueryResultDetails &)>;
using HandleExceptionInOutputFormatFunc = std::function<void(IOutputFormat & output_format, const String & format_name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings)>;

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
};


/// Parse and execute a query.
void executeQuery(
    ReadBuffer & istr,                  /// Where to read query from (and data for INSERT, if present).
    WriteBuffer & ostr,                 /// Where to write query output to.
    bool allow_into_outfile,            /// If true and the query contains INTO OUTFILE section, redirect output to that file.
    ContextMutablePtr context,          /// DB, tables, data types, storage engines, functions, aggregate functions...
    SetResultDetailsFunc set_result_details, /// If a non-empty callback is passed, it will be called with the query id, the content-type, the format, and the timezone.
    QueryFlags flags = {},
    const std::optional<FormatSettings> & output_format_settings = std::nullopt, /// Format settings for output format, will be calculated from the context if not set.
    HandleExceptionInOutputFormatFunc handle_exception_in_output_format = {} /// If a non-empty callback is passed, it will be called on exception with created output format.
);


/// More low-level function for server-to-server interaction.
/// Prepares a query for execution but doesn't execute it.
/// Returns a pair of parsed query and BlockIO which, when used, will result in query execution.
/// This means that the caller can to the extent control the query execution pipeline.
///
/// To execute:
/// * if present, write INSERT data into BlockIO::out
/// * then read the results from BlockIO::in.
///
/// If the query doesn't involve data insertion or returning of results, out and in respectively
/// will be equal to nullptr.
///
/// Correctly formatting the results (according to INTO OUTFILE and FORMAT sections)
/// must be done separately.
std::pair<ASTPtr, BlockIO> executeQuery(
    const String & query,     /// Query text without INSERT data. The latter must be written to BlockIO::out.
    ContextMutablePtr context,       /// DB, tables, data types, storage engines, functions, aggregate functions...
    QueryFlags flags = {},
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete    /// To which stage the query must be executed.
);

/// Executes BlockIO returned from executeQuery(...)
/// if built pipeline does not require any input and does not produce any output.
void executeTrivialBlockIO(BlockIO & streams, ContextPtr context);

/// Prepares a QueryLogElement and, if enabled, logs it to system.query_log
QueryLogElement logQueryStart(
    const std::chrono::time_point<std::chrono::system_clock> & query_start_time,
    const ContextMutablePtr & context,
    const String & query_for_logging,
    const ASTPtr & query_ast,
    const QueryPipeline & pipeline,
    const std::unique_ptr<IInterpreter> & interpreter,
    bool internal,
    const String & query_database,
    const String & query_table,
    bool async_insert);

void logQueryFinish(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    const QueryPipeline & query_pipeline,
    bool pulling_pipeline,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    QueryCache::Usage query_cache_usage,
    bool internal);

void logQueryException(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const Stopwatch & start_watch,
    const ASTPtr & query_ast,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    bool internal,
    bool log_error);

void logExceptionBeforeStart(
    const String & query_for_logging,
    ContextPtr context,
    ASTPtr ast,
    const std::shared_ptr<OpenTelemetry::SpanHolder> & query_span,
    UInt64 elapsed_millliseconds);
}
