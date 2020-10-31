#pragma once

#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>

namespace DB
{

struct Settings;
class Context;
class IInterpreter;

/// Everything related to:
/// - query logging into log
/// - query logging into query_log/query_thread_log
/// - query process (system.processes)
class QueryProcess
{
public:
    QueryProcess(const String & query_for_logging_, const ASTPtr & ast_, bool internal_, Context & context_);
    QueryProcess(const ASTPtr & ast_, bool internal_, Context & context_);
    ~QueryProcess();

    QueryStatus & queryStatus() { return process_list_entry->get(); }

    CurrentThread::QueryScope & queryScope() { return *query_scope; }

    void queryExceptionBeforeStart();

    /// Log into system table start of query execution, if need.
    void queryStart(bool use_processors = true, IInterpreter * interpreter = nullptr);

    /// Also make possible for caller to log successful query finish and exception during execution.
    void queryFinish(IBlockInputStream * stream_in, IBlockOutputStream * stream_out, QueryPipeline * query_pipeline);
    void queryException();

    /// Common code for finish and exception callbacks
    void addQueryStatusInfoToQueryLogElement(const QueryStatusInfo &info, const ASTPtr query_ast);

    ///
    /// Static interface
    ///

    /// Log query into text log (not into system table).
    static void logQuery(const String & query_for_logging, const Context & context, bool internal);
    static void onExceptionBeforeStart(const String & query_for_logging, Context & context,
        const std::chrono::time_point<std::chrono::system_clock> & current_time, const ASTPtr & ast);

    /// Prepares query for logging:
    /// - wipe sensitive data
    /// - trim to log_queries_cut_to_length
    static String prepareQueryForLogging(const String & query, const Context & context);

private:
    Context & context;
    const Settings & settings;
    String query_for_logging;
    const ASTPtr ast;
    ProcessList::EntryPtr process_list_entry;
    QueryStatus * parent_process_list_element;
    bool internal;
    bool log_queries;
    std::chrono::time_point<std::chrono::system_clock> current_time;
    QueryLogElement query_log_element;

    String query_database;
    String query_table;

    std::unique_ptr<CurrentThread::QueryScope> query_scope;

    QueryLogElement makeQueryLogElement();

    static Context & prepareContext(Context & context);
};

}
