#pragma once

#include <Interpreters/SystemLog.h>


namespace ProfileEvents
{
    class Counters;
}


namespace DB
{


/** Allows to log information about queries execution:
  * - info about start of query execution;
  * - performance metrics (are set at the end of query execution);
  * - info about errors of query execution.
  */

/// A struct which will be inserted as row into query_log table
struct QueryLogElement
{
    enum Type
    {
        QUERY_START = 1,
        QUERY_FINISH = 2,
        EXCEPTION_BEFORE_START = 3,
        EXCEPTION_WHILE_PROCESSING = 4,
    };

    Type type = QUERY_START;

    /// Depending on the type of query and type of stage, not all the fields may be filled.

    time_t event_time{};
    time_t query_start_time{};
    UInt64 query_duration_ms{};

    /// The data fetched from DB to execute the query
    UInt64 read_rows{};
    UInt64 read_bytes{};

    /// The data written to DB
    UInt64 written_rows{};
    UInt64 written_bytes{};

    /// The data sent to the client
    UInt64 result_rows{};
    UInt64 result_bytes{};

    UInt64 memory_usage{};

    String query;

    String exception;
    String stack_trace;

    ClientInfo client_info;

    std::vector<UInt32> thread_numbers;
    std::shared_ptr<ProfileEvents::Counters> profile_counters;
    std::shared_ptr<Settings> query_settings;

    static std::string name() { return "QueryLog"; }

    static Block createBlock();
    void appendToBlock(Block & block) const;

    static void appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i);
};


/// Instead of typedef - to allow forward declaration.
class QueryLog : public SystemLog<QueryLogElement>
{
    using SystemLog<QueryLogElement>::SystemLog;
};

}
