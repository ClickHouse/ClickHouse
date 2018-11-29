#pragma once

#include <Interpreters/SystemLog.h>


namespace ProfileEvents
{
    class Counters;
}


namespace DB
{

struct QueryThreadLogElement
{
    time_t event_time{};
    /// When query was attached to current thread
    time_t query_start_time{};
    /// Real time spent by the thread to execute the query
    UInt64 query_duration_ms{};

    /// The data fetched from DB in current thread to execute the query
    UInt64 read_rows{};
    UInt64 read_bytes{};

    /// The data written to DB
    UInt64 written_rows{};
    UInt64 written_bytes{};

    Int64 memory_usage{};
    Int64 peak_memory_usage{};

    String thread_name;
    UInt32 thread_number{};
    Int32 os_thread_id{};
    UInt32 master_thread_number{};
    Int32 master_os_thread_id{};

    String query;
    ClientInfo client_info;

    std::shared_ptr<ProfileEvents::Counters> profile_counters;

    static std::string name() { return "QueryThreadLog"; }

    static Block createBlock();
    void appendToBlock(Block & block) const;
};


class QueryThreadLog : public SystemLog<QueryThreadLogElement>
{
    using SystemLog<QueryThreadLogElement>::SystemLog;
};


}



