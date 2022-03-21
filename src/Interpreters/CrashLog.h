#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>


/// Call this function on crash.
void collectCrashLog(Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace);


namespace DB
{

/** Information about fatal errors that lead to crash.
  * Before crash we are writing info into system table for further analysis.
  */
struct CrashLogElement
{
    time_t event_time{};
    UInt64 timestamp_ns{};
    Int32 signal{};
    UInt64 thread_id{};
    String query_id;
    Array trace;
    Array trace_full;

    static std::string name() { return "CrashLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class CrashLog : public SystemLog<CrashLogElement>
{
    using SystemLog<CrashLogElement>::SystemLog;
    friend void ::collectCrashLog(Int32, UInt64, const String &, const StackTrace &);

    static std::weak_ptr<CrashLog> crash_log;

public:
    static void initialize(std::shared_ptr<CrashLog> crash_log_)
    {
        crash_log = crash_log_;
    }
};

}
