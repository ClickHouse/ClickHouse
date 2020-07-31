#pragma once

#include <Interpreters/SystemLog.h>

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
    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};

class CrashLog : public SystemLog<CrashLogElement>
{
    using SystemLog<CrashLogElement>::SystemLog;

    static std::weak_ptr<CrashLog> crash_log;

public:
    static void initialize(std::shared_ptr<CrashLog> crash_log_)
    {
        crash_log = std::move(crash_log_);
    }

    static void collect(const CrashLogElement & element)
    {
        if (auto crash_log_owned = crash_log.lock())
            crash_log_owned->add(element);
    }
};

}

