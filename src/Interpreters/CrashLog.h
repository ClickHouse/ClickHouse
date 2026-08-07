#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Core/Field.h>
#include <Storages/ColumnsDescription.h>
#include <Common/FramePointers.h>


class StackTrace;

/// Call this function on crash.
void collectCrashLog(
    Int32 signal,
    Int32 signal_code,
    UInt64 thread_id,
    const String & query_id,
    const String & query,
    const StackTrace & stack_trace,
    std::optional<UInt64> fault_address,
    const String & fault_access_type,
    const String & signal_description,
    const FramePointers & current_exception_trace,
    size_t current_exception_trace_size);


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
    Int32 signal_code{};
    UInt64 thread_id{};
    String query_id;
    String query;
    Array trace;
    Array trace_full;
    std::optional<UInt64> fault_address;
    String fault_access_type;
    String signal_description;
    Array current_exception_trace_full;
    String git_hash;
    String architecture;

    static std::string name() { return "CrashLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class CrashLog : public SystemLog<CrashLogElement>
{
    using SystemLog<CrashLogElement>::SystemLog;
    friend void ::collectCrashLog(Int32, Int32, UInt64, const String &, const String &, const StackTrace &, std::optional<UInt64>, const String &, const String &, const FramePointers &, size_t);

    static std::weak_ptr<CrashLog> crash_log;

public:
    static void initialize(std::shared_ptr<CrashLog> crash_log_)
    {
        crash_log = crash_log_;
    }

    static consteval size_t getDefaultMaxSize() { return 1024; }
    static consteval size_t getDefaultReservedSize() { return 1024; }
    static consteval size_t getDefaultFlushIntervalMilliseconds() { return 1000; }
    static consteval size_t shouldNotifyFlushOnCrash() { return true; }
};

}
