#pragma once

#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>

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

}
