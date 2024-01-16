#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct MemoryDumpLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 timestamp_ns{};
    UInt64 thread_id{};
    String query_id{};
    Array trace{};
    UInt64 size{};
    UInt64 ptr{};

    static std::string name() { return "MemoryDumpLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class MemoryDumpLog : public SystemLog<MemoryDumpLogElement>
{
    using SystemLog<MemoryDumpLogElement>::SystemLog;
};

}
