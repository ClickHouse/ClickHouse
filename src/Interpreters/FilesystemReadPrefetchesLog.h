#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Common/Stopwatch.h>

namespace DB
{

enum class FilesystemPrefetchState
{
    USED,
    CANCELLED_WITH_SEEK,
    CANCELLED_WITH_RANGE_CHANGE,
    UNNEEDED,
};

struct FilesystemReadPrefetchesLogElement
{
    time_t event_time{};
    String query_id;
    String path;
    UInt64 offset;
    Int64 size; /// -1 means unknown
    Decimal64 prefetch_submit_time{};
    std::optional<Stopwatch> execution_watch;
    size_t priority;
    FilesystemPrefetchState state;
    UInt64 thread_id;
    String reader_id;

    static std::string name() { return "FilesystemReadPrefetchesLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class FilesystemReadPrefetchesLog : public SystemLog<FilesystemReadPrefetchesLogElement>
{
public:
    using SystemLog<FilesystemReadPrefetchesLogElement>::SystemLog;
};

}
