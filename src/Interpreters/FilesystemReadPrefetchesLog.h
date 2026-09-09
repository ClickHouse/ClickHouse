#pragma once

#include <Common/Priority.h>
#include <Common/Stopwatch.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

enum class FilesystemPrefetchState : uint8_t
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
    std::chrono::system_clock::time_point prefetch_submit_time;
    std::optional<Stopwatch> execution_watch;
    Priority priority;
    FilesystemPrefetchState state;
    UInt64 thread_id;
    String reader_id;

    static std::string name() { return "FilesystemReadPrefetchesLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class FilesystemReadPrefetchesLog : public SystemLog<FilesystemReadPrefetchesLogElement>
{
public:
    using SystemLog<FilesystemReadPrefetchesLogElement>::SystemLog;
};

using FilesystemReadPrefetchesLogPtr = std::shared_ptr<FilesystemReadPrefetchesLog>;

}
