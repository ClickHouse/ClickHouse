#pragma once

#include <Common/ProfileEvents.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct S3QueueLogElement
{
    time_t event_time{};

    std::string database;
    std::string table;
    std::string uuid;

    std::string file_name;
    size_t rows_processed = 0;

    enum class S3QueueStatus : uint8_t
    {
        Processed,
        Failed,
    };
    S3QueueStatus status;
    ProfileEvents::Counters::Snapshot counters_snapshot;
    time_t processing_start_time;
    time_t processing_end_time;
    std::string exception;

    static std::string name() { return "S3QueueLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class S3QueueLog : public SystemLog<S3QueueLogElement>
{
    using SystemLog<S3QueueLogElement>::SystemLog;
};

}
