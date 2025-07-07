#pragma once

#include <Common/ProfileEvents.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct ObjectStorageQueueLogElement
{
    time_t event_time{};

    std::string database;
    std::string table;
    std::string uuid;

    std::string file_name;
    size_t rows_processed = 0;

    enum class ObjectStorageQueueStatus : uint8_t
    {
        Processed,
        Failed,
    };
    ObjectStorageQueueStatus status;
    time_t processing_start_time;
    time_t processing_end_time;
    std::string exception;

    static std::string name() { return "ObjectStorageQueueLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class ObjectStorageQueueLog : public SystemLog<ObjectStorageQueueLogElement>
{
    using SystemLog<ObjectStorageQueueLogElement>::SystemLog;
};

}
