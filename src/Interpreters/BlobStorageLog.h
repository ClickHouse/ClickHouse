#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Poco/Message.h>
#include <Storages/ColumnsDescription.h>
#include <chrono>

namespace DB
{

struct BlobStorageLogElement
{
    enum class EventType : int8_t
    {
        Upload = 1,
        Delete = 2,
        MultiPartUploadCreate = 3,
        MultiPartUploadWrite = 4,
        MultiPartUploadComplete = 5,
        MultiPartUploadAbort = 6,
    };

    EventType event_type;

    String query_id;
    UInt64 thread_id = 0;
    String thread_name;

    String disk_name;
    String bucket;
    String remote_path;
    String local_path;

    size_t data_size;

    Int32 error_code = -1; /// negative if no error
    String error_message;

    using EvenTime = std::chrono::time_point<std::chrono::system_clock>;
    EvenTime event_time;

    static std::string name() { return "BlobStorageLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class BlobStorageLog : public SystemLog<BlobStorageLogElement>
{
    using SystemLog<BlobStorageLogElement>::SystemLog;
};

}
