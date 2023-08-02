#pragma once


#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Poco/Message.h>
#include <chrono>

namespace Aws::S3
{
    class S3Error;
}

namespace DB
{

struct BlobStorageLogElement
{
    enum class EventType : Int8
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

    String disk_name;
    String bucket;
    String remote_path;
    String referring_local_path;

    Int32 error_code = -1; /// negative if no error
    String error_msg;

    using EvenTime = std::chrono::time_point<std::chrono::system_clock>;
    EvenTime event_time;

    static std::string name() { return "BlobStorageLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};


class BlobStorageLog : public SystemLog<BlobStorageLogElement>
{
    using SystemLog<BlobStorageLogElement>::SystemLog;
};

using BlobStorageLogPtr = std::shared_ptr<BlobStorageLog>;

/// Writes events to BlobStorageLog
/// May contains some context information
class BlobStorageLogWriter
{
    BlobStorageLogPtr log;

public:
    BlobStorageLogWriter() = default;

    BlobStorageLogWriter(BlobStorageLogPtr log_)
        : log(std::move(log_))
    {}

    explicit BlobStorageLogWriter(
        BlobStorageLogPtr log_,
        const String & disk_name_,
        const String & query_id_,
        const String & referring_local_path_)
        : log(std::move(log_))
        , disk_name(disk_name_)
        , query_id(query_id_)
        , referring_local_path(referring_local_path_)
    {}

    void addEvent(
        BlobStorageLogElement::EventType event_type,
        const String & bucket,
        const String & remote_path,
        const String & local_path,
        const Aws::S3::S3Error * error,
        BlobStorageLogElement::EvenTime time_now = {});

    /// Optional context information
    String disk_name;
    String query_id;
    String referring_local_path;
};

}
