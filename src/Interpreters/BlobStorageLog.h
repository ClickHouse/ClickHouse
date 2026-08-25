#pragma once

#include <chrono>
#include <shared_mutex>

#include <Poco/Message.h>

#include <Common/setThreadName.h>
#include <Common/SharedMutex.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

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
        Read = 7,
    };

    EventType event_type{};

    String query_id;
    UInt64 thread_id = 0;
    ThreadName thread_name{};

    String disk_name;
    String bucket;
    String remote_path;
    String local_path;

    size_t data_size{};
    size_t elapsed_microseconds{};

    /// The HTTP connection this operation went over, and how worn it was. Zero for operations
    /// that used no HTTP connection (local object storage) and for the tail events of a batch
    /// that shared one request. See Common/HTTPConnectionInfo.h.
    UInt64 connection_id = 0;
    UInt16 connection_local_port = 0;
    UInt64 connection_socket_inode = 0;
    UInt32 connection_requests = 0;
    UInt64 connection_age_microseconds = 0;
    UInt64 connection_idle_microseconds = 0;

    Int32 error_code = 0; /// 0 if no error
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
public:
    using SystemLog<BlobStorageLogElement>::SystemLog;

    /// We should not log events for table itself to avoid infinite recursion
    bool shouldIgnorePath(const String & path) const
    {
        std::shared_lock lock{prepare_mutex};
        return !prefix_to_ignore.empty() && path.starts_with(prefix_to_ignore);
    }

protected:
    void prepareTable() override;
    void addSettingsForQuery(ContextMutablePtr & mutable_context, IAST::QueryKind query_kind) const override;

private:
    mutable SharedMutex prepare_mutex;
    String prefix_to_ignore;
};

}
