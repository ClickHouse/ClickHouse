#include <Interpreters/BlobStorageLog.h>
#include <base/getFQDNOrHostName.h>

#include <Common/DateLUTImpl.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDate.h>

#include <Interpreters/Context.h>

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

ColumnsDescription BlobStorageLogElement::getColumnsDescription()
{
    auto event_enum_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values{
            {"Upload", static_cast<Int8>(EventType::Upload)},
            {"Delete", static_cast<Int8>(EventType::Delete)},
            {"MultiPartUploadCreate", static_cast<Int8>(EventType::MultiPartUploadCreate)},
            {"MultiPartUploadWrite", static_cast<Int8>(EventType::MultiPartUploadWrite)},
            {"MultiPartUploadComplete", static_cast<Int8>(EventType::MultiPartUploadComplete)},
            {"MultiPartUploadAbort", static_cast<Int8>(EventType::MultiPartUploadAbort)},
            {"Read", static_cast<Int8>(EventType::Read)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the event."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the event."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the event with microseconds precision."},

        {"event_type", event_enum_type, "Type of the event. Possible values: 'Upload', 'Delete', 'MultiPartUploadCreate', 'MultiPartUploadWrite', 'MultiPartUploadComplete', 'MultiPartUploadAbort', 'Read'"},

        {"query_id", std::make_shared<DataTypeString>(), "Identifier of the query associated with the event, if any."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Identifier of the thread performing the operation."},
        {"thread_name", std::make_shared<DataTypeString>(), "Name of the thread performing the operation."},

        {"disk_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of the associated disk."},
        {"bucket", std::make_shared<DataTypeString>(), "Name of the bucket."},
        {"remote_path", std::make_shared<DataTypeString>(), "Path to the remote resource."},
        {"local_path", std::make_shared<DataTypeString>(), "Path to the metadata file on the local system, which references the remote resource."},
        {"data_size", std::make_shared<DataTypeUInt64>(), "Size of the data involved in the upload event."},
        {"elapsed_microseconds", std::make_shared<DataTypeUInt64>(), "Elapsed time for the operation, in microseconds."},

        {"connection_id", std::make_shared<DataTypeUInt64>(), "Identifier of the HTTP connection that carried the operation, unique within the server process. Group by this to follow one connection: the OS recycles ports and socket inodes as soon as a socket closes, so those cannot tell consecutive connections apart. 0 if the operation used no HTTP connection."},
        {"connection_local_port", std::make_shared<DataTypeUInt16>(), "Local TCP port of that connection. Unique only among sockets open at the same moment; use it to join against a packet capture or a /proc/net/tcp row taken at the time."},
        {"connection_socket_inode", std::make_shared<DataTypeUInt64>(), "Socket inode of that connection, as reported by fstat. Same caveat as the port: unique only while the socket is open."},
        {"connection_requests", std::make_shared<DataTypeUInt32>(), "Number of requests the connection had already sent before this one. 0 means this was its first request."},
        {"connection_age_microseconds", std::make_shared<DataTypeUInt64>(), "Time since the connection's TCP socket was established. Reset by every reconnect, so it measures the life of the socket rather than of the session object."},
        {"connection_idle_microseconds", std::make_shared<DataTypeUInt64>(), "Time the connection sat idle in the pool before being handed out for this operation. 0 on a connection's first request."},

        {"error_code", std::make_shared<DataTypeInt32>(), "Error code of the operation. 0 if there was no error."},
        {"error", std::make_shared<DataTypeString>(), "Error message associated with the event, if any."},
    };
}

void BlobStorageLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    auto event_time_seconds = timeInSeconds(event_time);
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[i++]->insert(event_time_seconds);
    columns[i++]->insert(Decimal64(timeInMicroseconds(event_time)));
    columns[i++]->insert(static_cast<Int8>(event_type));
    columns[i++]->insert(query_id);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(toString(thread_name));
    columns[i++]->insert(disk_name);
    columns[i++]->insert(bucket);
    columns[i++]->insert(remote_path);
    columns[i++]->insert(local_path);
    columns[i++]->insert(data_size);
    columns[i++]->insert(elapsed_microseconds);
    columns[i++]->insert(connection_id);
    columns[i++]->insert(connection_local_port);
    columns[i++]->insert(connection_socket_inode);
    columns[i++]->insert(connection_requests);
    columns[i++]->insert(connection_age_microseconds);
    columns[i++]->insert(connection_idle_microseconds);
    columns[i++]->insert(error_code);
    columns[i++]->insert(error_message);
}

void BlobStorageLog::addSettingsForQuery(ContextMutablePtr & mutable_context, IAST::QueryKind query_kind) const
{
    SystemLog<BlobStorageLogElement>::addSettingsForQuery(mutable_context, query_kind);

    if (query_kind == IAST::QueryKind::Insert)
    {
        mutable_context->setSetting("enable_blob_storage_log", false);
        mutable_context->setSetting("enable_blob_storage_log_for_read_operations", false);
    }
}

static std::string_view normalizePath(std::string_view path)
{
    if (path.starts_with("./"))
        path.remove_prefix(2);
    if (path.ends_with("/"))
        path.remove_suffix(1);
    return path;
}

void BlobStorageLog::prepareTable()
{
    SystemLog<BlobStorageLogElement>::prepareTable();
    if (auto merge_tree_table = std::dynamic_pointer_cast<MergeTreeData>(getStorage()))
    {
        std::unique_lock lock{prepare_mutex};
        const auto & relative_data_path = merge_tree_table->getRelativeDataPath();
        prefix_to_ignore = normalizePath(relative_data_path);
        LOG_DEBUG(log, "Will ignore blobs with prefix {}", prefix_to_ignore);
    }
}

}
