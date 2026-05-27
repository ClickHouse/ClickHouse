#include <Storages/System/StorageSystemRemoteReadConnections.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <IO/SourceBufferLimit.h>
#include <Interpreters/Context.h>

namespace DB
{

ColumnsDescription StorageSystemRemoteReadConnections::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"object_path", std::make_shared<DataTypeString>(), "Path of the remote object being read."},
        {"local_path", std::make_shared<DataTypeString>(), "Local (data part) file the read maps to. Empty if the source isn't a `StoredObject` with a local mapping."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query holding this connection."},
        {"position", std::make_shared<DataTypeUInt64>(), "Current read position in the object (bytes)."},
        {"elapsed_seconds", std::make_shared<DataTypeFloat64>(), "Seconds since the connection was opened."},
    };
}

void StorageSystemRemoteReadConnections::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto buffer_limit = context->getSourceBufferLimit();
    auto active = buffer_limit->getActive();
    auto now = std::chrono::steady_clock::now();

    for (const auto & info : active)
    {
        res_columns[0]->insert(info.object_path);
        res_columns[1]->insert(info.local_path);
        res_columns[2]->insert(info.query_id);
        res_columns[3]->insert(info.position);

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - info.acquired_time).count();
        res_columns[4]->insert(static_cast<double>(elapsed) / 1000.0);
    }
}

}
