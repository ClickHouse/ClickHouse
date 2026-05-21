#include <Storages/System/StorageSystemKeeperSnapshots.h>

#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperStateMachine.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{

ColumnsDescription StorageSystemKeeperSnapshots::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"last_log_index", std::make_shared<DataTypeUInt64>(), "Last log index covered by the snapshot."},
        {"path", std::make_shared<DataTypeString>(), "Snapshot file path on the disk."},
        {"disk_name", std::make_shared<DataTypeString>(), "Name of the disk that stores the snapshot."},
        {"size_bytes", std::make_shared<DataTypeUInt64>(), "Size of the snapshot file on disk."},
        {"last_modified_at", std::make_shared<DataTypeDateTime>(), "Last modification time of the snapshot file."},
        {"is_received", DataTypeFactory::instance().get("Bool"), "True if the snapshot is currently being received from the leader. For such rows `size_bytes` and `last_modified_at` reflect the partial file as written so far and may underreport."},
    };
}

void StorageSystemKeeperSnapshots::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto dispatcher = context->tryGetKeeperDispatcher();
    if (!dispatcher)
        return;

    auto entries = dispatcher->getStateMachine().getSnapshotsStatus();

    auto log = getLogger("SystemKeeperSnapshots");

    for (const auto & entry : entries)
    {
        UInt64 size_bytes = 0;
        UInt32 last_modified_at = 0;
        try
        {
            if (entry.disk->existsFile(entry.path))
            {
                size_bytes = entry.disk->getFileSize(entry.path);
                last_modified_at = static_cast<UInt32>(entry.disk->getLastModified(entry.path).epochTime());
            }
            else if (!entry.is_received)
            {
                LOG_WARNING(log, "Finalized snapshot file {} on disk {} is missing", entry.path, entry.disk->getName());
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to stat snapshot file {} on disk {}", entry.path, entry.disk->getName()));
        }

        size_t i = 0;
        res_columns[i++]->insert(entry.last_log_index);
        res_columns[i++]->insert(entry.path);
        res_columns[i++]->insert(entry.disk->getName());
        res_columns[i++]->insert(size_bytes);
        res_columns[i++]->insert(last_modified_at);
        res_columns[i++]->insert(static_cast<UInt8>(entry.is_received));
    }
}

}
