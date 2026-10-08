#include <Storages/System/StorageSystemKeeperSnapshots.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#if USE_NURAFT

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
        {"exists_on_disk", DataTypeFactory::instance().get("Bool"), "Whether the snapshot file is currently present on disk. Always true for finalized snapshots (`is_received` = false) unless the file was removed out of band or is corrupted; may be false for snapshots being received (`is_received` = true) before any bytes are written."},
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
        bool exists_on_disk = false;
        try
        {
            if (entry.disk->existsFile(entry.path))
            {
                exists_on_disk = true;
                size_bytes = entry.disk->getFileSize(entry.path);
                last_modified_at = static_cast<UInt32>(entry.disk->getLastModified(entry.path).epochTime());
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
        res_columns[i++]->insert(static_cast<UInt8>(exists_on_disk));
    }
}

}


/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemKeeperSnapshots) }

#endif
