#include <Storages/System/StorageSystemIcebergHistory.h>
#include <mutex>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Access/ContextAccess.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Core/Settings.h>

/// Iceberg specs mention that the timestamps are stored in ms: https://iceberg.apache.org/spec/#table-metadata-fields
static constexpr auto TIME_SCALE = 3;

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool use_iceberg_metadata_files_cache;
}

ColumnsDescription StorageSystemIcebergHistory::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database",std::make_shared<DataTypeString>(),"Database name."},
        {"table",std::make_shared<DataTypeString>(),"Table name."},
        {"made_current_at",std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),"Date & time when this snapshot was made current snapshot"},
        {"snapshot_id",std::make_shared<DataTypeUInt64>(),"Snapshot id which is used to identify a snapshot."},
        {"parent_id",std::make_shared<DataTypeUInt64>(),"Parent id of this snapshot."},
        {"is_current_ancestor",std::make_shared<DataTypeUInt8>(),"Flag that indicates if this snapshot is an ancestor of the current snapshot."}
    };
}

void StorageSystemIcebergHistory::fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_AVRO
    ContextMutablePtr context_copy = Context::createCopy(context);
    Settings settings_copy = context_copy->getSettingsCopy();
    settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
    context_copy->setSettings(settings_copy);

    const auto access = context_copy->getAccess();

    auto add_history_record = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        /// Unfortunately this try/catch is unavoidable. Iceberg tables can be broken in arbitrary way, it's impossible
        /// to handle properly all possible errors which we can get when attempting to read metadata of iceberg table
        try
        {
            if (IcebergMetadata * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage->getExternalMetadata(context_copy)); iceberg_metadata)
            {
                IcebergMetadata::IcebergHistory iceberg_history_items = iceberg_metadata->getHistory(context_copy);

                for (auto & iceberg_history_item : iceberg_history_items)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(it->databaseName());
                    res_columns[column_index++]->insert(it->name());
                    res_columns[column_index++]->insert(iceberg_history_item.made_current_at);
                    res_columns[column_index++]->insert(iceberg_history_item.snapshot_id);
                    res_columns[column_index++]->insert(iceberg_history_item.parent_id);
                    res_columns[column_index++]->insert(iceberg_history_item.is_current_ancestor);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("SystemIcebergHistory"), fmt::format("Ignoring broken table {}", object_storage->getStorageID().getFullTableName()));
        }

    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        for (const auto & db: databases)
        {
            /// with last flag we are filtering out all non iceberg table
            for (auto iterator = db.second->getTablesIterator(context_copy, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    // Table was dropped while acquiring the lock, skipping table
                    continue;

                if (auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                {
                    add_history_record(iterator, object_storage_table);
                }
            }
        }
    }
#endif
}
}
