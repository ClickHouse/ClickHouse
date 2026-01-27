#include <mutex>
#include <Access/ContextAccess.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDeltaLakeHistory.h>

#include "config.h"

/// Delta Lake timestamps are stored in milliseconds
static constexpr auto TIME_SCALE = 3;

namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

ColumnsDescription StorageSystemDeltaLakeHistory::getColumnsDescription()
{
    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"version", std::make_shared<DataTypeUInt64>(), "Version number of the Delta Lake table."},
        {"timestamp",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),
         "Timestamp when this version was committed."},
        {"operation", std::make_shared<DataTypeString>(), "Operation type (e.g., WRITE, DELETE, MERGE)."},
        {"operation_parameters",
         std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
         "Parameters of the operation."},
        {"is_current", std::make_shared<DataTypeUInt8>(), "Flag indicating if this is the current version."}};
}

void StorageSystemDeltaLakeHistory::fillData(
    [[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_PARQUET
    ContextMutablePtr context_copy = Context::createCopy(context);

    const auto access = context_copy->getAccess();

    auto add_history_record = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        /// Unfortunately this try/catch is unavoidable. Delta Lake tables can be broken in arbitrary way, it's impossible
        /// to handle properly all possible errors which we can get when attempting to read metadata of delta lake table
        try
        {
            if (IDataLakeMetadata * data_lake_metadata = object_storage->getExternalMetadata(context_copy);
                data_lake_metadata)
            {
                DataLakeHistory delta_history_items = data_lake_metadata->getHistory(context_copy);

                for (auto & delta_history_item : delta_history_items)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(it->databaseName());
                    res_columns[column_index++]->insert(it->name());
                    res_columns[column_index++]->insert(delta_history_item.version);

                    if (delta_history_item.timestamp.has_value())
                        res_columns[column_index++]->insert(delta_history_item.timestamp.value());
                    else
                        res_columns[column_index++]->insertDefault();

                    res_columns[column_index++]->insert(delta_history_item.operation);

                    /// Insert operation_parameters as Map
                    Map params_map;
                    for (const auto & [key, value] : delta_history_item.operation_parameters)
                    {
                        Tuple tuple;
                        tuple.push_back(key);
                        tuple.push_back(value);
                        params_map.push_back(std::move(tuple));
                    }
                    res_columns[column_index++]->insert(params_map);

                    res_columns[column_index++]->insert(delta_history_item.is_current ? 1 : 0);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("SystemDeltaLakeHistory"),
                fmt::format("Ignoring broken table {}", object_storage->getStorageID().getFullTableName()));
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        for (const auto & db : databases)
        {
            /// with last flag we are filtering out all non delta lake tables
            for (auto iterator = db.second->getLightweightTablesIterator(context_copy, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(
                    context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    /// Table was dropped while acquiring the lock, skipping table
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
