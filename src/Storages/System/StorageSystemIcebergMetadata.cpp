#include <Storages/System/StorageSystemIcebergMetadata.h>
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
#include <base/Decimal.h>

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

ColumnsDescription StorageSystemIcebergMetadata::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"event_time",std::make_shared<DataTypeDateTime>(),"Event time."},
        {"content_type",std::make_shared<DataTypeInt64>(),"Content type."},
        {"path",std::make_shared<DataTypeString>(),"Path of table."},
        {"file_name",std::make_shared<DataTypeString>(),"Name of file."},
        {"content",std::make_shared<DataTypeString>(),"Content of metadata."}
    };
}

void StorageSystemIcebergMetadata::fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_AVRO
    const auto access = context->getAccess();

    auto add_history_record = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        {
            if (IcebergMetadata * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage->getExternalMetadata(context)); iceberg_metadata)
            {
                auto metadata_items = iceberg_metadata->getMetadataLog();

                for (auto & iceberg_metadata_item : metadata_items)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(iceberg_metadata_item.current_time);
                    res_columns[column_index++]->insert(iceberg_metadata_item.content_type);
                    res_columns[column_index++]->insert(iceberg_metadata_item.path);
                    res_columns[column_index++]->insert(iceberg_metadata_item.filename);
                    res_columns[column_index++]->insert(iceberg_metadata_item.metadata_content);
                }
            }
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & db: databases)
        {
            /// with last flag we are filtering out all non iceberg table
            for (auto iterator = db.second->getLightweightTablesIterator(context, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
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
