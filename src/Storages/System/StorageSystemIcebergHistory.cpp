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

static constexpr auto TIME_SCALE = 6;

namespace DB
{

ColumnsDescription StorageSystemIcebergHistory::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database_name",std::make_shared<DataTypeString>(),"Database name"},
        {"table_name",std::make_shared<DataTypeString>(),"Table name."},
        {"made_current_at",std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),"made current date time"},
        {"snapshot_id",std::make_shared<DataTypeUInt64>(),"snapshot id."},
        {"parent_id",std::make_shared<DataTypeUInt64>(),"parent id."},
        {"is_current_ancestor",std::make_shared<DataTypeUInt8>(),"Flag that indicates if its current ancestor."}
    };
}

void StorageSystemIcebergHistory::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();

    auto add_row = [&](const DatabaseTablesIteratorPtr &it, StorageObjectStorage *object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
        {
            return;
        }

#if USE_AVRO
        auto current_metadata = IcebergMetadata::create(
                        object_storage->getObjectStorage(),
                        object_storage->getConfiguration(),
                        context);

        if (auto *iceberg_metadata = dynamic_cast<IcebergMetadata *>(current_metadata.get()))
        {
            std::vector<Iceberg::IcebergHistory> iceberg_history_items = iceberg_metadata->getHistory();

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
#endif


    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        for (const auto &db: databases)
        {
            for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();
                if (auto *object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                {
                    add_row(iterator, object_storage_table);
                }
            }
        }

    }
}
}
