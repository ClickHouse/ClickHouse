#include <Storages/StorageMaterializedMySQL.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDroppedTablesParts.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{


StoragesDroppedInfoStream::StoragesDroppedInfoStream(const ActionsDAGPtr & filter, ContextPtr context)
        : StoragesInfoStreamBase(context)
{
    /// Will apply WHERE to subset of columns and then add more columns.
    /// This is kind of complicated, but we use WHERE to do less work.

    Block block_to_filter;

    MutableColumnPtr database_column_mut = ColumnString::create();
    MutableColumnPtr table_column_mut = ColumnString::create();
    MutableColumnPtr engine_column_mut = ColumnString::create();
    MutableColumnPtr active_column_mut = ColumnUInt8::create();
    MutableColumnPtr storage_uuid_column_mut = ColumnUUID::create();

    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();
    for (const auto & dropped_table : tables_mark_dropped)
    {
        StoragePtr storage = dropped_table.table;
        if (!storage)
            continue;

        UUID storage_uuid = storage->getStorageID().uuid;
        String database_name = storage->getStorageID().getDatabaseName();
        String table_name = storage->getStorageID().getTableName();
        String engine_name = storage->getName();
#if USE_MYSQL
        if (auto * proxy = dynamic_cast<StorageMaterializedMySQL *>(storage.get()))
        {
            auto nested = proxy->getNested();
            storage.swap(nested);
        }
#endif
        if (!dynamic_cast<MergeTreeData *>(storage.get()))
            continue;

        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
            continue;

        storages[storage_uuid] = storage;

        /// Add all combinations of flag 'active'.
        for (UInt64 active : {0, 1})
        {
            database_column_mut->insert(database_name);
            table_column_mut->insert(table_name);
            engine_column_mut->insert(engine_name);
            active_column_mut->insert(active);
            storage_uuid_column_mut->insert(storage_uuid);
        }
    }

    block_to_filter.insert(ColumnWithTypeAndName(
        std::move(database_column_mut), std::make_shared<DataTypeString>(), StorageSystemPartsBase::database_column_name));
    block_to_filter.insert(
        ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), StorageSystemPartsBase::table_column_name));
    block_to_filter.insert(ColumnWithTypeAndName(
        std::move(engine_column_mut), std::make_shared<DataTypeString>(), StorageSystemPartsBase::engine_column_name));
    block_to_filter.insert(
        ColumnWithTypeAndName(std::move(active_column_mut), std::make_shared<DataTypeUInt8>(), StorageSystemPartsBase::active_column_name));
    block_to_filter.insert(ColumnWithTypeAndName(
        std::move(storage_uuid_column_mut), std::make_shared<DataTypeUUID>(), StorageSystemPartsBase::storage_uuid_column_name));

    if (block_to_filter.rows())
    {
        /// Filter block_to_filter with columns 'database', 'table', 'engine', 'active'.
        if (filter)
            VirtualColumnUtils::filterBlockWithDAG(filter, block_to_filter, context);
        rows = block_to_filter.rows();
    }

    database_column = block_to_filter.getByName(StorageSystemPartsBase::database_column_name).column;
    table_column = block_to_filter.getByName(StorageSystemPartsBase::table_column_name).column;
    active_column = block_to_filter.getByName(StorageSystemPartsBase::active_column_name).column;
    storage_uuid_column = block_to_filter.getByName(StorageSystemPartsBase::storage_uuid_column_name).column;
}


}
