#include <Storages/System/StorageSystemDroppedTables.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <base/types.h>


namespace DB
{

ColumnsDescription StorageSystemDroppedTables::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"index", std::make_shared<DataTypeUInt32>(), "Index in marked_dropped_tables queue."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Table UUID."},
        {"engine", std::make_shared<DataTypeString>(), "Table engine name."},
        {"metadata_dropped_path", std::make_shared<DataTypeString>(), "Path of table's metadata file in metadata_dropped directory."},
        {"table_dropped_time", std::make_shared<DataTypeDateTime>(), "The time when the next attempt to remove table's data is scheduled on. Usually it's the table when the table was dropped plus `database_atomic_delay_before_drop_table_sec`."},
    };
}


void StorageSystemDroppedTables::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;

    auto & column_index = assert_cast<ColumnUInt32 &>(*res_columns[index++]);
    auto & column_database = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_uuid = assert_cast<ColumnUUID &>(*res_columns[index++]).getData();
    auto & column_engine = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_metadata_dropped_path = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_table_dropped_time = assert_cast<ColumnUInt32 &>(*res_columns[index++]);

    auto add_row = [&](UInt32 idx, const DatabaseCatalog::TableMarkedAsDropped & table_mark_dropped)
    {
        column_index.insertValue(idx);
        column_database.insertData(table_mark_dropped.table_id.getDatabaseName().data(), table_mark_dropped.table_id.getDatabaseName().size());
        column_table.insertData(table_mark_dropped.table_id.getTableName().data(), table_mark_dropped.table_id.getTableName().size());
        column_uuid.push_back(table_mark_dropped.table_id.uuid.toUnderType());
        if (table_mark_dropped.table)
            column_engine.insertData(table_mark_dropped.table->getName().data(), table_mark_dropped.table->getName().size());
        else
            column_engine.insertData({}, 0);
        column_metadata_dropped_path.insertData(table_mark_dropped.metadata_path.data(), table_mark_dropped.metadata_path.size());
        column_table_dropped_time.insertValue(static_cast<UInt32>(table_mark_dropped.drop_time));
    };

    UInt32 idx = 0;
    for (const auto & table_mark_dropped : tables_mark_dropped)
        add_row(idx++, table_mark_dropped);
}

}
