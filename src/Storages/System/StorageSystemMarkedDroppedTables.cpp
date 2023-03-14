#include <Storages/System/StorageSystemMarkedDroppedTables.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include "base/types.h"


namespace DB
{

NamesAndTypesList StorageSystemMarkedDroppedTables::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"index", std::make_shared<DataTypeUInt32>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"metadata_dropped_path", std::make_shared<DataTypeString>()},
        {"table_dropped_time", std::make_shared<DataTypeDateTime>()},
    };
    return names_and_types;
}


void StorageSystemMarkedDroppedTables::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto tables = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;

    auto & column_index = assert_cast<ColumnUInt32 &>(*res_columns[index++]);
    auto & column_database = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_uuid = assert_cast<ColumnUUID &>(*res_columns[index++]).getData();
    auto & column_engine = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_metadata_dropped_path = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & column_table_dropped_time = assert_cast<ColumnUInt32 &>(*res_columns[index++]);

    auto add_row = [&](const DatabaseCatalog::TableMarkedDroppedForSystemTable & table)
    {
        column_index.insertValue(table.index);
        column_database.insertData(table.database.data(), table.database.size());
        column_table.insertData(table.table.data(), table.table.size());
        column_uuid.push_back(table.uuid.toUnderType());
        column_engine.insertData(table.database.data(), table.database.size());
        column_metadata_dropped_path.insertData(table.metadata_dropped_path.data(), table.metadata_dropped_path.size());
        column_table_dropped_time.insertValue(static_cast<UInt32>(table.table_dropped_time));
    };

    for (const auto & table : tables)
        add_row(table);
}

}
