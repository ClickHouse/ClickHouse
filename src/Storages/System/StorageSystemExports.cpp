#include <Storages/System/StorageSystemExports.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/ExportList.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

ColumnsDescription StorageSystemExports::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"source_database", std::make_shared<DataTypeString>(), "Name of the source database."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"destination_database", std::make_shared<DataTypeString>(), "Name of the destination database."},
        {"destination_table", std::make_shared<DataTypeString>(), "Name of the destination table."},
        {"create_time", std::make_shared<DataTypeDateTime>(), "Date and time when the export command was received in the server."},
        {"part_name", std::make_shared<DataTypeString>(), "Name of the part"},
        {"destination_file_path", std::make_shared<DataTypeString>(), "File path where the part is being exported."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time elapsed (in seconds) since the export started."},
        {"rows_read", std::make_shared<DataTypeUInt64>(), "The number of rows read from the exported part."},
        {"total_rows_to_read", std::make_shared<DataTypeUInt64>(), "The total number of rows to read from the exported part."},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>(), "The total size of the compressed data in the exported part."},
        {"total_size_bytes_uncompressed", std::make_shared<DataTypeUInt64>(), "The total size of the uncompressed data in the exported part."},
        {"bytes_read_uncompressed", std::make_shared<DataTypeUInt64>(), "The number of uncompressed bytes read from the exported part."},
        {"memory_usage", std::make_shared<DataTypeUInt64>(), "Current memory usage in bytes for the export operation."},
        {"peak_memory_usage", std::make_shared<DataTypeUInt64>(), "Peak memory usage in bytes during the export operation."},
    };
}

void StorageSystemExports::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & export_info : context->getExportsList().get())
    {
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, export_info.source_database, export_info.source_table))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(export_info.source_database);
        res_columns[i++]->insert(export_info.source_table);
        res_columns[i++]->insert(export_info.destination_database);
        res_columns[i++]->insert(export_info.destination_table);
        res_columns[i++]->insert(export_info.create_time);
        res_columns[i++]->insert(export_info.part_name);
        res_columns[i++]->insert(export_info.destination_file_path);
        res_columns[i++]->insert(export_info.elapsed);
        res_columns[i++]->insert(export_info.rows_read);
        res_columns[i++]->insert(export_info.total_rows_to_read);
        res_columns[i++]->insert(export_info.total_size_bytes_compressed);
        res_columns[i++]->insert(export_info.total_size_bytes_uncompressed);
        res_columns[i++]->insert(export_info.bytes_read_uncompressed);
        res_columns[i++]->insert(export_info.memory_usage);
        res_columns[i++]->insert(export_info.peak_memory_usage);
    }
}

}
