#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Access/ContextAccess.h>


namespace DB
{

ColumnsDescription StorageSystemMerges::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time elapsed (in seconds) since the merge started."},
        {"progress", std::make_shared<DataTypeFloat64>(), "The percentage of completed work from 0 to 1."},
        {"num_parts", std::make_shared<DataTypeUInt64>(), "The number of parts to be merged."},
        {"source_part_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of source parts names."},
        {"result_part_name", std::make_shared<DataTypeString>(), "The name of the part that will be formed as the result of merging."},
        {"source_part_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of paths for each source part."},
        {"result_part_path", std::make_shared<DataTypeString>(), "The path of the part that will be formed as the result of merging."},
        {"partition_id", std::make_shared<DataTypeString>(), "The identifier of the partition where the merge is happening."},
        {"partition", std::make_shared<DataTypeString>(), "The name of the partition"},
        {"is_mutation", std::make_shared<DataTypeUInt8>(), "1 if this process is a part mutation."},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>(), "The total size of the compressed data in the merged chunks."},
        {"total_size_bytes_uncompressed", std::make_shared<DataTypeUInt64>(), "The total size of compressed data in the merged chunks."},
        {"total_size_marks", std::make_shared<DataTypeUInt64>(), "The total number of marks in the merged parts."},
        {"bytes_read_uncompressed", std::make_shared<DataTypeUInt64>(), "Number of bytes read, uncompressed."},
        {"rows_read", std::make_shared<DataTypeUInt64>(), "Number of rows read."},
        {"bytes_written_uncompressed", std::make_shared<DataTypeUInt64>(), "Number of bytes written, uncompressed."},
        {"rows_written", std::make_shared<DataTypeUInt64>(), "Number of rows written."},
        {"columns_written", std::make_shared<DataTypeUInt64>(), "Number of columns written (for Vertical merge algorithm)."},
        {"memory_usage", std::make_shared<DataTypeUInt64>(), "Memory consumption of the merge process."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread ID of the merge process."},
        {"merge_type", std::make_shared<DataTypeString>(), "The type of current merge. Empty if it's an mutation."},
        {"merge_algorithm", std::make_shared<DataTypeString>(), "The algorithm used in current merge. Empty if it's an mutation."},
    };
}


void StorageSystemMerges::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & merge : context->getMergeList().get())
    {
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, merge.database, merge.table))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(merge.database);
        res_columns[i++]->insert(merge.table);
        res_columns[i++]->insert(merge.elapsed);
        res_columns[i++]->insert(merge.progress);
        res_columns[i++]->insert(merge.num_parts);
        res_columns[i++]->insert(merge.source_part_names);
        res_columns[i++]->insert(merge.result_part_name);
        res_columns[i++]->insert(merge.source_part_paths);
        res_columns[i++]->insert(merge.result_part_path);
        res_columns[i++]->insert(merge.partition_id);
        res_columns[i++]->insert(merge.partition);
        res_columns[i++]->insert(merge.is_mutation);
        res_columns[i++]->insert(merge.total_size_bytes_compressed);
        res_columns[i++]->insert(merge.total_size_bytes_uncompressed);
        res_columns[i++]->insert(merge.total_size_marks);
        res_columns[i++]->insert(merge.bytes_read_uncompressed);
        res_columns[i++]->insert(merge.rows_read);
        res_columns[i++]->insert(merge.bytes_written_uncompressed);
        res_columns[i++]->insert(merge.rows_written);
        res_columns[i++]->insert(merge.columns_written);
        res_columns[i++]->insert(merge.memory_usage);
        res_columns[i++]->insert(merge.thread_id);
        if (!merge.is_mutation)
        {
            res_columns[i++]->insert(merge.merge_type);
            res_columns[i++]->insert(merge.merge_algorithm);
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }
    }
}

}
