#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Access/ContextAccess.h>


namespace DB
{

NamesAndTypesList StorageSystemMerges::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"elapsed", std::make_shared<DataTypeFloat64>()},
        {"progress", std::make_shared<DataTypeFloat64>()},
        {"num_parts", std::make_shared<DataTypeUInt64>()},
        {"source_part_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"result_part_name", std::make_shared<DataTypeString>()},
        {"source_part_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"result_part_path", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"is_mutation", std::make_shared<DataTypeUInt8>()},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>()},
        {"total_size_marks", std::make_shared<DataTypeUInt64>()},
        {"bytes_read_uncompressed", std::make_shared<DataTypeUInt64>()},
        {"rows_read", std::make_shared<DataTypeUInt64>()},
        {"bytes_written_uncompressed", std::make_shared<DataTypeUInt64>()},
        {"rows_written", std::make_shared<DataTypeUInt64>()},
        {"columns_written", std::make_shared<DataTypeUInt64>()},
        {"memory_usage", std::make_shared<DataTypeUInt64>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"merge_type", std::make_shared<DataTypeString>()},
        {"merge_algorithm", std::make_shared<DataTypeString>()},
    };
}


void StorageSystemMerges::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto access = context.getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & merge : context.getMergeList().get())
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
        res_columns[i++]->insert(merge.is_mutation);
        res_columns[i++]->insert(merge.total_size_bytes_compressed);
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
