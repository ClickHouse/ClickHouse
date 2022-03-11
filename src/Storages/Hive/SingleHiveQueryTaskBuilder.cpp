#include <Storages/Hive/SingleHiveQueryTaskBuilder.h>
#if USE_HIVE
#include <Storages/Hive/HiveFilesCollector.h>
namespace DB
{

void SingleHiveQueryTaskFilesCollector::setupArgs(const Arguments & args_)
{
    args = args_;
}

HiveFiles SingleHiveQueryTaskFilesCollector::collectHiveFiles()
{
    HiveFilesCollector files_collector(
        args.context,
        args.query_info,
        args.partition_by_ast,
        args.columns,
        args.hive_metastore_url,
        args.hive_database,
        args.hive_table,
        args.num_streams,
        args.storage_settings);
    auto total_hive_files = files_collector.collect();
    HiveFiles task_files;
    task_files.reserve(total_hive_files.size());
    for (const auto & file : total_hive_files)
        task_files.emplace_back(file.file_ptr);
    return task_files;
}
}
#endif
