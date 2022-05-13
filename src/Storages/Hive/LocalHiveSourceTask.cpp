#include <memory>
#include <Storages/Hive/LocalHiveSourceTask.h>
#if USE_HIVE
#include <Storages/Hive/HiveFilesCollector.h>
namespace DB
{

void LocalHiveSourceFilesCollector::initialize(const Arguments & args_)
{
    args = args_;
    files_collector = std::make_unique<HiveFilesCollector>(
        args.context,
        args.query_info,
        args.partition_by_ast,
        args.columns,
        args.hive_metastore_url,
        args.hive_database,
        args.hive_table,
        args.num_streams,
        args.storage_settings);
}

HiveFiles LocalHiveSourceFilesCollector::collect(HivePruneLevel prune_level)
{
    return files_collector->collect(prune_level);
}

void registerLocalHiveSourceFilesCollector(HiveSourceCollectorFactory & factory)
{
    auto builder = []() { return std::make_shared<LocalHiveSourceFilesCollector>(); };
    factory.registerBuilder(LocalHiveSourceFilesCollector::NAME, builder);
}
}
#endif
