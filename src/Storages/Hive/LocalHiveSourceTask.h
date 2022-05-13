#pragma once
#include <memory>
#include <Common/config.h>
#if USE_HIVE
#include <Poco/Logger.h>
#include <Storages/Hive/HiveSourceTask.h>
#include <Storages/Hive/HiveFilesCollector.h>
namespace DB
{
/**
 * @brief Used for Hive() engine, running the query on a single node.
 *
 */
class LocalHiveSourceFilesCollector : public IHiveSourceFilesCollector
{
public:
    static constexpr auto  NAME = "local_hive_source";
    using Arguments = IHiveSourceFilesCollector::Arguments;
    using PruneLevel = HivePruneLevel;
    void initialize(const Arguments & args_) override;
    HiveFiles collect(PruneLevel prune_level) override;
    String getName() override { return NAME; }

private:
    Arguments args;
    std::unique_ptr<HiveFilesCollector>  files_collector;
};

}
#endif
