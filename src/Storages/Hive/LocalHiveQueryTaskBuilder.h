#pragma once
#include <memory>
#include <Common/config.h>
#if USE_HIVE
#include <Poco/Logger.h>
#include <Storages/Hive/HiveQueryTask.h>
#include <Storages/Hive/HiveFilesCollector.h>
namespace DB
{
/**
 * @brief Used for Hive() engine, running the query on a single node.
 *
 */
class LocalHiveQueryTaskFilesCollector : public IHiveQueryTaskFilesCollector
{
public:
    using Arguments = IHiveQueryTaskFilesCollector::Arguments;
    using PruneLevel = IHiveQueryTaskFilesCollector::PruneLevel;
    void setupArgs(const Arguments & args_) override;
    HiveFiles collect(PruneLevel prune_level) override;
    String getName() override { return "SingleHiveTask"; }
    void setupCallbackData(const String &) override { }

private:
    Arguments args;
    std::unique_ptr<HiveFilesCollector>  files_collector;
};

}
#endif
