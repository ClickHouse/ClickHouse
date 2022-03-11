#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <Poco/Logger.h>
#include <Storages/Hive/HiveQueryTask.h>
namespace DB
{
/**
 * @brief Used for Hive() engin, running the query on a single node.
 *
 */
class SingleHiveQueryTaskFilesCollector : public IHiveQueryTaskFilesCollector
{
public:
    using Arguments = IHiveQueryTaskFilesCollector::Arguments;
    void setupArgs(const Arguments & args_) override;
    HiveFiles collectHiveFiles() override;
    String getName() override { return "SingleHiveTask"; }
    void setupCallbackData(const String &) override { }

private:
    Arguments args;
};

}
#endif
