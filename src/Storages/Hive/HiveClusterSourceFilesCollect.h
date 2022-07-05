#pragma once
#include <memory>
#include <Common/config.h>
#if USE_HIVE
#include <unordered_map>
#include <Storages/Hive/HiveSourceTask.h>
namespace DB
{
class HiveClusterSourceFilesCollectCallback : public IHiveSourceFilesCollectCallback
{
public:
    static constexpr auto NAME = "hive_cluster_source_node_hash";
    using Arguments = IHiveSourceFilesCollectCallback::Arguments;
    ~HiveClusterSourceFilesCollectCallback() override = default;

    void initialize(const Arguments & arguments) override;
    std::shared_ptr<TaskIterator> buildCollectCallback(const Cluster::Address & address) override;
    String getName() override { return NAME; }

private:
    Arguments args;
    std::unordered_map<String, String> nodes_callback_data;

    void dispatchHiveFiles(const HiveFiles & hive_files);
};

class HiveClusterSourceFilesCollector : public IHiveSourceFilesCollector
{
public:
    static constexpr auto NAME = "hive_cluster_source_node_hash";
    using Arguments = IHiveSourceFilesCollector::Arguments;
    ~HiveClusterSourceFilesCollector() override = default;

    void initialize(const Arguments & arguments) override;
    HiveFiles collect(HivePruneLevel prune_level) override;
    String getName() override { return NAME; }

private:
    Arguments args;
    NamesAndTypesList all_name_and_types;
    Poco::JSON::Array::Ptr files_in_json;
};
}
#endif

