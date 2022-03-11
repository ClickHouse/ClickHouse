#pragma once
#include <memory>
#include <ostream>
#include <Common/config.h>
#if USE_HIVE
#include <IO/Marshallable.h>
#include <Storages/Hive/HiveCommon.h>
#include <Storages/Hive/HiveFilesCollector.h>
#include <Storages/Hive/HiveQueryTask.h>
#include <Poco/Logger.h>
namespace DB
{

#define HIVE_TASK_NODE_HASH_POLICY "hive_task_node_hash_policy"

/**
 *  Data structures for sending task information among nodes.
 *
 */
//using BasicFileInfo = HiveMetastoreClient::FileInfo;

class DistributedHiveQueryTaskPartitionInfo : public Marshallable
{
public:
    DistributedHiveQueryTaskPartitionInfo() = default;
    DistributedHiveQueryTaskPartitionInfo(const Strings & partition_values_, const std::vector<HiveMetastoreClient::FileInfo> & files_)
        : partition_values(partition_values_), files(files_)
    {
    }

    Strings partition_values;
    std::vector<HiveMetastoreClient::FileInfo> files;

    void marshal(MarshallablePack & p) const override { p << partition_values << files; }

    void unmarshal(MarshallableUnPack & p) override { p >> partition_values >> files; }
    MarshallableTraceBuffer & trace(MarshallableTraceBuffer & buf) const override
    {
        buf << "partition_values=" << partition_values << ";"
           << "files=" << files;
        return buf;
    }
};

class DistributedHiveQueryTaskMetadata : public Marshallable
{
public:
    DistributedHiveQueryTaskMetadata() = default;
    DistributedHiveQueryTaskMetadata(
        const String & file_format_, const String & hdfs_namenode_url_, const std::map<String, DistributedHiveQueryTaskPartitionInfo> & files_)
        : file_format(file_format_), hdfs_namenode_url(hdfs_namenode_url_), files(files_)
    {
    }

    String file_format;
    String hdfs_namenode_url;
    std::map<String, DistributedHiveQueryTaskPartitionInfo> files;

    void marshal(MarshallablePack & p) const override { p << file_format << hdfs_namenode_url << files; }
    void unmarshal(MarshallableUnPack & p) override { p >> file_format >> hdfs_namenode_url >> files; }
    MarshallableTraceBuffer & trace(MarshallableTraceBuffer & buf) const override
    {
        buf << "file_format=" << file_format << ";"
           << "hdfs_namenode_url=" << hdfs_namenode_url << ";"
           << "files=" << files;
        return buf;
    }
};

/**
 * @brief collect all hdfs files and distribute them into different nodes
 *
 */
class DistributedHiveQueryTaskterateCallback : public IHiveQueryTaskIterateCallback
{
public:
    using Arguments = IHiveQueryTaskIterateCallback::Arguments;

    void setupArgs(const Arguments & args_) override;
    std::shared_ptr<TaskIterator> buildCallback(const Cluster::Address & address_) override;

    String getName() override { return HIVE_TASK_NODE_HASH_POLICY; }

private:
    Poco::Logger * logger = &Poco::Logger::get("DistributedHiveQueryTaskterateCallback");

    Arguments args;
    /**
     * @brief map node to a list of hive files
     *
     */
    std::map<String, DistributedHiveQueryTaskMetadata> node_tasks;
    Cluster::Addresses getSortedShardAddresses() const;

    void distributeHiveFilesToNodes(const std::vector<HiveFilesCollector::FileInfo> & hive_file_infos_);
};

class DistributedHiveQueryTaskFilesCollector : public IHiveQueryTaskFilesCollector
{
public:
    using Arguments = IHiveQueryTaskFilesCollector::Arguments;
    void setupArgs(const Arguments & args) override;
    HiveFiles collectHiveFiles() override;
    String getName() override { return HIVE_TASK_NODE_HASH_POLICY; }

    void setupCallbackData(const String & data_) override;

private:
    Poco::Logger * logger = &Poco::Logger::get("DistributedHiveQueryTaskFilesCollector");
    Arguments args;
    DistributedHiveQueryTaskMetadata task_metadata;

    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;
    NamesAndTypesList partition_name_and_types;
    ExpressionActionsPtr hive_file_minmax_idx_expr;
    NamesAndTypesList hive_file_name_and_types;

    HiveFiles collectPartitionHiveFiles(const String & format_name_, const DistributedHiveQueryTaskPartitionInfo & partition_info_);
};
}
#endif
