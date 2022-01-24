#pragma once
#include <memory>
#include <Common/config.h>
#if USE_HIVE
#include <IO/Marshallable.h>
#include <Poco/Logger.h>
#include <Storages/Hive/IHiveTaskPolicy.h>
#include <Storages/Hive/HiveCommon.h>
namespace DB
{

#define HIVE_TASK_NODE_HASH_POLICY "hive_task_node_hash_policy"

//using BasicFileInfo = HiveMetastoreClient::FileInfo;
struct HiveTaskFileInfo
{
    HiveMetastoreClient::FileInfo file_info;
    Strings partition_values;
};

class HiveTaskPartitionInfo : public Marshallable
{
public:
    HiveTaskPartitionInfo() = default;
    HiveTaskPartitionInfo(const Strings & partition_values_, const std::vector<HiveMetastoreClient::FileInfo> & files_)
        : partition_values(partition_values_)
        , files(files_)
    {}

    Strings partition_values;
    std::vector<HiveMetastoreClient::FileInfo> files;

    void marshal(MarshallablePack & p) const override
    {
        p << partition_values << files;
    }

    void unmarshal(MarshallableUnPack & p) override
    {
        p >> partition_values >> files;
    }
};

class AssginedTaskMetadata : public Marshallable
{
public:
    AssginedTaskMetadata() = default;
    AssginedTaskMetadata(const String & file_format_,
        const String & hdfs_namenode_url_,
        const std::map<String, HiveTaskPartitionInfo> & files_)
        : file_format(file_format_)
        , hdfs_namenode_url(hdfs_namenode_url_)
        , files(files_)
    {}

    String file_format;
    String hdfs_namenode_url;
    std::map<String, HiveTaskPartitionInfo> files;

    void marshal(MarshallablePack & p) const override
    {
        p << file_format << hdfs_namenode_url << files;
    }
    void unmarshal(MarshallableUnPack & p) override
    {
        p >> file_format >> hdfs_namenode_url >> files;
    }
}; 

/**
 * @brief collect all hdfs files and distribute them into different nodes
 * 
 */
class HiveTaskNodeHashIterateCallback : public IHiveTaskIterateCallback
{
public:
    using Arguments = IHiveTaskIterateCallback::Arguments;

    void init(const Arguments & args_) override;
    std::shared_ptr<TaskIterator> buildCallback(const Cluster::Address & address_) override;  

    String getName() override
    {
        return HIVE_TASK_NODE_HASH_POLICY;
    }  

private:
    Poco::Logger *logger = &Poco::Logger::get("HiveTaskNodeHashIterateCallback");

    Arguments args;
    /**
     * @brief map node to a list of hive files
     * 
     */
    std::map<String, AssginedTaskMetadata> node_tasks;

    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;
    NamesAndTypesList partition_name_and_types;
    ExpressionActionsPtr hive_file_minmax_idx_expr;
    NamesAndTypesList hive_file_name_and_types;
    //std::vector<Apache::Hadoop::Hive::Partition> hive_partitions;
    String format_name;
    String hdfs_namenode_url;

    std::vector<HiveTaskFileInfo> collectHiveFiles();

    std::vector<HiveTaskFileInfo> collectHiveFilesFromPartition(
        const Apache::Hadoop::Hive::Partition & partition_,
        SelectQueryInfo & query_info_,
        HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
        const HDFSFSPtr & fs_,
        ContextPtr context_);

    HiveFilePtr createHiveFileIfNeeded(
        const HiveMetastoreClient::FileInfo & file_info_,
        const FieldVector & fields_,
        SelectQueryInfo & query_info_,
        ContextPtr context_ );

    Cluster::Addresses getSortedShardAddresses() const;

    void distributeHiveFilesToNodes(const std::vector<HiveTaskFileInfo> & hive_file_infos_);
};

class HiveTaskNodeHashFilesCollector : public IHiveTaskFilesCollector
{
public:
    using Arguments = IHiveTaskFilesCollector::Arguments;
    void initQueryEnv(const Arguments & args) override;
    HiveFiles collectHiveFiles() override;
    String getName() override
    {
        return HIVE_TASK_NODE_HASH_POLICY;
    }

    void setupCallbackData(const String & data_) override;

private:
    //Poco::Logger *logger = &Poco::Logger::get("HiveTaskNodeHashFilesCollector");
    Arguments args;
    AssginedTaskMetadata task_metadata;
    
    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;
    NamesAndTypesList partition_name_and_types;
    ExpressionActionsPtr hive_file_minmax_idx_expr;
    NamesAndTypesList hive_file_name_and_types;

    HiveFiles collectPartitionHiveFiles(const String & format_name_, const HiveTaskPartitionInfo & partition_info_);

};
} // namespace DB
#endif
