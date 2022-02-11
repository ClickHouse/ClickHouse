#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <Interpreters/ExpressionActions.h>
#include <Poco/Logger.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/Hive/IHiveTaskPolicy.h>
#include <Storages/Hive/StorageHive.h>
namespace DB
{
class SingleHiveTaskFilesCollector : public IHiveTaskFilesCollector
{
public:
    using Arguments = IHiveTaskFilesCollector::Arguments;
    void setupArgs(const Arguments & args_) override;
    HiveFiles collectHiveFiles() override;
    String getName() override { return "SingleHiveTask"; }
    void setupCallbackData(const String &) override { }

private:
    Arguments args;
    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;
    NamesAndTypesList partition_name_and_types;
    ExpressionActionsPtr hive_file_minmax_idx_expr;
    NamesAndTypesList hive_file_name_and_types;
    String format_name;
    String hdfs_namenode_url;

    Poco::Logger * logger = &Poco::Logger::get("SingleHiveTaskFilesCollector");

    HiveFiles collectHiveFilesFromPartition(
        const Apache::Hadoop::Hive::Partition & partition_,
        SelectQueryInfo & query_info_,
        HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
        const HDFSFSPtr & fs_,
        ContextPtr context_);

    ASTPtr extractKeyExpressionList(const ASTPtr & node);

    HiveFilePtr createHiveFileIfNeeded(
        const HiveMetastoreClient::FileInfo & file_info_, const FieldVector & fields_, SelectQueryInfo & query_info_, ContextPtr context_);
};

}
#endif
