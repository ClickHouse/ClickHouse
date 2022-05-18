#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <Storages/Hive/HiveFile.h>
#include <Storages/Hive/HiveCommon.h>
#include <Poco/Logger.h>
#include <Storages/Hive/HiveSourceTask.h>
namespace DB
{
/**
 * @brief Collect hdfs files for specified query
 *
 */
class HiveFilesCollector
{
public:
    using PruneLevel = HivePruneLevel;

    explicit HiveFilesCollector(
        ContextPtr context_,
        const SelectQueryInfo * query_info_,
        ASTPtr partition_by_ast_,
        const ColumnsDescription & columns_, const String & hive_metastore_url_,
        const String & hive_database_,
        const String & hive_table_,
        UInt32 num_streams_,
        std::shared_ptr<HiveSettings> storage_settings_)
        : context(context_)
        , query_info(query_info_)
        , partition_by_ast(partition_by_ast_)
        , columns(columns_)
        , hive_metastore_url(hive_metastore_url_)
        , hive_database(hive_database_)
        , hive_table(hive_table_)
        , num_streams(num_streams_)
        , storage_settings(storage_settings_)
    {
        prepare();
    }

    HiveFiles collect(PruneLevel prune_level = PruneLevel::Max);
private:
    using HiveTableMetadataPtr = HiveMetastoreClient::HiveTableMetadataPtr;
    ContextPtr context;
    const SelectQueryInfo * query_info;
    ASTPtr partition_by_ast; // for partition filter
    ColumnsDescription columns;
    String hive_metastore_url;
    String hive_database;
    String hive_table;
    UInt32 num_streams;
    std::shared_ptr<HiveSettings> storage_settings;

    String format_name;
    String hdfs_namenode_url;
    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;
    NamesAndTypesList partition_name_and_types;
    ExpressionActionsPtr hive_file_minmax_idx_expr;
    NamesAndTypesList hive_file_name_and_types;

    Poco::Logger * logger = &Poco::Logger::get("HiveFilesCollector");

    static ASTPtr extractKeyExpressionList(const ASTPtr & node);
    void prepare();
    HiveFiles collectHiveFilesFromPartition(
        const Apache::Hadoop::Hive::Partition & partition_,
        HiveMetastoreClient::HiveTableMetadataPtr hive_table_metadata_,
        const HDFSFSPtr & fs_,
        PruneLevel prune_level = PruneLevel::Max);

    HiveFilePtr getHiveFileIfNeeded(
        const HiveMetastoreClient::FileInfo & file_info,
        const FieldVector & fields,
        const HiveTableMetadataPtr & hive_table_metadata,
        PruneLevel prune_level = PruneLevel::Max) const;
};
}
#endif
