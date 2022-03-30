#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <base/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Poco/Logger.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
namespace DB
{
/**
 * @brief StorageHiveCluster is used to run hive query in distributed mode.
 *
 * StorageHiveCluster is implemented on StorageHive, and StorageHive would receive a IHiveQueryTaskFilesCollector object to
 * get assigned hdfs files to read.
 *
 * Initiator node collects all related hdfs files by parsing the query, and assign files into different cluster nodes.
 * All the cominunications with hive metastore server happens on initiator node, since the rpc calls to hive metastore server
 * are costly.
 *
 */
class HiveSettings;
class StorageHiveCluster : public shared_ptr_helper<StorageHiveCluster>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageHiveCluster>;

public:
    String getName() const override { return "HiveCluster"; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & /* left_in_operand */,
        ContextPtr /* query_context */,
        const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        return true;
    }

    bool isRemote() const override { return true; }

    Pipe read(
        const Names & column_names_,
        const StorageSnapshotPtr & metadata_snapshot_,
        SelectQueryInfo & query_info_,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage_,
        size_t max_block_size_,
        unsigned num_streams_) override;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context_, QueryProcessingStage::Enum to_stage_, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

protected:
    StorageHiveCluster(
        const String & cluster_name_,
        const String & hive_metastore_url_,
        const String & hive_database_,
        const String & hive_table_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        const ASTPtr & partition_by_ast_,
        std::unique_ptr<HiveSettings> storage_settings_,
        ContextPtr context_);

private:
    String cluster_name;
    String hive_metastore_url;
    String hive_database;
    String hive_table;

    const ASTPtr partition_by_ast;

    std::shared_ptr<HiveSettings> storage_settings;

    Poco::Logger * logger = &Poco::Logger::get("StorageHiveCluster");
};
}

#endif
