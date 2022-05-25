#pragma once

#include <Common/config.h>

#if USE_HIVE

#include <Poco/URI.h>
#include <ThriftHiveMetastore.h>

#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/Hive/HiveCommon.h>
#include <Storages/Hive/HiveFile.h>
#include <Storages/Hive/HiveSourceTask.h>

namespace DB
{

class HiveSettings;
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHive final : public IStorage, WithContext
{
public:
    friend class StorageHiveSource;

    StorageHive(
        const String & hive_metastore_url_,
        const String & hive_database_,
        const String & hive_table_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        const ASTPtr & partition_by_ast_,
        std::unique_ptr<HiveSettings> storage_settings_,
        ContextPtr context_,
        std::shared_ptr<HiveSourceFilesCollectorBuilder> hive_task_files_collector_builder_ = nullptr,
        bool is_distributed_mode_ = false);

    String getName() const override { return "Hive"; }

    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & /* left_in_operand */,
        ContextPtr /* query_context */,
        const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        return true;
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/) override;

    NamesAndTypesList getVirtuals() const override;

    bool isColumnOriented() const override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr context_) const override;

private:
    using FileFormat = IHiveFile::FileFormat;
    using FileInfo = HiveMetastoreClient::FileInfo;
    using HiveTableMetadataPtr = HiveMetastoreClient::HiveTableMetadataPtr;
    using PruneLevel = HivePruneLevel;

    static ASTPtr extractKeyExpressionList(const ASTPtr & node);
    void lazyInitialize();

    std::optional<UInt64>
    totalRowsImpl(const SelectQueryInfo & query_info, PruneLevel prune_level) const;

    std::shared_ptr<IHiveSourceFilesCollector> getHiveFilesCollector(const SelectQueryInfo & query_info) const;

    String hive_metastore_url;

    /// Hive database and table
    String hive_database;
    String hive_table;

    mutable std::mutex init_mutex;
    bool has_initialized = false;

    /// Hive table meta
    std::vector<Apache::Hadoop::Hive::FieldSchema> table_schema;
    Names text_input_field_names; /// Defines schema of hive file, only used when text input format is TEXT

    String hdfs_namenode_url;

    String format_name;
    String compression_method;

    const ASTPtr partition_by_ast;
    NamesAndTypesList partition_name_types;

    std::shared_ptr<HiveSettings> storage_settings;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");

    std::shared_ptr<HiveSourceFilesCollectorBuilder> hive_task_files_collector_builder;
    bool is_distributed_mode;
};

}

#endif
