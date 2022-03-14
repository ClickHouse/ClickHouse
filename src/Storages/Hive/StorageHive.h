#pragma once

#include <Common/config.h>

#if USE_HIVE

#include <Poco/URI.h>
#include <ThriftHiveMetastore.h>

#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/Hive/HiveCommon.h>
#include <Storages/Hive/HiveFile.h>

namespace DB
{

class HiveSettings;
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHive final : public shared_ptr_helper<StorageHive>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageHive>;

public:
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

protected:
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
        ContextPtr context_);

private:
    using FileFormat = IHiveFile::FileFormat;
    using FileInfo = HiveMetastoreClient::FileInfo;
    using HiveTableMetadataPtr = HiveMetastoreClient::HiveTableMetadataPtr;

    static ASTPtr extractKeyExpressionList(const ASTPtr & node);

    static std::vector<FileInfo> listDirectory(const String & path, HiveTableMetadataPtr hive_table_metadata, const HDFSFSPtr & fs);

    void initMinMaxIndexExpression();

    std::vector<HiveFilePtr> collectHiveFilesFromPartition(
        const Apache::Hadoop::Hive::Partition & partition,
        SelectQueryInfo & query_info,
        HiveTableMetadataPtr hive_table_metadata,
        const HDFSFSPtr & fs,
        ContextPtr context_);

    HiveFilePtr
    createHiveFileIfNeeded(const FileInfo & file_info, const FieldVector & fields, SelectQueryInfo & query_info, ContextPtr context_);

    void getActualColumnsToRead(Block & sample_block, const Block & header_block, const NameSet & partition_columns) const;

    String hive_metastore_url;

    /// Hive database and table
    String hive_database;
    String hive_table;

    std::mutex init_mutex;
    bool has_initialized = false;

    /// Hive table meta
    std::vector<Apache::Hadoop::Hive::FieldSchema> table_schema;
    Names text_input_field_names; /// Defines schema of hive file, only used when text input format is TEXT

    String hdfs_namenode_url;

    String format_name;
    String compression_method;

    const ASTPtr partition_by_ast;
    NamesAndTypesList partition_name_types;
    Names partition_names;
    DataTypes partition_types;
    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;

    NamesAndTypesList hivefile_name_types;
    ExpressionActionsPtr hivefile_minmax_idx_expr;

    std::shared_ptr<HiveSettings> storage_settings;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");

    void lazyInitialize();
};
}

#endif
