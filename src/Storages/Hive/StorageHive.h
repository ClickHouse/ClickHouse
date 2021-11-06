#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_HDFS

#include <Poco/URI.h>
#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>

#include <Interpreters/Context.h>
#include <Interpreters/SubqueryForSet.h>
#include <Storages/IStorage.h>
#include <ThriftHiveMetastore.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class HiveSettings;
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHive final : public shared_ptr_helper<StorageHive>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageHive>;

public:
    enum class FileFormat
    {
        RC_FILE,
        TEXT,
        LZO_TEXT,
        SEQUENCE_FILE,
        AVRO,
        PARQUET,
        ORC,
    };

    // TODO: json support
    inline static const String RCFILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    inline static const String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    inline static const String LZO_TEXT_INPUT_FORMAT = "com.hadoop.mapred.DeprecatedLzoTextInputFormat";
    inline static const String SEQUENCE_INPUT_FORMAT = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    inline static const String PARQUET_INPUT_FORMAT = "com.cloudera.impala.hive.serde.ParquetInputFormat";
    inline static const String MR_PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    inline static const String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    inline static const String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    inline static const std::map<String, FileFormat> VALID_HDFS_FORMATS = {
        {RCFILE_INPUT_FORMAT, FileFormat::RC_FILE},
        {TEXT_INPUT_FORMAT, FileFormat::TEXT},
        {LZO_TEXT_INPUT_FORMAT, FileFormat::LZO_TEXT},
        {SEQUENCE_INPUT_FORMAT, FileFormat::SEQUENCE_FILE},
        {PARQUET_INPUT_FORMAT, FileFormat::PARQUET},
        {MR_PARQUET_INPUT_FORMAT, FileFormat::PARQUET},
        {AVRO_INPUT_FORMAT, FileFormat::AVRO},
        {ORC_INPUT_FORMAT, FileFormat::ORC},
    };

    static inline bool isFormatClass(const String & format_class) { return VALID_HDFS_FORMATS.count(format_class) > 0; }
    static inline FileFormat toFileFormat(const String & format_class)
    {
        if (isFormatClass(format_class))
        {
            return VALID_HDFS_FORMATS.find(format_class)->second;
        }
        throw Exception("Unsupported hdfs file format " + format_class, ErrorCodes::NOT_IMPLEMENTED);
    }


    String getName() const override { return "Hive"; }

    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & /* left_in_operand */,
        ContextPtr /* query_context */,
        const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        return false;
    }


    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/) override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageHive(
        const String & hms_url_,
        const String & hive_database_,
        const String & hive_table_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        const ASTPtr & partition_by_ast_,
        std::unique_ptr<HiveSettings> storage_settings_,
        ContextPtr context_);

    void initMinMaxIndexExpression();
    static ASTPtr extractKeyExpressionList(const ASTPtr & node);

private:
    // hive metastore url
    String hms_url;

    // hive database and table
    String hive_database;
    String hive_table;

    // hive table meta
    std::vector<Apache::Hadoop::Hive::FieldSchema> table_schema;
    Names text_input_field_names; // Defines schema of hive file, only used when text input format is TEXT

    // hdfs relative information
    String hdfs_namenode_url;

    String format_name;
    String compression_method;

    const ASTPtr partition_by_ast;
    NamesAndTypesList partition_name_types;
    ExpressionActionsPtr partition_key_expr;
    ExpressionActionsPtr partition_minmax_idx_expr;

    NamesAndTypesList hivefile_name_types;
    ExpressionActionsPtr hivefile_minmax_idx_expr;

    std::shared_ptr<HiveSettings> storage_settings;

    Poco::Logger * log = &Poco::Logger::get("StorageHive");
};
}

#endif
