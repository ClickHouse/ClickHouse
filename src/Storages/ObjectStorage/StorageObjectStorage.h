#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Core/SchemaInferenceMode.h>
#include <Storages/IStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/PartitionColumns.h>

namespace DB
{

class ReadBufferIterator;
class SchemaCache;
class NamedCollection;

/**
 * A general class containing implementation for external table engines
 * such as StorageS3, StorageAzure, StorageHDFS.
 * Works with an object of IObjectStorage class.
 */
class StorageObjectStorage : public IStorage
{
public:
    class Configuration;
    using ConfigurationPtr = std::shared_ptr<Configuration>;
    using ObjectInfo = RelativePathWithMetadata;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    struct QuerySettings
    {
        /// Insert settings:
        bool truncate_on_insert;
        bool create_new_file_on_insert;

        /// Schema inference settings:
        bool schema_inference_use_cache;
        SchemaInferenceMode schema_inference_mode;

        /// List settings:
        bool skip_empty_files;
        size_t list_object_keys_size;
        bool throw_on_zero_files_match;
        bool ignore_non_existent_file;
    };

    StorageObjectStorage(
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    bool supportsPartitionBy() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsDynamicSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & context, const std::string & storage_type_name);

    static ColumnsDescription resolveSchemaFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

    static std::string resolveFormatFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

    static std::pair<ColumnsDescription, std::string> resolveSchemaAndFormatFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

protected:
    virtual void updateConfiguration(ContextPtr local_context);

    String getPathSample(StorageInMemoryMetadata metadata, ContextPtr context);

    virtual ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context);

    static std::unique_ptr<ReadBufferIterator> createReadBufferIterator(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ObjectInfos & read_keys,
        const ContextPtr & context);

    ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const ASTPtr partition_by;
    const bool distributed_processing;

    LoggerPtr log;
};

class StorageObjectStorage::Configuration
{
public:
    Configuration() = default;
    Configuration(const Configuration & other);
    virtual ~Configuration() = default;

    using Path = std::string;
    using Paths = std::vector<Path>;

    static void initialize(
        Configuration & configuration,
        ASTs & engine_args,
        ContextPtr local_context,
        bool with_table_structure);

    /// Storage type: s3, hdfs, azure, local.
    virtual std::string getTypeName() const = 0;
    /// Engine name: S3, HDFS, Azure.
    virtual std::string getEngineName() const = 0;
    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual std::string getNamespaceType() const { return "namespace"; }

    virtual Path getPath() const = 0;
    virtual void setPath(const Path & path) = 0;

    virtual const Paths & getPaths() const = 0;
    virtual void setPaths(const Paths & paths) = 0;

    virtual String getDataSourceDescription() const = 0;
    virtual String getNamespace() const = 0;

    virtual StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const = 0;

    /// Add/replace structure and format arguments in the AST arguments if they have 'auto' values.
    virtual void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context) = 0;

    bool withPartitionWildcard() const;
    bool withGlobs() const { return isPathWithGlobs() || isNamespaceWithGlobs(); }
    bool withGlobsIgnorePartitionWildcard() const;
    bool isPathWithGlobs() const;
    bool isNamespaceWithGlobs() const;
    virtual std::string getPathWithoutGlobs() const;

    virtual bool isArchive() const { return false; }
    bool isPathInArchiveWithGlobs() const;
    virtual std::string getPathInArchive() const;

    virtual void check(ContextPtr context) const;
    virtual void validateNamespace(const String & /* name */) const {}

    virtual ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) = 0;
    virtual ConfigurationPtr clone() = 0;
    virtual bool isStaticConfiguration() const { return true; }

    void setPartitionColumns(const DataLakePartitionColumns & columns) { partition_columns = columns; }
    const DataLakePartitionColumns & getPartitionColumns() const { return partition_columns; }

    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";

protected:
    virtual void fromNamedCollection(const NamedCollection & collection, ContextPtr context) = 0;
    virtual void fromAST(ASTs & args, ContextPtr context, bool with_structure) = 0;

    void assertInitialized() const;

    bool initialized = false;
    DataLakePartitionColumns partition_columns;
};

}
