#pragma once

#include <Storages/IPartitionStrategy.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Interpreters/ActionsDAG.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Interpreters/StorageID.h>
#include <Databases/DataLake/ICatalog.h>
#include <Storages/MutationCommands.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>

namespace DB
{

class NamedCollection;
class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


struct StorageObjectStorageQuerySettings
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


class StorageObjectStorageConfiguration
{
public:
    StorageObjectStorageConfiguration() = default;
    virtual ~StorageObjectStorageConfiguration() = default;

    struct Path
    {
        Path() = default;
        /// A partial prefix is a prefix that does not represent an actual object (directory or file), usually strings that do not end with a slash character.
        /// Example: `table_root/year=20`. AWS S3 supports partial prefixes, but HDFS does not.
        Path(const std::string & path_) : path(path_) {} /// NOLINT(google-explicit-constructor)

        std::string path;

        bool hasPartitionWildcard() const;
        bool hasGlobsIgnorePartitionWildcard() const;
        bool hasGlobs() const;
        std::string cutGlobs(bool supports_partial_prefix) const;
    };

    using Paths = std::vector<Path>;

    /// Initialize configuration from either AST or NamedCollection.
    static void initialize(
        StorageObjectStorageConfiguration & configuration_to_initialize,
        ASTs & engine_args,
        ContextPtr local_context,
        bool with_table_structure);

    /// Storage type: s3, hdfs, azure, local.
    virtual ObjectStorageType getType() const = 0;
    virtual std::string getTypeName() const = 0;
    /// Engine name: S3, HDFS, Azure.
    virtual std::string getEngineName() const = 0;
    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual std::string getNamespaceType() const { return "namespace"; }


    // Path provided by the user in the query
    virtual Path getRawPath() const = 0;

    /// Raw URI, specified by a user. Used in permission check.
    virtual const String & getRawURI() const = 0;

    const Path & getPathForRead() const;
    // Path used for writing, it should not be globbed and might contain a partition key
    Path getPathForWrite(const std::string & partition_id = "") const;

    void setPathForRead(const Path & path)
    {
        read_path = path;
    }

    /*
     * When using `s3_create_new_file_on_insert`, each new file path generated will be appended to the path list.
     * This list is used to determine the next file name and the set of files that shall be read from remote storage.
     * This is not ideal, there are much better ways to implement reads and writes. It should be eventually removed
     */
    virtual const Paths & getPaths() const = 0;
    virtual void setPaths(const Paths & paths) = 0;

    virtual String getDataSourceDescription() const = 0;
    virtual String getNamespace() const = 0;

    virtual StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const = 0;

    /// Add/replace structure and format arguments in the AST arguments if they have 'auto' values.
    virtual void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) = 0;

    bool isNamespaceWithGlobs() const;

    virtual bool isArchive() const { return false; }
    bool isPathInArchiveWithGlobs() const;
    virtual std::string getPathInArchive() const;

    virtual void check(ContextPtr context) const;
    virtual void validateNamespace(const String & /* name */) const {}

    virtual ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) = 0;
    virtual bool isStaticConfiguration() const { return true; }

    virtual bool isDataLakeConfiguration() const { return false; }

    virtual std::optional<size_t> totalRows(ContextPtr) { return {}; }
    virtual std::optional<size_t> totalBytes(ContextPtr) { return {}; }

    virtual bool hasExternalDynamicMetadata() { return false; }

    virtual IDataLakeMetadata * getExternalMetadata() { return nullptr; }

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr, ObjectInfoPtr) const { return {}; }

    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, ObjectInfoPtr) const { return {}; }

    virtual void modifyFormatSettings(FormatSettings &) const {}

    virtual void addDeleteTransformers(
        ObjectInfoPtr object_info,
        QueryPipelineBuilder & builder,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr local_context) const;

    virtual ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        bool supports_tuple_elements,
        ContextPtr local_context,
        const PrepareReadingFromFormatHiveParams & hive_parameters);

    void initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context);

    virtual std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const;

    virtual bool supportsFileIterator() const { return false; }
    virtual bool supportsWrites() const { return true; }

    virtual bool supportsPartialPathPrefix() const { return true; }

    virtual ObjectIterator iterate(
        const ActionsDAG * /* filter_dag */,
        std::function<void(FileProgress)> /* callback */,
        size_t /* list_batch_size */,
        ContextPtr /*context*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method iterate() is not implemented for configuration type {}", getTypeName());
    }

    /// Returns true, if metadata is of the latest version, false if unknown.
    virtual bool update(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        bool if_not_updated_before,
        bool check_consistent_with_previous_metadata);

    virtual void create(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_);

    virtual SinkToStoragePtr write(
        SharedHeader /* sample_block */,
        const StorageID & /* table_id */,
        ObjectStoragePtr /* object_storage */,
        const std::optional<FormatSettings> & /* format_settings */,
        ContextPtr /* context */,
        std::shared_ptr<DataLake::ICatalog> /* catalog */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write() is not implemented for configuration type {}", getTypeName());
    }

    virtual bool supportsDelete() const { return false; }
    virtual void mutate(const MutationCommands & /*commands*/,
        ContextPtr /*context*/,
        const StorageID & /*storage_id*/,
        StorageMetadataPtr /*metadata_snapshot*/,
        std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const std::optional<FormatSettings> & /*format_settings*/) {}
    virtual void checkMutationIsPossible(const MutationCommands & /*commands*/) {}

    virtual void checkAlterIsPossible(const AlterCommands & /*commands*/) {}
    virtual void alter(const AlterCommands & /*params*/, ContextPtr /*context*/) {}

    virtual const DataLakeStorageSettings & getDataLakeSettings() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataLakeSettings() is not implemented for configuration type {}", getTypeName());
    }

    virtual ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr /**/) const { return nullptr; }

    virtual ColumnMapperPtr getColumnMapperForCurrentSchema() const { return nullptr; }


    virtual std::shared_ptr<DataLake::ICatalog> getCatalog(ContextPtr /*context*/, bool /*is_attach*/) const { return nullptr; }

    virtual bool optimize(const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, const std::optional<FormatSettings> & /*format_settings*/)
    {
        return false;
    }

    virtual void drop(ContextPtr) {}

    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    /// Whether partition column values are contained in the actual data.
    /// And alternative is with hive partitioning, when they are contained in file path.
    bool partition_columns_in_data_file = true;
    std::shared_ptr<IPartitionStrategy> partition_strategy;

protected:
    virtual void fromNamedCollection(const NamedCollection & collection, ContextPtr context) = 0;
    virtual void fromAST(ASTs & args, ContextPtr context, bool with_structure) = 0;

    void assertInitialized() const;

    bool initialized = false;

private:
    // Path used for reading, by default it is the same as `getRawPath`
    // When using `partition_strategy=hive`, a recursive reading pattern will be appended `'table_root/**.parquet'
    Path read_path;
};

using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;
using StorageObjectStorageConfigurationWeakPtr = std::weak_ptr<StorageObjectStorageConfiguration>;


}
