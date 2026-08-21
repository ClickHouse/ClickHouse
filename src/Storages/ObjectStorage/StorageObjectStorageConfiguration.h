#pragma once

#include <Storages/IPartitionStrategy.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Interpreters/ActionsDAG.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Interpreters/StorageID.h>
#include <Databases/DataLake/ICatalog.h>
#include <Storages/MutationCommands.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Databases/DataLake/StorageCredentials.h>

namespace DB
{

class NamedCollection;
class SinkToStorage;
class IDataLakeMetadata;
struct IObjectIterator;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;
using ObjectIterator = std::shared_ptr<IObjectIterator>;

struct StorageParsedArguments;

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
    using CredentialsConfigurationCallback = std::optional<std::function<std::shared_ptr<DataLake::IStorageCredentials>()>>;

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
        bool with_table_structure,
        const StorageID * table_id = nullptr);

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

    virtual void check(ContextPtr context);
    virtual void validateNamespace(const String & /* name */) const {}

    virtual ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) = 0;
    virtual bool isStaticConfiguration() const { return true; }

    virtual bool isDataLakeConfiguration() const { return false; }

    virtual bool supportsTotalRows(ContextPtr, ObjectStorageType) const { return false; }
    virtual std::optional<size_t> totalRows(ContextPtr) { return {}; }
    virtual bool supportsTotalBytes(ContextPtr, ObjectStorageType) const { return false; }
    virtual std::optional<size_t> totalBytes(ContextPtr) { return {}; }
    /// NOTE: In this function we are going to check is data which we are going to read sorted by sorting key specified in StorageMetadataPtr.
    /// It may look confusing that this function checks only StorageMetadataPtr, and not StorageSnapshot.
    /// However snapshot_id is specified in StorageMetadataPtr, so we can extract necessary information from it.
    virtual bool isDataSortedBySortingKey(StorageMetadataPtr, ContextPtr) const { return false; }

    virtual IDataLakeMetadata * getExternalMetadata() { return nullptr; }

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr, ObjectInfoPtr) const { return {}; }

    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, ObjectInfoPtr) const { return {}; }

    virtual void modifyFormatSettings(FormatSettings &, const Context &) const {}

    virtual void addDeleteTransformers(
        ObjectInfoPtr object_info,
        QueryPipelineBuilder & builder,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
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

    virtual std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr local_context) const;
    virtual std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(const DataLakeTableStateSnapshot & state, ContextPtr local_context) const;
    virtual bool shouldReloadSchemaForConsistency(ContextPtr local_context) const;
    virtual std::optional<ColumnsDescription> tryGetTableStructureFromMetadata(ContextPtr local_context) const;

    virtual bool supportsFileIterator() const { return false; }
    virtual bool supportsParallelInsert() const { return false; }
    virtual bool supportsWrites() const { return true; }

    virtual bool supportsPartialPathPrefix() const { return true; }

    virtual ObjectIterator iterate(
        const ActionsDAG * /* filter_dag */,
        std::function<void(FileProgress)> /* callback */,
        size_t /* list_batch_size */,
        StorageMetadataPtr /*storage_metadata*/,
        ContextPtr /*context*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method iterate() is not implemented for configuration type {}", getTypeName());
    }

    /// Returns true, if metadata is of the latest version, false if unknown.
    virtual void update(ObjectStoragePtr object_storage, ContextPtr local_context, bool if_not_updated_before);

    virtual void create(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        ASTPtr order_by,
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
        const std::optional<FormatSettings> & /*format_settings*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support mutations", getTypeName());
    }
    virtual void checkMutationIsPossible(const MutationCommands & /*commands*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support mutations", getTypeName());
    }

    virtual void checkAlterIsPossible(const AlterCommands & commands)
    {
        for (const auto & command : commands)
        {
            if (!command.isCommentAlter())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                    command.type, getEngineName());
        }
    }

    virtual void alter(const AlterCommands & /*params*/, ContextPtr /*context*/) {}

    virtual const DataLakeStorageSettings & getDataLakeSettings() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataLakeSettings() is not implemented for configuration type {}", getTypeName());
    }

    virtual ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr /**/) const { return nullptr; }

    virtual ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr /**/, ContextPtr /**/) const { return nullptr; }


    virtual std::shared_ptr<DataLake::ICatalog> getCatalog(ContextPtr /*context*/, bool /*is_attach*/) const { return nullptr; }

    virtual bool optimize(const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, const std::optional<FormatSettings> & /*format_settings*/)
    {
        return false;
    }

    virtual bool supportsPrewhere() const
    {
        return true;
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
    void initializeFromParsedArguments(const StorageParsedArguments & parsed_arguments);
    virtual void fromNamedCollection(const NamedCollection & collection, ContextPtr context) = 0;
    virtual void fromAST(ASTs & args, ContextPtr context, bool with_structure) = 0;
    virtual void fromDisk(const String & /*disk_name*/, ASTs & /*args*/, ContextPtr /*context*/, bool /*with_structure*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method fromDisk is not implemented");
    }

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
