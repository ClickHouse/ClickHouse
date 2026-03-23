#pragma once

#include <Storages/IPartitionStrategy.h>
#include <Formats/FormatSettings.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <Common/Exception.h>

namespace DB
{

class NamedCollection;
struct StorageID;

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

    static constexpr auto SCHEMA_HASH_WILDCARD = "{_schema_hash}";

    struct Path
    {
        Path() = default;
        /// A partial prefix is a prefix that does not represent an actual object (directory or file), usually strings that do not end with a slash character.
        /// Example: `table_root/year=20`. AWS S3 supports partial prefixes, but HDFS does not.
        Path(const std::string & path_) : path(path_) {} /// NOLINT(google-explicit-constructor)

        std::string path;

        bool hasPartitionWildcard() const;
        bool hasSchemaHashWildcard() const;
        bool hasGlobsIgnorePlaceholders() const;
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
        const StorageID * table_id = nullptr,
        const String & disk_name = "");

    /// Storage type: s3, hdfs, azure, local.
    virtual ObjectStorageType getType() const = 0;
    virtual std::string getTypeName() const = 0;
    /// Engine name: S3, HDFS, Azure.
    virtual std::string getEngineName() const = 0;
    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual std::string getNamespaceType() const { return "namespace"; }


    /// Base path for the object key. May be modified after construction by placeholder resolution.
    virtual Path getRawPath() const = 0;
    virtual void setRawPath(const Path & path) = 0;

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

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback);
    virtual ObjectStoragePtr doCreateObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) = 0;
    virtual bool isStaticConfiguration() const { return true; }



    virtual void modifyFormatSettings(FormatSettings &, const Context &) const {}

    virtual ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        bool supports_tuple_elements,
        ContextPtr local_context,
        const PrepareReadingFromFormatHiveParams & hive_parameters);

    static String computeSchemaHash(const ColumnsDescription & columns);
    void setSchemaHash(const String & hash);

    void initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context);

    virtual bool supportsWrites() const { return true; }

    virtual bool supportsPartialPathPrefix() const { return true; }

    virtual void update(ObjectStoragePtr object_storage, ContextPtr local_context);
    virtual void lazyInitializeIfNeeded(ObjectStoragePtr object_storage, ContextPtr local_context);

    const DataLakeStorageSettings & getDataLakeSettings() const
    {
        chassert(datalake_settings);
        return *datalake_settings;
    }

    void setDataLakeSettings(DataLakeStorageSettingsPtr settings_) { datalake_settings = std::move(settings_); }

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
    String schema_hash;

    /// Object storage obtained from a named disk during `fromDisk` initialization.
    /// Used by `createObjectStorage` to return the pre-created storage instead of creating a new one.
    ObjectStoragePtr ready_object_storage;

    /// DataLake storage settings, injected by StorageDataLake/StorageDataLakeCluster.
    DataLakeStorageSettingsPtr datalake_settings;

private:
    // Path used for reading, by default it is the same as `getRawPath`
    // When using `partition_strategy=hive`, a recursive reading pattern will be appended `'table_root/**.parquet'
    Path read_path;
};

using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;
using StorageObjectStorageConfigurationWeakPtr = std::weak_ptr<StorageObjectStorageConfiguration>;


}
