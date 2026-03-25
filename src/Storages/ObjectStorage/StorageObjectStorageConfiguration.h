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
struct StorageObjectStorageTableOptions;

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


class ObjectStorageConnectionConfiguration
{
public:
    using CredentialsConfigurationCallback = std::optional<std::function<std::shared_ptr<DataLake::IStorageCredentials>()>>;

    ObjectStorageConnectionConfiguration() = default;
    virtual ~ObjectStorageConnectionConfiguration() = default;

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

    /// Create a configuration of the given type, initialize it from AST / NamedCollection / disk,
    /// and return both the configuration and the extracted table options.
    /// Create a configuration of the given type, initialize it, and return both.
    static std::pair<std::shared_ptr<ObjectStorageConnectionConfiguration>, StorageObjectStorageTableOptions> initialize(
        ObjectStorageType type,
        ASTs & engine_args,
        ContextPtr local_context,
        bool with_table_structure,
        const StorageID * table_id = nullptr,
        const String & disk_name = "");
    /// Create a configuration shared_ptr of the given concrete type.
    static std::shared_ptr<ObjectStorageConnectionConfiguration> createByType(ObjectStorageType type);


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

    /// Returns the raw path as the default reading path.
    /// The partition-strategy-aware read path is managed by `StorageObjectStorageTableOptions`.
    Path getPathForRead() const { return getRawPath(); }

    /// Raw URI, specified by a user. Used in permission check.
    virtual const String & getRawURI() const = 0;

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

    virtual void check(ContextPtr /* context */) {}
    virtual void validateNamespace(const String & /* name */) const {}

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback);
    virtual ObjectStoragePtr
    createObjectStorageImpl(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback)
        = 0;
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

    virtual bool supportsWrites() const { return true; }

    virtual bool supportsPartialPathPrefix() const { return true; }

    virtual void update(ObjectStoragePtr object_storage, ContextPtr local_context);
    virtual void lazyInitializeIfNeeded(ObjectStoragePtr object_storage, ContextPtr local_context);

    virtual bool supportsPrewhere() const
    {
        return true;
    }

    virtual void drop(ContextPtr) {}

    /// Apply post-initialization processing: namespace validation, format detection,
    /// partition promotion, read_path setup.
    static StorageObjectStorageTableOptions postInitializeExisting(
        ObjectStorageConnectionConfiguration & configuration_to_initialize,
        StorageObjectStorageTableOptions & table_options,
        ContextPtr local_context,
        const String & disk_name = "");

protected:
    void assertInitialized() const;

    bool initialized = false;

    /// Object storage obtained from a named disk during `fromDisk` initialization.
    /// Used by `createObjectStorage` to return the pre-created storage instead of creating a new one.
    ObjectStoragePtr ready_object_storage;
};

using ObjectStorageConnectionConfigurationPtr = std::shared_ptr<ObjectStorageConnectionConfiguration>;
using ObjectStorageConnectionConfigurationWeakPtr = std::weak_ptr<ObjectStorageConnectionConfiguration>;
using ConfigWithOptions = std::pair<ObjectStorageConnectionConfigurationPtr, StorageObjectStorageTableOptions>;

}
