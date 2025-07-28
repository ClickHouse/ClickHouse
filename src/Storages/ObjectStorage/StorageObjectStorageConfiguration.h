#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Interpreters/ActionsDAG.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>


namespace DB
{

class NamedCollection;

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

    using Path = std::string;
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

    virtual Path getFullPath() const { return ""; }
    virtual Path getPath() const = 0;
    virtual void setPath(const Path & path) = 0;

    virtual const Paths & getPaths() const = 0;
    virtual void setPaths(const Paths & paths) = 0;

    virtual String getDataSourceDescription() const = 0;
    virtual String getNamespace() const = 0;

    virtual StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const = 0;

    /// Add/replace structure and format arguments in the AST arguments if they have 'auto' values.
    virtual void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) = 0;

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
    virtual bool isStaticConfiguration() const { return true; }

    virtual bool isDataLakeConfiguration() const { return false; }

    virtual std::optional<size_t> totalRows(ContextPtr) { return {}; }
    virtual std::optional<size_t> totalBytes(ContextPtr) { return {}; }

    virtual bool hasExternalDynamicMetadata() { return false; }

    virtual IDataLakeMetadata * getExternalMetadata() { return nullptr; }

    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr, const String &) const { return {}; }

    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr, const String &) const { return {}; }

    virtual void modifyFormatSettings(FormatSettings &) const {}

    virtual ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context);

    virtual std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const;

    virtual bool supportsFileIterator() const { return false; }
    virtual bool supportsWrites() const { return true; }

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
        bool if_not_exists);

    virtual const DataLakeStorageSettings & getDataLakeSettings() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataLakeSettings() is not implemented for configuration type {}", getTypeName());
    }

    virtual ColumnMapperPtr getColumnMapper() const { return nullptr; }

    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";

protected:
    virtual void fromNamedCollection(const NamedCollection & collection, ContextPtr context) = 0;
    virtual void fromAST(ASTs & args, ContextPtr context, bool with_structure) = 0;

    void assertInitialized() const;

    bool initialized = false;
};

using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;
using StorageObjectStorageConfigurationWeakPtr = std::weak_ptr<StorageObjectStorageConfiguration>;


}
