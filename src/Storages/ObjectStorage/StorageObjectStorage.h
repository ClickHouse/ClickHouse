#pragma once

#include <Common/re2.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB
{

struct SelectQueryInfo;
class StorageObjectStorageConfiguration;
struct S3StorageSettings;
struct HDFSStorageSettings;
struct AzureStorageSettings;
class PullingPipelineExecutor;
using ReadTaskCallback = std::function<String()>;
class IOutputFormat;
class IInputFormat;
class SchemaCache;
class ReadBufferIterator;


template <typename StorageSettings>
class StorageObjectStorage : public IStorage
{
public:
    using Configuration = StorageObjectStorageConfiguration;
    using ConfigurationPtr = std::shared_ptr<Configuration>;
    using ObjectInfo = RelativePathWithMetadata;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    StorageObjectStorage(
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const String & engine_name_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override { return engine_name; }

    void read(
        QueryPlan & query_plan,
        const Names &,
        const StorageSnapshotPtr &,
        SelectQueryInfo &,
        ContextPtr,
        QueryProcessingStage::Enum,
        size_t,
        size_t) override;

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

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    static Names getVirtualColumnNames();

    bool supportsPartitionBy() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & context);

    static ColumnsDescription getTableStructureFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static void setFormatFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

protected:
    virtual void updateConfiguration(ContextPtr local_context);

    static std::unique_ptr<ReadBufferIterator> createReadBufferIterator(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ObjectInfos & read_keys,
        const ContextPtr & context);

    const std::string engine_name;
    const NamesAndTypesList virtual_columns;
    std::optional<FormatSettings> format_settings;
    const ASTPtr partition_by;
    const bool distributed_processing;

    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    std::mutex configuration_update_mutex;
};

using StorageS3 = StorageObjectStorage<S3StorageSettings>;
using StorageAzureBlob = StorageObjectStorage<AzureStorageSettings>;
using StorageHDFS = StorageObjectStorage<HDFSStorageSettings>;

}
