#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Storages/VirtualColumnUtils.h>
#include <TableFunctions/ITableFunction.h>

#include "config.h"


namespace DB
{

class Context;
class StorageS3Configuration;
class StorageAzureConfiguration;
class StorageHDFSConfiguration;
class StorageLocalConfiguration;
struct S3StorageSettings;
struct AzureStorageSettings;
struct HDFSStorageSettings;

template <typename Definition, typename Configuration>
class TableFunctionDeltaLakeImpl : public ITableFunction
{
public:
    static constexpr auto name = Definition::name;
    using Settings = DataLakeStorageSettings;

    String getName() const override { return name; }

    bool hasStaticStructure() const override { return configuration->structure != "auto"; }

    bool needStructureHint() const override { return configuration->structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override
    {
        return configuration->format != "auto" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context);
    }

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
    {
        return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
    }

    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context)
    {
        StorageObjectStorageConfiguration::initialize(*getConfiguration(context), args, context, true);
    }

    static void updateStructureAndFormatArgumentsIfNeeded(
      ASTs & args,
      const String & structure,
      const String & format,
      const ContextPtr & context)
    {
        Configuration configuration(createEmptySettings());
        if (configuration.format == "auto")
            configuration.format = "Parquet"; /// Default format of data lakes.

        configuration.addStructureAndFormatToArgsIfNeeded(args, structure, format, context, /*with_structure=*/true);
    }

    void setPartitionBy(const ASTPtr & partition_by_) override
    {
        partition_by = partition_by_;
    }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return Definition::storage_engine_name; }
    const String & getFunctionURI() const override { return configuration->getRawURI(); }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ObjectStoragePtr getObjectStorage(const ContextPtr & context, bool create_readonly) const;
    StorageObjectStorageConfigurationPtr getConfiguration(ContextPtr context) const;

    static std::shared_ptr<Settings> createEmptySettings();

    mutable StorageObjectStorageConfigurationPtr configuration;
    mutable ObjectStoragePtr object_storage;
    ColumnsDescription structure_hint;
    std::shared_ptr<Settings> settings;
    ASTPtr partition_by;

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;
};

#if USE_PARQUET && USE_DELTA_KERNEL_RS && USE_AWS_S3
using TableFunctionDeltaLake = TableFunctionDeltaLakeImpl<DeltaLakeDefinition, StorageS3DeltaLakeConfiguration>;
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS && USE_AWS_S3
using TableFunctionDeltaLakeS3 = TableFunctionDeltaLakeImpl<DeltaLakeS3Definition, StorageS3DeltaLakeConfiguration>;
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS && USE_AZURE_BLOB_STORAGE
using TableFunctionDeltaLakeAzure = TableFunctionDeltaLakeImpl<DeltaLakeAzureDefinition, StorageAzureDeltaLakeConfiguration>;
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS
using TableFunctionDeltaLakeLocal = TableFunctionDeltaLakeImpl<DeltaLakeLocalDefinition, StorageLocalDeltaLakeConfiguration>;
#endif
}
