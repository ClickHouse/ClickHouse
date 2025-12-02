#pragma once

#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
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
class TableFunctionDataLake : public ITableFunction
{
public:
    static constexpr auto name = Definition::name;

    String getName() const override { return name; }

    bool hasStaticStructure() const override { return false; }

    bool needStructureHint() const override { return false; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & /**/) override
    {
        return false;
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

#if USE_AVRO
#    if USE_AWS_S3
using TableFunctionIceberg = TableFunctionDataLake<IcebergDefinition, StorageS3IcebergConfiguration>;
using TableFunctionIcebergS3 = TableFunctionDataLake<IcebergS3Definition, StorageS3IcebergConfiguration>;
#    endif
#    if USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzure = TableFunctionDataLake<IcebergAzureDefinition, StorageAzureIcebergConfiguration>;
#    endif
#    if USE_HDFS
using TableFunctionIcebergHDFS = TableFunctionDataLake<IcebergHDFSDefinition, StorageHDFSIcebergConfiguration>;
#    endif
using TableFunctionIcebergLocal = TableFunctionDataLake<IcebergLocalDefinition, StorageLocalIcebergConfiguration>;
#endif
#if USE_AVRO
#    if USE_AWS_S3
using TableFunctionPaimon = TableFunctionDataLake<PaimonDefinition, StorageS3PaimonConfiguration>;
using TableFunctionPaimonS3 = TableFunctionDataLake<PaimonS3Definition, StorageS3PaimonConfiguration>;
#    endif
#    if USE_AZURE_BLOB_STORAGE
using TableFunctionPaimonAzure = TableFunctionDataLake<PaimonAzureDefinition, StorageAzurePaimonConfiguration>;
#    endif
#    if USE_HDFS
using TableFunctionPaimonHDFS = TableFunctionDataLake<PaimonHDFSDefinition, StorageHDFSPaimonConfiguration>;
#    endif
using TableFunctionPaimonLocal = TableFunctionDataLake<PaimonLocalDefinition, StorageLocalPaimonConfiguration>;
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS
#if USE_AWS_S3
using TableFunctionDeltaLake = TableFunctionDataLake<DeltaLakeDefinition, StorageS3DeltaLakeConfiguration>;
using TableFunctionDeltaLakeS3 = TableFunctionDataLake<DeltaLakeS3Definition, StorageS3DeltaLakeConfiguration>;
#endif
#if USE_AZURE_BLOB_STORAGE
using TableFunctionDeltaLakeAzure = TableFunctionDataLake<DeltaLakeAzureDefinition, StorageAzureDeltaLakeConfiguration>;
#endif
// New alias for local Delta Lake table function
using TableFunctionDeltaLakeLocal = TableFunctionDataLake<DeltaLakeLocalDefinition, StorageLocalDeltaLakeConfiguration>;
#endif
#if USE_AWS_S3
using TableFunctionHudi = TableFunctionDataLake<HudiDefinition, StorageS3HudiConfiguration>;
#endif
}
