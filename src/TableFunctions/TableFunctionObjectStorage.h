#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
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

template <typename Definition, typename Configuration, bool is_data_lake = false>
class TableFunctionObjectStorage : public ITableFunction
{
public:
    static constexpr auto name = Definition::name;
    using Settings = typename std::conditional_t<
        is_data_lake,
        DataLakeStorageSettings,
        StorageObjectStorageSettings>;

    String getName() const override { return name; }

    bool hasStaticStructure() const override { return table_options.structure != "auto"; }

    bool needStructureHint() const override { return table_options.structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

    bool supportsReadingSubsetOfColumns(const ContextPtr & context) override
    {
        return table_options.format != "auto" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(table_options.format, context);
    }

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
    {
        return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
    }

    virtual void parseArgumentsImpl(ASTs & args, const ContextPtr & context)
    {
        auto [config, opts]
            = ObjectStorageConnectionConfiguration::initialize(Configuration::type, args, context, true, nullptr, getDiskName());
        configuration = config;
        table_options = std::move(opts);
        if constexpr (is_data_lake)
        {
            if (table_options.format == "auto")
                table_options.format = "Parquet";
        }
    }

    String getDiskName() const;

    static void updateStructureAndFormatArgumentsIfNeeded(
      ASTs & args,
      const String & structure,
      const String & format,
      const ContextPtr & context)
    {
        Configuration().addStructureAndFormatToArgsIfNeeded(args, structure, format, context, /*with_structure=*/true);
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
    ObjectStorageConnectionConfigurationPtr getConfiguration(ContextPtr context) const;

    static std::shared_ptr<Settings> createEmptySettings();

    mutable ObjectStorageConnectionConfigurationPtr configuration;
    mutable StorageObjectStorageTableOptions table_options;
    mutable ObjectStoragePtr object_storage;
    ColumnsDescription structure_hint;
    std::shared_ptr<Settings> settings;
    ASTPtr partition_by;

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;
};

#if USE_AWS_S3
using TableFunctionS3 = TableFunctionObjectStorage<S3Definition, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
using TableFunctionAzureBlob = TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>;
#endif

#if USE_HDFS
using TableFunctionHDFS = TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>;
#endif


#if USE_AVRO
#    if USE_AWS_S3
using TableFunctionIceberg = TableFunctionObjectStorage<IcebergDefinition, StorageS3Configuration, true>;
using TableFunctionIcebergS3 = TableFunctionObjectStorage<IcebergS3Definition, StorageS3Configuration, true>;
#    endif
#    if USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzure = TableFunctionObjectStorage<IcebergAzureDefinition, StorageAzureConfiguration, true>;
#    endif
#    if USE_HDFS
using TableFunctionIcebergHDFS = TableFunctionObjectStorage<IcebergHDFSDefinition, StorageHDFSConfiguration, true>;
#    endif
using TableFunctionIcebergLocal = TableFunctionObjectStorage<IcebergLocalDefinition, StorageLocalConfiguration, true>;
#endif
#if USE_AVRO
#    if USE_AWS_S3
using TableFunctionPaimon = TableFunctionObjectStorage<PaimonDefinition, StorageS3Configuration, true>;
using TableFunctionPaimonS3 = TableFunctionObjectStorage<PaimonS3Definition, StorageS3Configuration, true>;
#    endif
#    if USE_AZURE_BLOB_STORAGE
using TableFunctionPaimonAzure = TableFunctionObjectStorage<PaimonAzureDefinition, StorageAzureConfiguration, true>;
#    endif
#    if USE_HDFS
using TableFunctionPaimonHDFS = TableFunctionObjectStorage<PaimonHDFSDefinition, StorageHDFSConfiguration, true>;
#    endif
using TableFunctionPaimonLocal = TableFunctionObjectStorage<PaimonLocalDefinition, StorageLocalConfiguration, true>;
#endif
#if USE_PARQUET && USE_DELTA_KERNEL_RS
#if USE_AWS_S3
using TableFunctionDeltaLake = TableFunctionObjectStorage<DeltaLakeDefinition, StorageS3Configuration, true>;
using TableFunctionDeltaLakeS3 = TableFunctionObjectStorage<DeltaLakeS3Definition, StorageS3Configuration, true>;
#endif
#if USE_AZURE_BLOB_STORAGE
using TableFunctionDeltaLakeAzure = TableFunctionObjectStorage<DeltaLakeAzureDefinition, StorageAzureConfiguration, true>;
#endif
// New alias for local Delta Lake table function
using TableFunctionDeltaLakeLocal = TableFunctionObjectStorage<DeltaLakeLocalDefinition, StorageLocalConfiguration, true>;
#endif
#if USE_AWS_S3
using TableFunctionHudi = TableFunctionObjectStorage<HudiDefinition, StorageS3Configuration, true>;
#endif
}
