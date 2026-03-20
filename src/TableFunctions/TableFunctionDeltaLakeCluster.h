#pragma once
#include "config.h"

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <TableFunctions/TableFunctionDeltaLake.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>


namespace DB
{

class Context;

class StorageS3Settings;
class StorageAzureBlobSettings;
class StorageS3Configuration;
class StorageAzureConfiguration;

ContextPtr getQueryOrGlobalContext();

template <typename Definition, typename Configuration>
class TableFunctionDeltaLakeClusterImpl : public ITableFunctionCluster<TableFunctionDeltaLakeImpl<Definition, Configuration>>
{
public:
    static constexpr auto name = Definition::name;

    String getName() const override { return name; }

protected:
    using Base = TableFunctionDeltaLakeImpl<Definition, Configuration>;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return Definition::storage_engine_name; }
    const char * getNonClusteredStorageEngineName() const override { return Definition::non_clustered_storage_engine_name; }
    bool hasStaticStructure() const override { return Base::getConfiguration(getQueryOrGlobalContext())->structure != "auto"; }
    bool needStructureHint() const override { return Base::getConfiguration(getQueryOrGlobalContext())->structure == "auto"; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { Base::structure_hint = structure_hint_; }
};

#if USE_AWS_S3 && USE_PARQUET && USE_DELTA_KERNEL_RS
using TableFunctionDeltaLakeCluster = TableFunctionDeltaLakeClusterImpl<DeltaLakeClusterDefinition, StorageS3Configuration>;
#endif
#if USE_AWS_S3 && USE_PARQUET && USE_DELTA_KERNEL_RS
using TableFunctionDeltaLakeS3Cluster = TableFunctionDeltaLakeClusterImpl<DeltaLakeS3ClusterDefinition, StorageS3Configuration>;
#endif
#if USE_PARQUET && USE_AZURE_BLOB_STORAGE && USE_DELTA_KERNEL_RS
using TableFunctionDeltaLakeAzureCluster = TableFunctionDeltaLakeClusterImpl<DeltaLakeAzureClusterDefinition, StorageAzureConfiguration>;
#endif
}
