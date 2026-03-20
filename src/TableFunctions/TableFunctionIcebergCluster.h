#pragma once
#include "config.h"

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <TableFunctions/TableFunctionIceberg.h>
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
class TableFunctionIcebergClusterImpl : public ITableFunctionCluster<TableFunctionIcebergImpl<Definition, Configuration>>
{
public:
    static constexpr auto name = Definition::name;

    String getName() const override { return name; }

protected:
    using Base = TableFunctionIcebergImpl<Definition, Configuration>;

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

#if USE_AVRO
using TableFunctionIcebergLocalCluster = TableFunctionIcebergClusterImpl<IcebergLocalClusterDefinition, StorageLocalConfiguration>;
#endif
#if USE_AVRO && USE_AWS_S3
using TableFunctionIcebergS3Cluster = TableFunctionIcebergClusterImpl<IcebergS3ClusterDefinition, StorageS3Configuration>;
#endif
#if USE_AVRO && USE_AWS_S3
using TableFunctionIcebergCluster = TableFunctionIcebergClusterImpl<IcebergClusterDefinition, StorageS3Configuration>;
#endif
#if USE_AVRO && USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzureCluster = TableFunctionIcebergClusterImpl<IcebergAzureClusterDefinition, StorageAzureConfiguration>;
#endif
#if USE_AVRO && USE_HDFS
using TableFunctionIcebergHDFSCluster = TableFunctionIcebergClusterImpl<IcebergHDFSClusterDefinition, StorageHDFSConfiguration>;
#endif
}
