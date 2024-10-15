#pragma once

#include "config.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IStorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

template <typename Name, typename Storage, typename TableFunction>
class ITableFunctionDataLake : public TableFunction
{
public:
    static constexpr auto name = Name::name;
    std::string getName() const override { return name; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool /*is_insert_query*/) const override
    {
        ColumnsDescription columns;
        auto configuration = TableFunction::getConfiguration();
        if (configuration->structure != "auto")
            columns = parseColumnsListFromString(configuration->structure, context);
        else if (!cached_columns.empty())
            columns = cached_columns;

        StoragePtr storage = Storage::create(
            configuration, context, StorageID(TableFunction::getDatabaseName(), table_name),
            columns, ConstraintsDescription{}, String{}, std::nullopt, LoadingStrictnessLevel::CREATE);

        storage->startup();
        return storage;
    }

    const char * getStorageTypeName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override
    {
        auto configuration = TableFunction::getConfiguration();
        if (configuration->structure == "auto")
        {
            context->checkAccess(TableFunction::getSourceAccessType());
            auto object_storage = TableFunction::getObjectStorage(context, !is_insert_query);
            return Storage::getTableStructureFromData(object_storage, configuration, std::nullopt, context);
        }

        return parseColumnsListFromString(configuration->structure, context);
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        auto configuration = TableFunction::getConfiguration();
        configuration->format = "Parquet";
        /// Set default format to Parquet if it's not specified in arguments.
        TableFunction::parseArguments(ast_function, context);
    }
};

struct TableFunctionIcebergName
{
    static constexpr auto name = "iceberg";
};

struct TableFunctionIcebergS3Name
{
    static constexpr auto name = "icebergS3";
};

struct TableFunctionIcebergAzureName
{
    static constexpr auto name = "icebergAzure";
};

struct TableFunctionIcebergLocalName
{
    static constexpr auto name = "icebergLocal";
};

struct TableFunctionIcebergHDFSName
{
    static constexpr auto name = "icebergHDFS";
};

struct TableFunctionDeltaLakeName
{
    static constexpr auto name = "deltaLake";
};

struct TableFunctionHudiName
{
    static constexpr auto name = "hudi";
};

#if USE_AVRO
#    if USE_AWS_S3
using TableFunctionIceberg = ITableFunctionDataLake<TableFunctionIcebergName, StorageIceberg, TableFunctionS3>;
using TableFunctionIcebergS3 = ITableFunctionDataLake<TableFunctionIcebergS3Name, StorageIceberg, TableFunctionS3>;
#    endif
#    if USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzure = ITableFunctionDataLake<TableFunctionIcebergAzureName, StorageIceberg, TableFunctionAzureBlob>;
#    endif
using TableFunctionIcebergLocal = ITableFunctionDataLake<TableFunctionIcebergLocalName, StorageIceberg, TableFunctionLocal>;
#if USE_HDFS
using TableFunctionIcebergHDFS = ITableFunctionDataLake<TableFunctionIcebergHDFSName, StorageIceberg, TableFunctionHDFS>;
#endif
#endif
#if USE_AWS_S3
#    if USE_PARQUET
using TableFunctionDeltaLake = ITableFunctionDataLake<TableFunctionDeltaLakeName, StorageDeltaLake, TableFunctionS3>;
#endif
using TableFunctionHudi = ITableFunctionDataLake<TableFunctionHudiName, StorageHudi, TableFunctionS3>;
#endif
}
