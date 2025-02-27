#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <TableFunctions/TableFunctionAzureBlobStorageCluster.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/StorageAzureBlob.h>

#include "registerTableFunctions.h"

#include <memory>


namespace DB
{

StoragePtr TableFunctionAzureBlobStorageCluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    StoragePtr storage;
    ColumnsDescription columns;

    if (configuration.structure != "auto")
    {
        columns = parseColumnsListFromString(configuration.structure, context);
    }
    else if (!structure_hint.empty())
    {
        columns = structure_hint;
    }

    auto client = StorageAzureBlob::createClient(configuration, !is_insert_query);
    auto settings = StorageAzureBlob::createSettings(context);

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageAzureBlob>(
            configuration,
            std::make_unique<AzureObjectStorage>(table_name, std::move(client), std::move(settings), configuration.container),
            context,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */String{},
            /* format_settings */std::nullopt, /// No format_settings
            /* distributed_processing */ true,
            /*partition_by_=*/nullptr);
    }
    else
    {
        storage = std::make_shared<StorageAzureBlobCluster>(
            cluster_name,
            configuration,
            std::make_unique<AzureObjectStorage>(table_name, std::move(client), std::move(settings), configuration.container),
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            context);
    }

    storage->startup();

    return storage;
}


void registerTableFunctionAzureBlobStorageCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionAzureBlobStorageCluster>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Azure Blob Storage in parallel for many nodes in a specified cluster.)",
            .examples{{"azureBlobStorageCluster", "SELECT * FROM  azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])", ""}}},
         .allow_readonly = false}
        );
}


}

#endif
