#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionAzureBlobStorage.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <Storages/StorageAzureBlobCluster.h>


namespace DB
{

class Context;

/**
 * azureBlobStorageCluster(cluster_name, source, [access_key_id, secret_access_key,] format, compression_method, structure)
 * A table function, which allows to process many files from Azure Blob Storage on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
 * in Azure Blob Storage file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionAzureBlobStorageCluster : public ITableFunctionCluster<TableFunctionAzureBlobStorage>
{
public:
    static constexpr auto name = "azureBlobStorageCluster";
    static constexpr auto signature = " - cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure]";

    String getName() const override
    {
        return name;
    }

    String getSignature() const override
    {
        return signature;
    }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "AzureBlobStorageCluster"; }
};

}

#endif
