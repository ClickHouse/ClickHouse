#pragma once

#include <Interpreters/SecretArgumentsSpec.h>

namespace DB
{

/// `azureBlobStorage` and the Azure data lake table functions; `azureBlobStorageCluster` and the Azure data lake cluster ones.
SecretArgumentsSpec azureTableFunctionSecretArguments(bool is_cluster_function);
/// The `AzureBlobStorage` and `AzureQueue` table engines.
SecretArgumentsSpec azureTableEngineSecretArguments();
/// `BACKUP ... TO AzureBlobStorage(...)`.
SecretArgumentsSpec azureBackupSecretArguments();

}
