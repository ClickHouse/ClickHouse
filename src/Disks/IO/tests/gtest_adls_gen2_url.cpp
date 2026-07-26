#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <gtest/gtest.h>

#include <Poco/URI.h>
#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>

using namespace DB;

namespace
{

/// A OneLake endpoint: the host is the Blob endpoint (the default), while writes
/// must go to the DFS host. Empty account_name keeps getContainerEndpoint from
/// adding an extra path segment.
AzureBlobStorage::Endpoint makeOneLakeBlobEndpoint()
{
    AzureBlobStorage::Endpoint endpoint;
    endpoint.storage_account_url = "https://onelake.blob.fabric.microsoft.com";
    endpoint.account_name = "";
    endpoint.container_name = "11111111-2222-3333-4444-555555555555";
    endpoint.prefix = "lakehouse-guid/Tables/dbo/mytable";
    return endpoint;
}

}

TEST(AdlsGen2Url, OneLakeBlobEndpointIsGen2)
{
    EXPECT_TRUE(isAdlsGen2Endpoint(makeOneLakeBlobEndpoint()));
}

/// The write URL must target the DFS host; the Blob host does not serve the DFS API.
TEST(AdlsGen2Url, OneLakeUrlTargetsDfsHost)
{
    const String url = buildAdlsGen2FileUrl(makeOneLakeBlobEndpoint(), "data/data-0.parquet");
    EXPECT_EQ(Poco::URI(url).getHost(), "onelake.dfs.fabric.microsoft.com") << url;
}

/// A regular Azure account already on a .dfs host is left as-is.
TEST(AdlsGen2Url, NonFabricDfsHostUnchanged)
{
    AzureBlobStorage::Endpoint endpoint;
    endpoint.storage_account_url = "https://myacct.dfs.core.windows.net";
    endpoint.container_name = "mycontainer";
    endpoint.prefix = "path/to/table";

    const String url = buildAdlsGen2FileUrl(endpoint, "data/data-0.parquet");
    EXPECT_EQ(Poco::URI(url).getHost(), "myacct.dfs.core.windows.net");
}

/// A host that only contains the Fabric suffix as a substring is not a Fabric
/// host: not treated as Gen2, not retargeted.
TEST(AdlsGen2Url, NonFabricLookalikeHostNotRetargeted)
{
    AzureBlobStorage::Endpoint endpoint;
    endpoint.storage_account_url = "https://proxy-blob.fabric.microsoft.com.example.com";
    endpoint.container_name = "c";
    endpoint.prefix = "p";

    EXPECT_FALSE(isAdlsGen2Endpoint(endpoint));
    const String url = buildAdlsGen2FileUrl(endpoint, "data/data-0.parquet");
    EXPECT_EQ(Poco::URI(url).getHost(), "proxy-blob.fabric.microsoft.com.example.com");
}

#endif
