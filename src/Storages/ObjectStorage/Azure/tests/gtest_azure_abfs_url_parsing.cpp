#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <gtest/gtest.h>
#include <base/types.h>

namespace DB
{

namespace
{

/// Mirrors `initializeForOneLake`, which passes ".com" / the Fabric dfs suffix.
AzureStorageParsedArguments parseOneLakeURL(const String & url)
{
    AzureStorageParsedArguments args;
    String connection_url = url;
    args.fillBlobsFromURLCommon(connection_url, ".com", ".dfs.fabric.microsoft.com");
    return args;
}

}

TEST(AzureAbfsUrlParsing, OneLakeAbfssUrlIsParsed)
{
    auto args = parseOneLakeURL("abfss://myworkspace@onelake.dfs.fabric.microsoft.com/mylakehouse/Tables/t");

    EXPECT_EQ(args.connection_params.endpoint.storage_account_url, "https://onelake.dfs.fabric.microsoft.com");
    EXPECT_EQ(args.connection_params.endpoint.container_name, "myworkspace");
}

TEST(AzureAbfsUrlParsing, OneLakeAbfsUrlIsParsed)
{
    auto args = parseOneLakeURL("abfs://myworkspace@onelake.dfs.fabric.microsoft.com/mylakehouse/Tables/t");

    /// The plain spelling must resolve exactly like the secure one: an unassigned
    /// `storage_account_url` means the container was never extracted.
    EXPECT_EQ(args.connection_params.endpoint.storage_account_url, "https://onelake.dfs.fabric.microsoft.com");
    EXPECT_EQ(args.connection_params.endpoint.container_name, "myworkspace");
}

TEST(AzureAbfsUrlParsing, UnityCatalogAbfsUrlIsParsed)
{
    AzureStorageParsedArguments args;
    String connection_url = "abfs://mycontainer@mydatalakestorage.dfs.core.windows.net/subdirectory/file.txt";
    args.fillBlobsFromURLCommon(connection_url, ".net", ".blob.core.windows.net");

    EXPECT_EQ(args.connection_params.endpoint.storage_account_url, "https://mydatalakestorage.blob.core.windows.net");
    EXPECT_EQ(args.connection_params.endpoint.container_name, "mycontainer");
}

TEST(AzureAbfsUrlParsing, MalformedAbfsUrlIsRejected)
{
    AzureStorageParsedArguments args;
    String connection_url = "abfs://onelake.dfs.fabric.microsoft.com/no/at/sign";

    EXPECT_THROW(args.fillBlobsFromURLCommon(connection_url, ".com", ".dfs.fabric.microsoft.com"), DB::Exception);
}

TEST(AzureAbfsUrlParsing, NonAbfsUrlIsLeftAlone)
{
    AzureStorageParsedArguments args;
    String connection_url = "https://mystorageaccount.blob.core.windows.net/mycontainer/path";
    args.fillBlobsFromURLCommon(connection_url, ".net", ".blob.core.windows.net");

    EXPECT_TRUE(args.connection_params.endpoint.storage_account_url.empty());
}

TEST(AzureAbfsUrlParsing, NearPrefixSchemeIsNotTreatedAsAbfs)
{
    AzureStorageParsedArguments args;
    String connection_url = "abfsx://mycontainer@mydatalakestorage.dfs.core.windows.net/p";
    args.fillBlobsFromURLCommon(connection_url, ".net", ".blob.core.windows.net");

    EXPECT_TRUE(args.connection_params.endpoint.storage_account_url.empty());
}

}

#endif
