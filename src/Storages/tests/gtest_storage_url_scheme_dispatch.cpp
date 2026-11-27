#include <gtest/gtest.h>

#include <Storages/StorageURL.h>

using namespace DB;

/// `parseAzureURL` decomposes an `az`/`azure`/`abfss`/`abfs` URL into the (account_url, container,
/// blob_path) arguments the `AzureBlobStorage` engine expects when the unified `URL` engine/`url`
/// function dispatches an Azure-shaped URL. `AzureBlobStorage::processURL` recovers a SAS token only
/// from the account/connection URL (it splits that argument on `?`), so the SAS query must be carried
/// on `account_url` rather than left on the blob path; otherwise authenticated links silently break.

TEST(StorageURLSchemeDispatch, ParseAzureURLWithoutQuery)
{
    const auto parts = parseAzureURL("az://account.blob.core.windows.net/container/path/blob.csv");
    EXPECT_EQ(parts.account_url, "https://account.blob.core.windows.net");
    EXPECT_EQ(parts.container, "container");
    EXPECT_EQ(parts.blob_path, "path/blob.csv");
}

TEST(StorageURLSchemeDispatch, ParseAzureURLKeepsSASOnAccountURL)
{
    /// The SAS query must end up on `account_url` (where `processURL` looks for it), not on `blob_path`.
    const auto parts = parseAzureURL("az://account.blob.core.windows.net/container/blob.csv?sp=r&sig=abc");
    EXPECT_EQ(parts.account_url, "https://account.blob.core.windows.net?sp=r&sig=abc");
    EXPECT_EQ(parts.container, "container");
    EXPECT_EQ(parts.blob_path, "blob.csv");
}

TEST(StorageURLSchemeDispatch, ParseAzureURLSASWithSlashInSignature)
{
    /// A real SAS `sig=` value is base64 and may contain `/` and `+`. The query has to be split off
    /// before the container/blob split, otherwise a slash inside the signature would be mistaken for a
    /// path separator and corrupt both the container and the blob path.
    const auto parts = parseAzureURL("az://account.blob.core.windows.net/container/dir/blob.csv?sig=a/b+c=&sp=r");
    EXPECT_EQ(parts.account_url, "https://account.blob.core.windows.net?sig=a/b+c=&sp=r");
    EXPECT_EQ(parts.container, "container");
    EXPECT_EQ(parts.blob_path, "dir/blob.csv");
}

TEST(StorageURLSchemeDispatch, ParseAbfssURLKeepsSASOnAccountURL)
{
    const auto parts = parseAzureURL("abfss://container@account.dfs.core.windows.net/dir/blob.csv?sig=a/b+c=&sp=r");
    EXPECT_EQ(parts.account_url, "https://account.blob.core.windows.net?sig=a/b+c=&sp=r");
    EXPECT_EQ(parts.container, "container");
    EXPECT_EQ(parts.blob_path, "dir/blob.csv");
}
