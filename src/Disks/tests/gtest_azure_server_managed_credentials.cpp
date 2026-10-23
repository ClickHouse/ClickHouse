#include <gtest/gtest.h>
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ACCESS_DENIED;
}

static AzureBlobStorage::ConnectionParams makeRestrictedParams(const String & url)
{
    AzureBlobStorage::ConnectionParams params;
    AzureBlobStorage::processURL(url, "container", params.endpoint, params.auth_method);
    params.forbid_implicit_credentials = true;
    return params;
}

/// `processURL` keeps a managed identity next to a SAS token, which authorizes the request by itself.
/// The OneLake write path asks before it looks at the SAS token, so it must not be refused.
TEST(AzureServerManagedCredentials, SasTokenIsNotRefused)
{
    auto params = makeRestrictedParams("https://onelake.dfs.fabric.microsoft.com/workspace?sv=2021-08-06&sig=abc");
    ASSERT_FALSE(params.endpoint.sas_auth.empty());
    EXPECT_FALSE(params.checkOrDropServerManagedCredentials());
}

TEST(AzureServerManagedCredentials, ServerIdentityIsRefused)
{
    auto params = makeRestrictedParams("https://onelake.dfs.fabric.microsoft.com/workspace");
    try
    {
        params.checkOrDropServerManagedCredentials();
        FAIL() << "Expected ACCESS_DENIED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ACCESS_DENIED);
    }
}

TEST(AzureServerManagedCredentials, ServerIdentityIsDroppedWithFallback)
{
    auto params = makeRestrictedParams("https://onelake.dfs.fabric.microsoft.com/workspace");
    params.anonymous_fallback_for_server_credentials = true;
    EXPECT_TRUE(params.checkOrDropServerManagedCredentials());
}

#endif
