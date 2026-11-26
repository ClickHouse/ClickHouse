#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <gtest/gtest.h>

#include <IO/AzureBlobStorage/PocoHTTPClient.h>
#include <Common/RemoteHostFilter.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/URI.h>
#include <Poco/Util/XMLConfiguration.h>

#include <sstream>

namespace DB::ErrorCodes
{
    extern const int UNACCEPTABLE_URL;
}

using namespace DB;

/// A write to the `.dfs` host must be rejected when only the `.blob` read host
/// is allowlisted, i.e. the request-time host check enforces remote_url_allow_hosts.
TEST(PocoAzureHTTPClientHostFilter, WriteToDisallowedDfsHostIsRejected)
{
    const std::string xml =
        "<clickhouse><remote_url_allow_hosts>"
        "<host>onelake.blob.fabric.microsoft.com</host>"
        "</remote_url_allow_hosts></clickhouse>";
    std::stringstream ss(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration(ss));

    RemoteHostFilter filter;
    filter.setValuesFromConfig(*config);

    PocoAzureHTTPClient client(PocoAzureHTTPClientConfiguration{
        .remote_host_filter = filter,
        .max_redirects = 10,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {},
    });

    Azure::Core::Http::Request write_request(
        Azure::Core::Http::HttpMethod::Put,
        Azure::Core::Url("https://onelake.dfs.fabric.microsoft.com/ws/lh/Tables/dbo/t/data-0.parquet"));
    Azure::Core::Context context;

    try
    {
        client.Send(write_request, context);
        FAIL() << "write to a non-allowlisted .dfs host must be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNACCEPTABLE_URL);
    }

    /// Sanity: the allowlisted read host still passes, so it's not a blanket deny.
    EXPECT_NO_THROW(filter.checkURL(Poco::URI("https://onelake.blob.fabric.microsoft.com/ws/lh")));
}

/// A `host:443` allowlist entry must accept a request URL with no explicit port:
/// the scheme lets Poco resolve the default 443 (this breaks if the scheme is dropped).
TEST(PocoAzureHTTPClientHostFilter, ExplicitDefaultPortAllowlistAcceptsImplicitHttpsPort)
{
    const std::string xml =
        "<clickhouse><remote_url_allow_hosts>"
        "<host>onelake.dfs.fabric.microsoft.com:443</host>"
        "</remote_url_allow_hosts></clickhouse>";
    std::stringstream ss(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration(ss));

    RemoteHostFilter filter;
    filter.setValuesFromConfig(*config);

    /// Built exactly as makeRequestInternalImpl does: scheme + host + GetPort()==0.
    Poco::URI request_uri;
    request_uri.setScheme("https");
    request_uri.setHost("onelake.dfs.fabric.microsoft.com");
    request_uri.setPort(0);
    EXPECT_NO_THROW(filter.checkURL(request_uri));
}

#endif
