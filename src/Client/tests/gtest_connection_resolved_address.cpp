#include "config.h"

#include <gtest/gtest.h>

#include <Client/Connection.h>
#include <Common/Exception.h>
#include <IO/ConnectionTimeouts.h>

#include <Poco/Net/NetException.h>

using namespace DB;

TEST(ConnectionResolvedAddress, LocalSetupFailureReportsNoAddress)
{
    /// The socket is never created, so no address has been dialled.
    Connection connection(
        "127.0.0.1", 9000, "", "default", "", "notchunked", "notchunked",
        SSHKey(), /*jwt*/ "", /*quota_key*/ "", /*cluster*/ "", /*cluster_secret*/ "", "client",
        Protocol::Compression::Disable, Protocol::Secure::Disable, /*tls_sni_override*/ "", /*bind_host*/ "",
#if USE_JWT_CPP && USE_SSL
        /*jwt_provider*/ nullptr,
#endif
        [](bool) -> std::unique_ptr<Poco::Net::StreamSocket> { throw Poco::Net::NetException("Local socket setup failed"); });

    ASSERT_THROW(connection.forceConnected(ConnectionTimeouts()), Exception);
    ASSERT_FALSE(connection.getResolvedAddress().has_value());
}
