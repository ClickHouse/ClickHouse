#include <gtest/gtest.h>

#include <optional>

#include <Client/ConnectionParameters.h>
#include <Core/Defines.h>
#include <Core/Protocol.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

using namespace DB;

namespace
{

/// Builds `ConnectionParameters` from an in-memory configuration without opening any
/// network connection. The host is always "localhost", so the constructor skips DNS
/// resolution (only used for compression auto-detection) and `isCloudEndpoint` returns
/// false; this keeps the test hermetic and lets us probe the secure-port logic alone.
Protocol::Secure securityFor(const Poco::Util::AbstractConfiguration & config, std::optional<UInt16> port)
{
    ConnectionParameters params(
        config,
        ConnectionParameters::Host(String("localhost")),
        ConnectionParameters::Database(String("default")),
        port);
    return params.security;
}

Poco::AutoPtr<Poco::Util::MapConfiguration> emptyConfig()
{
    return Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration);
}

}

/// Regression test for the secure-port auto-upgrade: `clickhouse-client --port 9440`
/// (the conventional TLS port) must enable a secure connection even without `--secure`.
/// A refactor once dropped the `port` argument from the internal `enableSecureConnection`
/// check, turning this branch into dead code; this test guards against that recurring.
TEST(ConnectionParametersSecurePort, DefaultSecurePortEnablesSecure)
{
    auto config = emptyConfig();
    EXPECT_EQ(securityFor(*config, static_cast<UInt16>(DBMS_DEFAULT_SECURE_PORT)), Protocol::Secure::Enable);
}

/// The default non-secure port must not be upgraded.
TEST(ConnectionParametersSecurePort, DefaultPortStaysInsecure)
{
    auto config = emptyConfig();
    EXPECT_EQ(securityFor(*config, static_cast<UInt16>(DBMS_DEFAULT_PORT)), Protocol::Secure::Disable);
}

/// The explicit `no-secure` override must win even on the conventional secure port.
TEST(ConnectionParametersSecurePort, NoSecureOverridesSecurePort)
{
    auto config = emptyConfig();
    config->setBool("no-secure", true);
    EXPECT_EQ(securityFor(*config, static_cast<UInt16>(DBMS_DEFAULT_SECURE_PORT)), Protocol::Secure::Disable);
}

/// The explicit `secure` flag enables TLS regardless of the (non-secure) port.
TEST(ConnectionParametersSecurePort, SecureFlagEnablesOnDefaultPort)
{
    auto config = emptyConfig();
    config->setBool("secure", true);
    EXPECT_EQ(securityFor(*config, static_cast<UInt16>(DBMS_DEFAULT_PORT)), Protocol::Secure::Enable);
}

/// The port can also arrive through the configuration rather than the explicit argument
/// (`getPortFromConfig` resolves it); the secure auto-upgrade must still fire.
TEST(ConnectionParametersSecurePort, SecurePortFromConfigEnablesSecure)
{
    auto config = emptyConfig();
    config->setInt("port", DBMS_DEFAULT_SECURE_PORT);
    /// std::nullopt forces the port to be resolved from the configuration.
    EXPECT_EQ(securityFor(*config, std::nullopt), Protocol::Secure::Enable);
}
