#include <gtest/gtest.h>

#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/HTTPAccessStorage.h>
#include <Common/Exception.h>
#include <Interpreters/ClientInfo.h>
#include <Poco/AutoPtr.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Util/XMLConfiguration.h>

#include <optional>
#include <sstream>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int IP_ADDRESS_NOT_ALLOWED;
}

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> configFromString(const std::string & xml)
{
    std::stringstream ss(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    config->load(ss);
    return config;
}

}

TEST(HTTPUserDirectoryConfig, SecondHTTPDirectoryRejected)
{
    /// Two <http> directories must fail at registration.
    const std::string xml = R"(<clickhouse>
        <user_directories>
            <http><server>s1</server></http>
            <http><server>s2</server></http>
        </user_directories>
    </clickhouse>)";
    auto config = configFromString(xml);
    AccessControl access_control;
    EXPECT_THROW(
        access_control.addStoragesFromUserDirectoriesConfig(*config, "user_directories", ".", ".", "", {}),
        Exception);
}

TEST(HTTPUserDirectoryConfig, SingleHTTPDirectoryAccepted)
{
    /// One <http> directory registers without throwing (the referenced server need not
    /// exist yet — the ban is on multiplicity, checked before per-storage construction).
    const std::string xml = R"(<clickhouse>
        <user_directories>
            <http><server>s1</server></http>
        </user_directories>
    </clickhouse>)";
    auto config = configFromString(xml);
    AccessControl access_control;
    EXPECT_NO_THROW(
        access_control.addStoragesFromUserDirectoriesConfig(*config, "user_directories", ".", ".", "", {}));
}

namespace
{

/// A directory whose <networks> policy rejects everything outside 10.0.0.0/8, so an
/// address outside that subnet is guaranteed to be disallowed by THIS directory.
const std::string RESTRICTIVE_NETWORKS_XML = R"(<clickhouse>
    <http_directory>
        <server>s1</server>
        <networks><ip>10.0.0.0/8</ip></networks>
    </http_directory>
</clickhouse>)";

const Poco::Net::IPAddress DISALLOWED_ADDRESS{"192.168.1.1"};

}

TEST(HTTPUserDirectoryApplicability, UnsupportedAuthenticationFormIsNotSubjectToNetworks)
{
    /// Regression for the applicability-vs-networks ordering: an authentication form this
    /// directory cannot evaluate is storage-non-applicable and must fall through to a
    /// later access storage untouched — NOT be failed closed with IP_ADDRESS_NOT_ALLOWED
    /// by a networks policy that has no bearing on it. `CredentialsWithScramble` is
    /// constructed ready (src/Access/Credentials.h) and is neither BasicCredentials nor
    /// AlwaysAllowCredentials, so it exercises exactly that classification.
    auto config = configFromString(RESTRICTIVE_NETWORKS_XML);
    AccessControl access_control;
    HTTPAccessStorage storage{"http", access_control, *config, "http_directory"};

    CredentialsWithScramble credentials{"scramble_user", "scramble", "scrambled_password"};
    ExternalAuthenticators external_authenticators;
    ClientInfo client_info;

    std::optional<AuthResult> result;
    ASSERT_NO_THROW(result = storage.authenticate(
        credentials, DISALLOWED_ADDRESS, external_authenticators, client_info,
        /* throw_if_user_not_exists = */ false,
        /* allow_no_password = */ true,
        /* allow_plaintext_password = */ true));
    EXPECT_FALSE(result.has_value());
}

TEST(HTTPUserDirectoryApplicability, ApplicableBasicAttemptIsRejectedByNetworks)
{
    /// Mutant arm for the test above: with the SAME directory and the SAME address, an
    /// applicable Basic attempt IS failed closed by the networks policy, before any
    /// external I/O (the referenced authentication server does not exist, so reaching
    /// the HTTP stage would surface a different error). Without this arm the test above
    /// would still pass if the networks policy silently parsed to "any host".
    auto config = configFromString(RESTRICTIVE_NETWORKS_XML);
    AccessControl access_control;
    HTTPAccessStorage storage{"http", access_control, *config, "http_directory"};

    BasicCredentials credentials{"basic_user", "password"};
    ExternalAuthenticators external_authenticators;
    ClientInfo client_info;

    try
    {
        storage.authenticate(
            credentials, DISALLOWED_ADDRESS, external_authenticators, client_info,
            /* throw_if_user_not_exists = */ false,
            /* allow_no_password = */ true,
            /* allow_plaintext_password = */ true);
        FAIL() << "Expected IP_ADDRESS_NOT_ALLOWED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
    }
}
