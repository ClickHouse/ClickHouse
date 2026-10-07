#include "config.h"

#include <gtest/gtest.h>

#include <Common/ProxyConfiguration.h>
#include <Poco/URI.h>

#if USE_SSL
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPSClientSession.h>
#endif

namespace DB
{

TEST(ProxyCredentials, ParseUserInfo)
{
    /// Empty userinfo means no credentials at all.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("");
        ASSERT_EQ(username, "");
        ASSERT_EQ(password, "");
    }

    /// Username and password.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:password");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "password");
    }

    /// Username only, no separator.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "");
    }

    /// Trailing separator, empty password.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "");
    }

    /// No username. Poco does not send an Authorization header in this case.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo(":password");
        ASSERT_EQ(username, "");
        ASSERT_EQ(password, "password");
    }

    /// A password may contain colons. Only the first one separates.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:pass:word");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "pass:word");
    }
}

TEST(ProxyCredentials, UserInfoIsNotDecodedByPocoURI)
{
    /// `EnvironmentProxyConfigurationResolver` percent-decodes the username and the password itself,
    /// after splitting the userinfo on the first colon. This relies on `Poco::URI` keeping the userinfo
    /// verbatim when parsing a URI string, otherwise credentials would be decoded twice and an encoded
    /// colon would be taken for the separator.
    const Poco::URI uri("http://user:p%2541ss%3Aword@proxy:3128");
    ASSERT_EQ(uri.getUserInfo(), "user:p%2541ss%3Aword");

    const auto [username, password] = ProxyConfiguration::parseUserInfo(uri.getUserInfo());
    ASSERT_EQ(username, "user");
    ASSERT_EQ(password, "p%2541ss%3Aword");

    std::string decoded_password;
    Poco::URI::decode(password, decoded_password);
    ASSERT_EQ(decoded_password, "p%41ss:word");
}

#if USE_SSL

namespace
{

/// Exposes the protected `proxyAuthenticate` hook that `sendRequest` calls for a request
/// sent through a proxy without a `CONNECT` tunnel.
class TestHTTPSClientSession : public Poco::Net::HTTPSClientSession
{
public:
    using Poco::Net::HTTPSClientSession::HTTPSClientSession;
    using Poco::Net::HTTPSClientSession::proxyAuthenticate;
};

}

TEST(ProxyCredentials, HTTPSSessionSendsCredentialsWithoutTunnel)
{
    Poco::Net::Context::Params params;
    params.verificationMode = Poco::Net::Context::VERIFY_NONE;
    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, params);

    TestHTTPSClientSession session("minio1", 9001, context);

    Poco::Net::HTTPClientSession::ProxyConfig proxy_config;
    proxy_config.host = "proxy";
    proxy_config.port = 443;
    proxy_config.protocol = "https";
    proxy_config.tunnel = false;
    proxy_config.username = "user";
    proxy_config.password = "p@ssword";
    session.setProxyConfig(proxy_config);

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, "/root/data", Poco::Net::HTTPMessage::HTTP_1_1);
    session.proxyAuthenticate(request);

    /// base64("user:p@ssword")
    ASSERT_EQ(request.get("Proxy-Authorization", ""), "Basic dXNlcjpwQHNzd29yZA==");

    /// Without credentials nothing is sent.
    proxy_config.username.clear();
    proxy_config.password.clear();
    session.setProxyConfig(proxy_config);

    Poco::Net::HTTPRequest request_without_credentials(Poco::Net::HTTPRequest::HTTP_GET, "/root/data", Poco::Net::HTTPMessage::HTTP_1_1);
    session.proxyAuthenticate(request_without_credentials);
    ASSERT_FALSE(request_without_credentials.has("Proxy-Authorization"));
}

#endif

}
