#pragma once

#include <common/types.h>
#include <Interpreters/Context.h>
#include <Core/MySQL/PacketEndpoint.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#endif

namespace DB
{

namespace MySQLProtocol
{

namespace Authentication
{

class IPlugin
{
public:
    virtual ~IPlugin() = default;

    virtual String getName() = 0;

    virtual String getAuthPluginData() = 0;

    virtual void authenticate(
        const String & user_name, std::optional<String> auth_response, ContextMutablePtr context,
        std::shared_ptr<PacketEndpoint> packet_endpoint, bool is_secure_connection, const Poco::Net::SocketAddress & address) = 0;
};

/// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
class Native41 : public IPlugin
{
public:
    Native41();

    Native41(const String & password, const String & auth_plugin_data);

    String getName() override { return "mysql_native_password"; }

    String getAuthPluginData() override { return scramble; }

    void authenticate(
        const String & user_name, std::optional<String> auth_response, ContextMutablePtr context,
        std::shared_ptr<PacketEndpoint> packet_endpoint, bool /* is_secure_connection */, const Poco::Net::SocketAddress & address) override;

private:
    String scramble;
};

#if USE_SSL
/// Caching SHA2 plugin is not used because it would be possible to authenticate knowing hash from users.xml.
/// https://dev.mysql.com/doc/internals/en/sha256.html
class Sha256Password : public IPlugin
{
public:
    Sha256Password(RSA & public_key_, RSA & private_key_, Poco::Logger * log_);

    String getName() override { return "sha256_password"; }

    String getAuthPluginData() override { return scramble; }

    void authenticate(
        const String & user_name, std::optional<String> auth_response, ContextMutablePtr context,
        std::shared_ptr<PacketEndpoint> packet_endpoint, bool is_secure_connection, const Poco::Net::SocketAddress & address) override;

private:
    RSA & public_key;
    RSA & private_key;
    Poco::Logger * log;
    String scramble;
};
#endif

}

}

}
