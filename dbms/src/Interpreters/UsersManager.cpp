#include <Interpreters/UsersManager.h>

#include "config_core.h"
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <IO/HexWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/SHA1Engine.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#if USE_SSL
#   include <openssl/sha.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int DNS_ERROR;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int UNKNOWN_USER;
    extern const int REQUIRED_PASSWORD;
    extern const int WRONG_PASSWORD;
    extern const int IP_ADDRESS_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

using UserPtr = UsersManager::UserPtr;

void UsersManager::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    Container new_users;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys("users", config_keys);

    for (const std::string & key : config_keys)
    {
        auto user = std::make_shared<const User>(key, "users." + key, config);
        new_users.emplace(key, std::move(user));
    }

    users = std::move(new_users);
}

UserPtr UsersManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address) const
{
    auto it = users.find(user_name);

    if (users.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    if (!it->second->addresses.contains(address))
        throw Exception("User " + user_name + " is not allowed to connect from address " + address.toString(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);

    auto on_wrong_password = [&]()
    {
        if (password.empty())
            throw Exception("Password required for user " + user_name, ErrorCodes::REQUIRED_PASSWORD);
        else
            throw Exception("Wrong password for user " + user_name, ErrorCodes::WRONG_PASSWORD);
    };

    if (!it->second->password_sha256_hex.empty())
    {
#if USE_SSL
        unsigned char hash[32];

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(password.data()), password.size());
        SHA256_Final(hash, &ctx);

        String hash_hex;
        {
            WriteBufferFromString buf(hash_hex);
            HexWriteBuffer hex_buf(buf);
            hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
        }

        Poco::toLowerInPlace(hash_hex);

        if (hash_hex != it->second->password_sha256_hex)
            on_wrong_password();
#else
        throw DB::Exception("SHA256 passwords support is disabled, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else if (!it->second->password_double_sha1_hex.empty())
    {
        Poco::SHA1Engine engine;
        engine.update(password);
        const auto & first_sha1 = engine.digest();

        /// If it was MySQL compatibility server, then first_sha1 already contains double SHA1.
        if (Poco::SHA1Engine::digestToHex(first_sha1) == it->second->password_double_sha1_hex)
            return it->second;

        engine.update(first_sha1.data(), first_sha1.size());

        if (Poco::SHA1Engine::digestToHex(engine.digest()) != it->second->password_double_sha1_hex)
            on_wrong_password();
    }
    else if (password != it->second->password)
    {
        on_wrong_password();
    }

    return it->second;
}

UserPtr UsersManager::getUser(const String & user_name) const
{
    auto it = users.find(user_name);

    if (users.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    return it->second;
}

bool UsersManager::hasAccessToDatabase(const std::string & user_name, const std::string & database_name) const
{
    auto it = users.find(user_name);

    if (users.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    auto user = it->second;
    return user->databases.empty() || user->databases.count(database_name);
}

}
