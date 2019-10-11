#include <Interpreters/UsersManager.h>

#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_USER;
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

    it->second->allowed_client_hosts.checkContains(address, user_name);
    it->second->authentication.checkPassword(password, user_name);
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

bool UsersManager::hasAccessToDictionary(const std::string & user_name, const std::string & dictionary_name) const
{
    auto it = users.find(user_name);

    if (users.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    auto user = it->second;
    return user->dictionaries.empty() || user->dictionaries.count(dictionary_name);
}
}
