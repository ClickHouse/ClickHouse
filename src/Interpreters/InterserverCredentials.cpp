#include <Interpreters/InterserverCredentials.h>
#include <Common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

std::unique_ptr<InterserverCredentials>
InterserverCredentials::make(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag)
{
    if (config.has("user") && !config.has("password"))
        throw Exception("Configuration parameter interserver_http_credentials.password can't be empty", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (!config.has("user") && config.has("password"))
        throw Exception("Configuration parameter interserver_http_credentials.user can't be empty if user specified", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    /// They both can be empty
    auto user = config.getString(root_tag + ".user", "");
    auto password = config.getString(root_tag + ".password", "");

    auto store = parseCredentialsFromConfig(user, password, config, root_tag);

    return std::make_unique<InterserverCredentials>(user, password, store);
}

InterserverCredentials::CurrentCredentials InterserverCredentials::parseCredentialsFromConfig(
    const std::string & current_user_,
    const std::string & current_password_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & root_tag)
{
    auto * log = &Poco::Logger::get("InterserverCredentials");
    CurrentCredentials store;
    store.emplace_back(current_user_, current_password_);
    if (config.getBool(root_tag + ".allow_empty", false))
    {
        LOG_DEBUG(log, "Allowing empty credentials");
        /// Allow empty credential to support migrating from no auth
        store.emplace_back("", "");
    }

    Poco::Util::AbstractConfiguration::Keys old_users;
    config.keys(root_tag, old_users);

    for (const auto & user_key : old_users)
    {
        if (startsWith(user_key, "old"))
        {
            std::string full_prefix = root_tag + "." + user_key;
            std::string old_user_name = config.getString(full_prefix + ".user");
            LOG_DEBUG(log, "Adding credentials for old user {}", old_user_name);

            std::string old_user_password =  config.getString(full_prefix + ".password");

            store.emplace_back(old_user_name, old_user_password);
        }
    }

    return store;
}

InterserverCredentials::CheckResult InterserverCredentials::isValidUser(const UserWithPassword & credentials) const
{
    auto itr = std::find(all_users_store.begin(), all_users_store.end(), credentials);

    if (itr == all_users_store.end())
    {
        if (credentials.first.empty())
            return {"Server requires HTTP Basic authentication, but client doesn't provide it", false};

        return {"Incorrect user or password in HTTP basic authentication: " + credentials.first, false};
    }

    return {"", true};
}

InterserverCredentials::CheckResult InterserverCredentials::isValidUser(const std::string & user, const std::string & password) const
{
    return isValidUser(std::make_pair(user, password));
}

}
