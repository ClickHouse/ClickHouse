#include <Interpreters/InterserverCredentials.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int WRONG_PASSWORD;
}

std::shared_ptr<ConfigInterserverCredentials>
ConfigInterserverCredentials::make(const Poco::Util::AbstractConfiguration & config, const std::string root_tag)
{
    const auto user = config.getString(root_tag + ".user", "");
    const auto password = config.getString(root_tag + ".password", "");

    if (user.empty())
        throw Exception("Configuration parameter interserver_http_credentials user can't be empty", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    auto store = makeCredentialStore(user, password, config, root_tag);

    return std::make_shared<ConfigInterserverCredentials>(user, password, store);
}

ConfigInterserverCredentials::Store ConfigInterserverCredentials::makeCredentialStore(
    const std::string current_user_,
    const std::string current_password_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string root_tag)
{
    Store store;
    store.insert({{current_user_, current_password_}, true});
    if (config.has(root_tag + ".allow_empty") && config.getBool(root_tag + ".allow_empty"))
    {
        /// Allow empty credential to support migrating from no auth
        store.insert({{"", ""}, true});
    }


    Poco::Util::AbstractConfiguration::Keys users;
    config.keys(root_tag + ".users", users);
    for (const auto & user : users)
    {
        LOG_DEBUG(&Poco::Logger::get("InterserverCredentials"), "Adding credential for {}", user);
        const auto password = config.getString(root_tag + ".users." + user);
        store.insert({{user, password}, true});
    }

    return store;
}

bool ConfigInterserverCredentials::isValidUser(const std::pair<std::string, std::string> credentials)
{
    const auto & valid = store.find(credentials);
    if (valid == store.end())
        throw Exception("Incorrect user or password in HTTP basic authentication: " + credentials.first, ErrorCodes::WRONG_PASSWORD);
    return true;
}

}
