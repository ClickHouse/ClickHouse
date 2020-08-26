#include <Interpreters/InterserverCredentials.h>

namespace DB
{
std::shared_ptr<SimpleInterserverCredentials>
SimpleInterserverCredentials::make(const Poco::Util::AbstractConfiguration & config, const std::string root_tag)
{
    const auto user = config.getString(root_tag + ".user", "");
    const auto password = config.getString(root_tag + ".password", "");

    if (user.empty())
        throw Exception("Configuration parameter interserver_http_credentials user can't be empty", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    auto store = makeCredentialStore(user, password, config, root_tag);

    return std::make_shared<SimpleInterserverCredentials>(user, password, store);
}

SimpleInterserverCredentials::Store SimpleInterserverCredentials::makeCredentialStore(
    const std::string current_user,
    const std::string current_password,
    const Poco::Util::AbstractConfiguration & config,
    const std::string root_tag)
{
    Store store;
    store.push_back({current_user, current_password});
    if (config.has(root_tag + ".allow_empty") && config.getBool(root_tag + ".allow_empty"))
    {
        /// Allow empty credential to support migrating from no auth
        store.push_back({"", ""});
    }


    Poco::Util::AbstractConfiguration::Keys users;
    config.keys(root_tag + ".users", users);
    for (const auto & user : users)
    {
        LOG_DEBUG(&Logger::get("InterserverCredentials"), "Adding credential for " << user);
        const auto password = config.getString(root_tag + ".users." + user);
        store.push_back({user, password});
    }

    return store;
}

std::pair<std::string, bool> SimpleInterserverCredentials::isValidUser(std::pair<std::string, std::string> check)
{
    bool valid = false;
    for (const auto & pair : store_)
    {
        if (pair == check)
        {
            valid = true;
            break;
        }
    }
    if (!valid)
        return {"Incorrect user or password in HTTP Basic authentification", false};

    return {"", true};
}

}
