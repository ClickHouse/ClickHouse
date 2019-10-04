#pragma once

#include <map>
#include <Interpreters/Users.h>

namespace DB
{

/** Default implementation of users manager used by native server application.
  * Manages fixed set of users listed in 'Users' configuration file.
  */
class UsersManager
{
public:
    using UserPtr = std::shared_ptr<const User>;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);

    UserPtr authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const;

    UserPtr getUser(const String & user_name) const;

    bool hasAccessToDatabase(const String & user_name, const String & database_name) const;
    bool hasAccessToDictionary(const String & user_name, const String & dictionary_name) const;

private:
    using Container = std::map<String, UserPtr>;
    Container users;
};

}
