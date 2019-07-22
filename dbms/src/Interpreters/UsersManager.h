#pragma once

#include <Interpreters/IUsersManager.h>

#include <map>

namespace DB
{

/** Default implementation of users manager used by native server application.
  * Manages fixed set of users listed in 'Users' configuration file.
  */
class UsersManager : public IUsersManager
{
public:
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config) override;

    UserPtr authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const override;

    UserPtr getUser(const String & user_name) const override;

    bool hasAccessToDatabase(const String & user_name, const String & database_name) const override;

private:
    using Container = std::map<String, UserPtr>;
    Container users;
};

}
