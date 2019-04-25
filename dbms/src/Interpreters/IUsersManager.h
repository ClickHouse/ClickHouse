#pragma once

#include <Interpreters/Users.h>

namespace DB
{

/** Duties of users manager:
  * 1) Authenticate users
  * 2) Provide user settings (profile, quota, ACLs)
  * 3) Grant access to databases
  */
class IUsersManager
{
public:
    using UserPtr = std::shared_ptr<const User>;

    virtual ~IUsersManager() = default;

    virtual void loadFromConfig(const Poco::Util::AbstractConfiguration & config) = 0;

    /// Find user and make authorize checks
    virtual UserPtr authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const = 0;

    /// Just find user
    virtual UserPtr getUser(const String & user_name) const = 0;

    /// Check if the user has access to the database.
    virtual bool hasAccessToDatabase(const String & user_name, const String & database_name) const = 0;
};

}
