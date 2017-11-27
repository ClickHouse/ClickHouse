#pragma once

#include <Interpreters/Users.h>

namespace DB
{

class ISecurityManager
{
public:
    virtual void loadFromConfig(Poco::Util::AbstractConfiguration & config) = 0;

    /// Find user and make authorize checks
    virtual const User & authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const = 0;

    /// Just find user
    virtual const User & getUser(const String & user_name) = 0;

    /// Check if the user has access to the database.
    virtual bool hasAccessToDatabase(const String & user_name, const String & database_name) const = 0;

    virtual ~ISecurityManager() {}
};

}
