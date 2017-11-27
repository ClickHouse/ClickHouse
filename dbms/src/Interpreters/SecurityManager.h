#pragma once

#include <Interpreters/ISecurityManager.h>

#include <map>

namespace DB
{

class SecurityManager : public ISecurityManager
{
private:
    using Container = std::map<String, User>;
    Container users;

public:
    void loadFromConfig(Poco::Util::AbstractConfiguration & config) override;

    const User & authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const override;

    const User & getUser(const String & user_name) const override;

    bool hasAccessToDatabase(const String & user_name, const String & database_name) const override;
};

}
