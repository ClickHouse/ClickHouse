#pragma once

#include <Access/RoleContext.h>
#include <Poco/ExpireCache.h>
#include <mutex>


namespace DB
{
class AccessControlManager;


class RoleContextFactory
{
public:
    RoleContextFactory(const AccessControlManager & manager_);
    ~RoleContextFactory();

    RoleContextPtr createContext(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option);

private:
    RoleContextPtr createContextImpl(const UUID & id, bool with_admin_option);

    const AccessControlManager & manager;
    Poco::ExpireCache<std::pair<UUID, bool>, RoleContextPtr> cache;
    std::mutex mutex;
};

}
