#pragma once

#include <Access/AccessRightsContext.h>
#include <Poco/ExpireCache.h>
#include <mutex>


namespace DB
{
class AccessControlManager;


class AccessRightsContextFactory
{
public:
    AccessRightsContextFactory(const AccessControlManager & manager_);
    ~AccessRightsContextFactory();

    using Params = AccessRightsContext::Params;
    AccessRightsContextPtr createContext(const Params & params);
    AccessRightsContextPtr createContext(const UUID & user_id, const Settings & settings, const String & current_database, const ClientInfo & client_info);

private:
    const AccessControlManager & manager;
    Poco::ExpireCache<Params, AccessRightsContextPtr> cache;
    std::mutex mutex;
};

}
