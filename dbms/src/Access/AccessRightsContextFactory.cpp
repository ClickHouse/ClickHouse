#include <Access/AccessRightsContextFactory.h>
#include <Access/AccessControlManager.h>
#include <Core/Settings.h>


namespace DB
{
AccessRightsContextFactory::AccessRightsContextFactory(const AccessControlManager & manager_)
    : manager(manager_), cache(600000 /* 10 minutes */) {}

AccessRightsContextFactory::~AccessRightsContextFactory() = default;


AccessRightsContextPtr AccessRightsContextFactory::createContext(const Params & params)
{
    std::lock_guard lock{mutex};
    auto x = cache.get(params);
    if (x)
        return *x;
    auto res = ext::shared_ptr_helper<AccessRightsContext>::create(manager, params);
    cache.add(params, res);
    return res;
}

AccessRightsContextPtr AccessRightsContextFactory::createContext(
    const UUID & user_id,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info)
{
    Params params;
    params.user_id = user_id;
    params.current_database = current_database;
    params.readonly = settings.readonly;
    params.allow_ddl = settings.allow_ddl;
    params.allow_introspection = settings.allow_introspection_functions;
    params.interface = client_info.interface;
    params.http_method = client_info.http_method;
    params.address = client_info.current_address.host();
    params.quota_key = client_info.quota_key;
    return createContext(params);
}

}
