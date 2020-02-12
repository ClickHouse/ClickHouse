#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/User.h>
#include <Access/QuotaContextFactory.h>
#include <Access/RowPolicyContextFactory.h>
#include <Access/AccessRightsContext.h>


namespace DB
{
namespace
{
    std::vector<std::unique_ptr<IAccessStorage>> createStorages()
    {
        std::vector<std::unique_ptr<IAccessStorage>> list;
        list.emplace_back(std::make_unique<MemoryAccessStorage>());
        list.emplace_back(std::make_unique<UsersConfigAccessStorage>());
        return list;
    }
}


AccessControlManager::AccessControlManager()
    : MultipleAccessStorage(createStorages()),
      quota_context_factory(std::make_unique<QuotaContextFactory>(*this)),
      row_policy_context_factory(std::make_unique<RowPolicyContextFactory>(*this))
{
}


AccessControlManager::~AccessControlManager()
{
}


UserPtr AccessControlManager::getUser(
    const String & user_name, std::function<void(const UserPtr &)> on_change, ext::scope_guard * subscription) const
{
    return getUser(getID<User>(user_name), std::move(on_change), subscription);
}


UserPtr AccessControlManager::getUser(
    const UUID & user_id, std::function<void(const UserPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (on_change && subscription)
    {
        *subscription = subscribeForChanges(user_id, [on_change](const UUID &, const AccessEntityPtr & user)
        {
            if (user)
                on_change(typeid_cast<UserPtr>(user));
        });
    }
    return read<User>(user_id);
}


UserPtr AccessControlManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address,
    std::function<void(const UserPtr &)> on_change,
    ext::scope_guard * subscription) const
{
    return authorizeAndGetUser(getID<User>(user_name), password, address, std::move(on_change), subscription);
}


UserPtr AccessControlManager::authorizeAndGetUser(
    const UUID & user_id,
    const String & password,
    const Poco::Net::IPAddress & address,
    std::function<void(const UserPtr &)> on_change,
    ext::scope_guard * subscription) const
{
    auto user = getUser(user_id, on_change, subscription);
    user->allowed_client_hosts.checkContains(address, user->getName());
    user->authentication.checkPassword(password, user->getName());
    return user;
}


void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
}


AccessRightsContextPtr AccessControlManager::getAccessRightsContext(const UserPtr & user, const ClientInfo & client_info, const Settings & settings, const String & current_database) const
{
    return std::make_shared<AccessRightsContext>(user, client_info, settings, current_database);
}


RowPolicyContextPtr AccessControlManager::getRowPolicyContext(const String & user_name) const
{
    return row_policy_context_factory->createContext(user_name);
}


QuotaContextPtr
AccessControlManager::getQuotaContext(const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const
{
    return quota_context_factory->createContext(user_name, address, custom_quota_key);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}

}
