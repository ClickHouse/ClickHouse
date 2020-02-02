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


UserPtr AccessControlManager::getUser(const String & user_name) const
{
    return getUser(user_name, {}, nullptr);
}

UserPtr AccessControlManager::getUser(
    const String & user_name, const std::function<void(const UserPtr &)> & on_change, ext::scope_guard * subscription) const
{
    UUID id = getID<User>(user_name);
    if (on_change && subscription)
    {
        *subscription = subscribeForChanges(id, [on_change](const UUID &, const AccessEntityPtr & user)
        {
            if (user)
                on_change(typeid_cast<UserPtr>(user));
        });
    }
    return read<User>(id);
}


UserPtr AccessControlManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address) const
{
    return authorizeAndGetUser(user_name, password, address, {}, nullptr);
}

UserPtr AccessControlManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address,
    const std::function<void(const UserPtr &)> & on_change,
    ext::scope_guard * subscription) const
{
    auto user = getUser(user_name, on_change, subscription);
    user->allowed_client_hosts.checkContains(address, user_name);
    user->authentication.checkPassword(password, user_name);
    return user;
}


void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
}


std::shared_ptr<const AccessRightsContext> AccessControlManager::getAccessRightsContext(const ClientInfo & client_info, const AccessRights & granted_to_user, const Settings & settings, const String & current_database)
{
    return std::make_shared<AccessRightsContext>(client_info, granted_to_user, settings, current_database);
}


std::shared_ptr<QuotaContext> AccessControlManager::createQuotaContext(
    const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key)
{
    return quota_context_factory->createContext(user_name, address, custom_quota_key);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}


std::shared_ptr<RowPolicyContext> AccessControlManager::getRowPolicyContext(const String & user_name) const
{
    return row_policy_context_factory->createContext(user_name);
}

}
