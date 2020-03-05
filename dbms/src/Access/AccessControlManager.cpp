#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/DiskAccessStorage.h>
#include <Access/AccessRightsContextFactory.h>
#include <Access/RoleContextFactory.h>
#include <Access/RowPolicyContextFactory.h>
#include <Access/QuotaContextFactory.h>


namespace DB
{
namespace
{
    std::vector<std::unique_ptr<IAccessStorage>> createStorages()
    {
        std::vector<std::unique_ptr<IAccessStorage>> list;
        list.emplace_back(std::make_unique<DiskAccessStorage>());
        list.emplace_back(std::make_unique<UsersConfigAccessStorage>());
        list.emplace_back(std::make_unique<MemoryAccessStorage>());
        return list;
    }

    constexpr size_t DISK_ACCESS_STORAGE_INDEX = 0;
    constexpr size_t USERS_CONFIG_ACCESS_STORAGE_INDEX = 1;
}


AccessControlManager::AccessControlManager()
    : MultipleAccessStorage(createStorages()),
      access_rights_context_factory(std::make_unique<AccessRightsContextFactory>(*this)),
      role_context_factory(std::make_unique<RoleContextFactory>(*this)),
      row_policy_context_factory(std::make_unique<RowPolicyContextFactory>(*this)),
      quota_context_factory(std::make_unique<QuotaContextFactory>(*this))
{
}


AccessControlManager::~AccessControlManager()
{
}


void AccessControlManager::setLocalDirectory(const String & directory_path)
{
    auto & disk_access_storage = dynamic_cast<DiskAccessStorage &>(getStorageByIndex(DISK_ACCESS_STORAGE_INDEX));
    disk_access_storage.setDirectory(directory_path);
}


void AccessControlManager::setUsersConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(USERS_CONFIG_ACCESS_STORAGE_INDEX));
    users_config_access_storage.setConfiguration(users_config);
}


AccessRightsContextPtr AccessControlManager::getAccessRightsContext(
    const UUID & user_id,
    const std::vector<UUID> & current_roles,
    bool use_default_roles,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info) const
{
    return access_rights_context_factory->createContext(user_id, current_roles, use_default_roles, settings, current_database, client_info);
}


RoleContextPtr AccessControlManager::getRoleContext(
    const std::vector<UUID> & current_roles,
    const std::vector<UUID> & current_roles_with_admin_option) const
{
    return role_context_factory->createContext(current_roles, current_roles_with_admin_option);
}


RowPolicyContextPtr AccessControlManager::getRowPolicyContext(const UUID & user_id, const std::vector<UUID> & enabled_roles) const
{
    return row_policy_context_factory->createContext(user_id, enabled_roles);
}


QuotaContextPtr AccessControlManager::getQuotaContext(
    const String & user_name, const UUID & user_id, const std::vector<UUID> & enabled_roles, const Poco::Net::IPAddress & address, const String & custom_quota_key) const
{
    return quota_context_factory->createContext(user_name, user_id, enabled_roles, address, custom_quota_key);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}

}
