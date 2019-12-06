#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/QuotaContextFactory.h>


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
      quota_context_factory(std::make_unique<QuotaContextFactory>(*this))
{
}


AccessControlManager::~AccessControlManager()
{
}


void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
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
}
