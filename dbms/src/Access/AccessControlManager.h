#pragma once

#include <Access/MultipleAccessStorage.h>
#include <Poco/AutoPtr.h>
#include <ext/scope_guard.h>
#include <memory>


namespace Poco
{
    namespace Net
    {
        class IPAddress;
    }
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{
struct User;
using UserPtr = std::shared_ptr<const User>;
class QuotaContext;
class QuotaContextFactory;
struct QuotaUsageInfo;
class RowPolicyContext;
class RowPolicyContextFactory;
class AccessRights;
class AccessRightsContext;
class ClientInfo;
struct Settings;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);

    UserPtr getUser(const String & user_name) const;
    UserPtr getUser(const String & user_name, const std::function<void(const UserPtr &)> & on_change, ext::scope_guard * subscription) const;
    UserPtr authorizeAndGetUser(const String & user_name, const String & password, const Poco::Net::IPAddress & address) const;
    UserPtr authorizeAndGetUser(const String & user_name, const String & password, const Poco::Net::IPAddress & address, const std::function<void(const UserPtr &)> & on_change, ext::scope_guard * subscription) const;

    std::shared_ptr<const AccessRightsContext> getAccessRightsContext(const ClientInfo & client_info, const AccessRights & granted_to_user, const Settings & settings, const String & current_database);

    std::shared_ptr<QuotaContext>
    createQuotaContext(const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key);

    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

    std::shared_ptr<RowPolicyContext> getRowPolicyContext(const String & user_name) const;

private:
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
};

}
