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
class AccessRightsContext;
using AccessRightsContextPtr = std::shared_ptr<const AccessRightsContext>;
class RowPolicyContext;
using RowPolicyContextPtr = std::shared_ptr<const RowPolicyContext>;
class RowPolicyContextFactory;
class QuotaContext;
using QuotaContextPtr = std::shared_ptr<const QuotaContext>;
class QuotaContextFactory;
struct QuotaUsageInfo;
class ClientInfo;
struct Settings;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);

    UserPtr getUser(const String & user_name, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr getUser(const UUID & user_id, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr authorizeAndGetUser(const String & user_name, const String & password, const Poco::Net::IPAddress & address, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr authorizeAndGetUser(const UUID & user_id, const String & password, const Poco::Net::IPAddress & address, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;

    AccessRightsContextPtr getAccessRightsContext(const UserPtr & user, const ClientInfo & client_info, const Settings & settings, const String & current_database) const;

    RowPolicyContextPtr getRowPolicyContext(const String & user_name) const;

    QuotaContextPtr getQuotaContext(const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const;
    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

private:
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
};

}
