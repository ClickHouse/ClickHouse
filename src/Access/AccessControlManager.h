#pragma once

#include <Access/MultipleAccessStorage.h>
#include <Poco/AutoPtr.h>
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
class AccessRightsContext;
using AccessRightsContextPtr = std::shared_ptr<const AccessRightsContext>;
class AccessRightsContextFactory;
struct User;
using UserPtr = std::shared_ptr<const User>;
class RoleContext;
using RoleContextPtr = std::shared_ptr<const RoleContext>;
class RoleContextFactory;
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

    void setLocalDirectory(const String & directory);
    void setUsersConfig(const Poco::Util::AbstractConfiguration & users_config);

    AccessRightsContextPtr getAccessRightsContext(
        const UUID & user_id,
        const std::vector<UUID> & current_roles,
        bool use_default_roles,
        const Settings & settings,
        const String & current_database,
        const ClientInfo & client_info) const;

    RoleContextPtr getRoleContext(
        const std::vector<UUID> & current_roles,
        const std::vector<UUID> & current_roles_with_admin_option) const;

    RowPolicyContextPtr getRowPolicyContext(
        const UUID & user_id,
        const std::vector<UUID> & enabled_roles) const;

    QuotaContextPtr getQuotaContext(
        const String & user_name,
        const UUID & user_id,
        const std::vector<UUID> & enabled_roles,
        const Poco::Net::IPAddress & address,
        const String & custom_quota_key) const;

    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

private:
    std::unique_ptr<AccessRightsContextFactory> access_rights_context_factory;
    std::unique_ptr<RoleContextFactory> role_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
};

}
