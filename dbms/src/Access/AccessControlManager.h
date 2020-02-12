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

    AccessRightsContextPtr getAccessRightsContext(
        const UUID & user_id, const Settings & settings, const String & current_database, const ClientInfo & client_info) const;

    RowPolicyContextPtr getRowPolicyContext(const UUID & user_id) const;

    QuotaContextPtr getQuotaContext(
        const UUID & user_id, const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const;

    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

private:
    std::unique_ptr<AccessRightsContextFactory> access_rights_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
};

}
