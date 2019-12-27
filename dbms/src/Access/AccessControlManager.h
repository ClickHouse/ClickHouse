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
class QuotaContext;
class QuotaContextFactory;
struct QuotaUsageInfo;
class RowPolicyContext;
class RowPolicyContextFactory;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);

    std::shared_ptr<QuotaContext>
    createQuotaContext(const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key);

    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

    std::shared_ptr<RowPolicyContext> getRowPolicyContext(const String & user_name) const;

private:
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
};

}
